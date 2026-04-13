use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use csv::{ReaderBuilder, StringRecord};
use flate2::read::MultiGzDecoder;
use md5::Context as Md5Context;
use tempfile::tempdir;

use crate::formats::{
    CanonicalRow, METADATA_FILENAME, SourceCounts, SourceMetadata, canonical_directory_path,
    decode_source_metadata, metadata_path_for_source, write_canonical_rows, write_source_metadata,
};
use crate::storage::{local_path, read_optional_bytes, validate_canonical_layout};

#[derive(Clone, Debug)]
pub struct IngestConfig {
    inputs: Vec<String>,
    input_manifest: Option<String>,
    output_root: String,
}

#[derive(Clone, Debug)]
pub struct IngestResult {
    pub metadata: Vec<SourceMetadata>,
}

#[derive(Clone, Debug)]
struct InputSpec {
    input: String,
    expected_md5: Option<String>,
}

#[derive(Debug)]
struct StagedInput {
    path: PathBuf,
    input_name: String,
    source_bulk_url: String,
    source_bulk_md5: String,
    _tempdir: Option<tempfile::TempDir>,
}

impl IngestConfig {
    pub fn new(output_root: String) -> Self {
        Self {
            inputs: Vec::new(),
            input_manifest: None,
            output_root,
        }
    }

    pub fn with_inputs(mut self, inputs: Vec<String>) -> Self {
        self.inputs = inputs;
        self
    }

    pub fn with_input_manifest(mut self, input_manifest: Option<String>) -> Self {
        self.input_manifest = input_manifest;
        self
    }
}

fn parse_optional_f32(value: Option<&str>) -> Result<Option<f32>> {
    match value.map(str::trim) {
        None | Some("") | Some("null") => Ok(None),
        Some(value) => {
            Ok(Some(value.parse().with_context(|| {
                format!("failed to parse float value {value}")
            })?))
        }
    }
}

fn optional_f32_or_nan(value: Option<&str>) -> Result<f32> {
    Ok(parse_optional_f32(value)?.unwrap_or(f32::NAN))
}

fn parse_required_f32(value: Option<&str>, field: &str) -> Result<f32> {
    let value = value
        .ok_or_else(|| anyhow!("missing field {field}"))?
        .trim();
    value
        .parse()
        .with_context(|| format!("failed to parse {field} value {value}"))
}

fn parse_required_u64(value: Option<&str>, field: &str) -> Result<u64> {
    let value = value
        .ok_or_else(|| anyhow!("missing field {field}"))?
        .trim();
    value
        .parse()
        .with_context(|| format!("failed to parse {field} value {value}"))
}

fn field_index(headers: &StringRecord, field: &str) -> Result<usize> {
    headers
        .iter()
        .position(|header| header == field)
        .ok_or_else(|| anyhow!("missing CSV field {field}"))
}

fn input_name(input: &str) -> String {
    input.rsplit('/').next().unwrap_or(input).trim().to_string()
}

fn parse_cloud_run_usize(name: &str) -> Result<Option<usize>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value.parse()?)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_manifest(path: &str) -> Result<Vec<InputSpec>> {
    Ok(std::fs::read_to_string(path)?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            if let Some((expected_md5, input)) = line.split_once('\t') {
                InputSpec {
                    input: input.to_string(),
                    expected_md5: Some(expected_md5.to_string()),
                }
            } else {
                InputSpec {
                    input: line.to_string(),
                    expected_md5: None,
                }
            }
        })
        .collect())
}

fn sharded_inputs(inputs: Vec<InputSpec>) -> Result<Vec<InputSpec>> {
    let task_index = parse_cloud_run_usize("CLOUD_RUN_TASK_INDEX")?;
    let task_count = parse_cloud_run_usize("CLOUD_RUN_TASK_COUNT")?;

    match (task_index, task_count) {
        (Some(index), Some(count)) => {
            if count == 0 {
                bail!("CLOUD_RUN_TASK_COUNT must be greater than zero");
            }
            if index >= count {
                bail!("CLOUD_RUN_TASK_INDEX must be less than CLOUD_RUN_TASK_COUNT");
            }
            Ok(inputs
                .into_iter()
                .enumerate()
                .filter_map(|(offset, input)| (offset % count == index).then_some(input))
                .collect())
        }
        (None, None) => Ok(inputs),
        _ => bail!("CLOUD_RUN_TASK_INDEX and CLOUD_RUN_TASK_COUNT must be set together"),
    }
}

fn collect_inputs(inputs: Vec<String>, input_manifest: Option<String>) -> Result<Vec<InputSpec>> {
    let mut all_inputs = inputs
        .into_iter()
        .map(|input| InputSpec {
            input,
            expected_md5: None,
        })
        .collect::<Vec<_>>();
    if let Some(input_manifest) = input_manifest {
        all_inputs.extend(read_manifest(&input_manifest)?);
    }
    if all_inputs.is_empty() {
        bail!("at least one --input or --input-manifest value is required");
    }
    sharded_inputs(all_inputs)
}

fn open_input(input: &str) -> Result<Box<dyn Read>> {
    if input.starts_with("http://") || input.starts_with("https://") {
        let response = reqwest::blocking::get(input)
            .with_context(|| format!("failed to GET {input}"))?
            .error_for_status()
            .with_context(|| format!("unsuccessful response for {input}"))?;
        return Ok(Box::new(response));
    }

    let path = input.strip_prefix("file://").unwrap_or(input);
    let file = fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    Ok(Box::new(file))
}

fn md5_hex<R: Read>(reader: &mut R) -> Result<String> {
    let mut context = Md5Context::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).context("failed to read input")?;
        if read == 0 {
            return Ok(format!("{:x}", context.compute()));
        }
        context.consume(&buffer[..read]);
    }
}

fn copy_with_md5<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<String> {
    let mut context = Md5Context::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).context("failed to read input")?;
        if read == 0 {
            return Ok(format!("{:x}", context.compute()));
        }
        context.consume(&buffer[..read]);
        writer
            .write_all(&buffer[..read])
            .context("failed to stage input")?;
    }
}

fn stage_input(input: &str, trusted_md5: Option<&str>) -> Result<StagedInput> {
    let input_name = input_name(input);
    if input.starts_with("http://") || input.starts_with("https://") {
        let tempdir = tempdir().context("failed to create temporary input directory")?;
        let path = tempdir.path().join(&input_name);
        let mut response = reqwest::blocking::get(input)
            .with_context(|| format!("failed to GET {input}"))?
            .error_for_status()
            .with_context(|| format!("unsuccessful response for {input}"))?;
        let file = fs::File::create(&path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        let mut writer = BufWriter::new(file);
        let source_bulk_md5 = copy_with_md5(&mut response, &mut writer)?;
        writer
            .flush()
            .with_context(|| format!("failed to flush {}", path.display()))?;
        if let Some(trusted_md5) = trusted_md5 {
            if source_bulk_md5 != trusted_md5 {
                bail!("trusted md5 mismatch for {input}");
            }
        }
        return Ok(StagedInput {
            path,
            input_name,
            source_bulk_url: input.to_string(),
            source_bulk_md5,
            _tempdir: Some(tempdir),
        });
    }

    let path = PathBuf::from(input.strip_prefix("file://").unwrap_or(input));
    let file =
        fs::File::open(&path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let source_bulk_md5 = md5_hex(&mut reader)?;
    if let Some(trusted_md5) = trusted_md5 {
        if source_bulk_md5 != trusted_md5 {
            bail!("trusted md5 mismatch for {input}");
        }
    }
    Ok(StagedInput {
        path,
        input_name,
        source_bulk_url: input.to_string(),
        source_bulk_md5,
        _tempdir: None,
    })
}

fn read_canonical_rows(input_path: &Path) -> Result<(Vec<CanonicalRow>, SourceCounts)> {
    let reader = open_input(&input_path.display().to_string())?;
    let decoder = MultiGzDecoder::new(BufReader::new(reader));
    let mut csv_reader = ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_reader(decoder);
    let headers = csv_reader
        .headers()
        .context("failed to read CSV headers")?
        .clone();
    let source_id_index = field_index(&headers, "source_id")?;
    let ra_index = field_index(&headers, "ra")?;
    let dec_index = field_index(&headers, "dec")?;
    let parallax_index = field_index(&headers, "parallax")?;
    let parallax_error_index = field_index(&headers, "parallax_error")?;
    let phot_g_mean_mag_index = field_index(&headers, "phot_g_mean_mag")?;
    let bp_rp_index = field_index(&headers, "bp_rp")?;

    let mut rows = Vec::new();
    let mut counts = SourceCounts {
        rows_seen: 0,
        rows_with_positive_parallax: 0,
        rows_written: 0,
    };

    for record in csv_reader.records() {
        let record = record.context("failed to read CSV record")?;
        counts.rows_seen += 1;

        let Some(parallax) = parse_optional_f32(record.get(parallax_index))? else {
            continue;
        };
        if parallax <= 0.0 {
            continue;
        }
        counts.rows_with_positive_parallax += 1;

        rows.push(CanonicalRow {
            source_id: parse_required_u64(record.get(source_id_index), "source_id")?,
            ra: parse_required_f32(record.get(ra_index), "ra")?,
            dec: parse_required_f32(record.get(dec_index), "dec")?,
            parallax,
            parallax_error: parse_required_f32(record.get(parallax_error_index), "parallax_error")?,
            phot_g_mean_mag: optional_f32_or_nan(record.get(phot_g_mean_mag_index))?,
            bp_rp: optional_f32_or_nan(record.get(bp_rp_index))?,
        });
    }

    counts.rows_written = rows.len() as u64;
    Ok((rows, counts))
}

fn metadata_matches(
    metadata: &SourceMetadata,
    source_bulk_url: &str,
    source_bulk_md5: &str,
    input_name: &str,
) -> bool {
    metadata.source_bulk_url == source_bulk_url
        && metadata.source_bulk_md5 == source_bulk_md5
        && metadata.input_name == input_name
}

fn load_existing_source(
    output_root: &Path,
    source_bulk_url: &str,
    source_bulk_md5: &str,
    input_name: &str,
) -> Result<Option<SourceMetadata>> {
    let Some(metadata_bytes) =
        read_optional_bytes(&output_root.join(metadata_path_for_source(input_name)))?
    else {
        return Ok(None);
    };
    let Ok(metadata) = decode_source_metadata(&metadata_bytes) else {
        return Ok(None);
    };
    if !metadata_matches(&metadata, source_bulk_url, source_bulk_md5, input_name) {
        return Ok(None);
    }
    if validate_canonical_layout(output_root, &metadata).is_err() {
        return Ok(None);
    }
    Ok(Some(metadata))
}

fn write_source_output(
    output_root: &Path,
    staged_input: &StagedInput,
    rows: &[CanonicalRow],
    counts: SourceCounts,
    started_at: String,
    finished_at: String,
) -> Result<SourceMetadata> {
    let canonical_directory = canonical_directory_path(&staged_input.input_name);
    let canonical_root = output_root.join(&canonical_directory);
    if canonical_root.exists() {
        fs::remove_dir_all(&canonical_root)
            .with_context(|| format!("failed to clear {}", canonical_root.display()))?;
    }
    fs::create_dir_all(&canonical_root)
        .with_context(|| format!("failed to create {}", canonical_root.display()))?;

    let canonical_parts = vec!["part-000.bin".to_string()];
    write_canonical_rows(&canonical_root.join(&canonical_parts[0]), rows)?;

    let metadata = SourceMetadata {
        source_bulk_url: staged_input.source_bulk_url.clone(),
        source_bulk_md5: staged_input.source_bulk_md5.clone(),
        input_name: staged_input.input_name.clone(),
        canonical_directory,
        canonical_parts,
        ingestion_started_at: started_at,
        ingestion_finished_at: finished_at,
        counts,
    };
    write_source_metadata(&canonical_root.join(METADATA_FILENAME), &metadata)?;
    Ok(metadata)
}

fn ingest_one(
    output_root: &Path,
    input: &str,
    trusted_md5: Option<&str>,
) -> Result<SourceMetadata> {
    let input_name = input_name(input);
    if let Some(trusted_md5) = trusted_md5 {
        if let Some(metadata) = load_existing_source(output_root, input, trusted_md5, &input_name)?
        {
            return Ok(metadata);
        }
    }

    let staged_input = stage_input(input, trusted_md5)?;
    if let Some(metadata) = load_existing_source(
        output_root,
        &staged_input.source_bulk_url,
        &staged_input.source_bulk_md5,
        &staged_input.input_name,
    )? {
        return Ok(metadata);
    }

    let started_at = Utc::now().to_rfc3339();
    let (rows, counts) = read_canonical_rows(&staged_input.path)?;
    let finished_at = Utc::now().to_rfc3339();

    fs::create_dir_all(output_root)
        .with_context(|| format!("failed to create {}", output_root.display()))?;
    write_source_output(
        output_root,
        &staged_input,
        &rows,
        counts,
        started_at,
        finished_at,
    )
}

pub fn run_ingestion(config: IngestConfig) -> Result<IngestResult> {
    let input_specs = collect_inputs(config.inputs, config.input_manifest)?;
    if input_specs.is_empty() {
        return Ok(IngestResult {
            metadata: Vec::new(),
        });
    }
    let output_root = local_path(&config.output_root)?;
    let mut trusted_md5_by_input = HashMap::new();
    let inputs = input_specs
        .into_iter()
        .map(|input_spec| {
            if let Some(expected_md5) = input_spec.expected_md5 {
                trusted_md5_by_input.insert(input_spec.input.clone(), expected_md5);
            }
            input_spec.input
        })
        .collect::<Vec<_>>();
    let mut metadata = Vec::with_capacity(inputs.len());
    for input in &inputs {
        metadata.push(ingest_one(
            &output_root,
            input,
            trusted_md5_by_input.get(input).map(String::as_str),
        )?);
    }
    Ok(IngestResult { metadata })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tempfile::tempdir;

    use crate::formats::{read_canonical_rows, read_source_metadata};

    use super::*;

    fn test_config(inputs: Vec<String>, output_path: &Path) -> IngestConfig {
        IngestConfig::new(output_path.display().to_string()).with_inputs(inputs)
    }

    fn write_gzip_file(path: &Path, body: &str) {
        let file = fs::File::create(path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(body.as_bytes()).unwrap();
        encoder.finish().unwrap();
    }

    #[test]
    fn ingests_positive_parallax_rows_and_writes_canonical_layout() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
        let output_path = dir.path().join("run");

        write_gzip_file(
            &input_path,
            "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             1,0,0,100,1,12.5,0.3\n\
             2,90,0,0.2,0.1,13.5,\n\
             3,0,0,,0.2,14.5,1.1\n\
             4,0,0,-1,0.3,15.5,1.2\n",
        );

        let result = run_ingestion(test_config(
            vec![input_path.display().to_string()],
            &output_path,
        ))
        .unwrap();

        let metadata = read_source_metadata(
            &output_path.join(metadata_path_for_source(&result.metadata[0].input_name)),
        )
        .unwrap();
        let canonical_rows = read_canonical_rows(
            &output_path
                .join(&metadata.canonical_directory)
                .join("part-000.bin"),
        )
        .unwrap();

        assert_eq!(result.metadata, vec![metadata.clone()]);
        assert_eq!(metadata.counts.rows_seen, 4);
        assert_eq!(metadata.counts.rows_with_positive_parallax, 2);
        assert_eq!(metadata.counts.rows_written, 2);
        assert_eq!(metadata.source_bulk_md5.len(), 32);
        assert_eq!(canonical_rows.len(), 2);
        assert_eq!(canonical_rows[0].parallax_error, 1.0);
        assert_eq!(canonical_rows[0].phot_g_mean_mag, 12.5);
        assert_eq!(canonical_rows[0].bp_rp, 0.3);
        assert_eq!(canonical_rows[1].phot_g_mean_mag, 13.5);
        assert!(canonical_rows[1].bp_rp.is_nan());
    }

    #[test]
    fn ingests_ecsv_with_comment_preamble() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
        let output_path = dir.path().join("run");

        write_gzip_file(
            &input_path,
            "# %ECSV 1.0\n\
             # ---\n\
             # delimiter: ','\n\
             # datatype:\n\
             # - {name: source_id, datatype: int64}\n\
             # - {name: ra, datatype: float64}\n\
             # - {name: dec, datatype: float64}\n\
             # - {name: parallax, datatype: float64}\n\
             # - {name: parallax_error, datatype: float64}\n\
             # - {name: phot_g_mean_mag, datatype: float32}\n\
             # - {name: bp_rp, datatype: float32}\n\
             source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             1,0,0,100,2,12.1,null\n\
             2,1,2,null,3,13.2,0.5\n",
        );

        let result = run_ingestion(test_config(
            vec![input_path.display().to_string()],
            &output_path,
        ))
        .unwrap();

        assert_eq!(result.metadata[0].counts.rows_seen, 2);
        assert_eq!(result.metadata[0].counts.rows_with_positive_parallax, 1);
        assert_eq!(result.metadata[0].counts.rows_written, 1);
    }

    #[test]
    fn skips_reingesting_unchanged_input() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
        let output_path = dir.path().join("run");

        write_gzip_file(
            &input_path,
            "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             1,0,0,100,1,12.5,0.3\n\
             2,90,0,100,1,13.5,0.6\n",
        );

        let first = run_ingestion(test_config(
            vec![input_path.display().to_string()],
            &output_path,
        ))
        .unwrap();
        let metadata_path =
            output_path.join(metadata_path_for_source(&first.metadata[0].input_name));
        let metadata_before = fs::read(&metadata_path).unwrap();

        let second = run_ingestion(test_config(
            vec![input_path.display().to_string()],
            &output_path,
        ))
        .unwrap();
        let metadata_after = fs::read(metadata_path).unwrap();

        assert_eq!(second.metadata, first.metadata);
        assert_eq!(metadata_after, metadata_before);
    }

    #[test]
    fn reingests_when_input_md5_changes() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
        let output_path = dir.path().join("run");

        write_gzip_file(
            &input_path,
            "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             1,0,0,100,1,12.5,0.3\n",
        );
        let first = run_ingestion(test_config(
            vec![input_path.display().to_string()],
            &output_path,
        ))
        .unwrap();

        write_gzip_file(
            &input_path,
            "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             7,0,0,100,1,14.5,0.8\n",
        );
        let second = run_ingestion(test_config(
            vec![input_path.display().to_string()],
            &output_path,
        ))
        .unwrap();
        let metadata = read_source_metadata(
            &output_path.join(metadata_path_for_source(&second.metadata[0].input_name)),
        )
        .unwrap();
        let canonical_rows = read_canonical_rows(
            &output_path
                .join(&metadata.canonical_directory)
                .join("part-000.bin"),
        )
        .unwrap();

        assert_ne!(
            first.metadata[0].source_bulk_md5,
            second.metadata[0].source_bulk_md5
        );
        assert_eq!(canonical_rows.len(), 1);
        assert_eq!(canonical_rows[0].source_id, 7);
    }

    #[test]
    fn skips_remote_input_before_download_when_trusted_md5_matches() {
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("run");
        let input = "https://example.invalid/GaiaSource_786097-786431.csv.gz".to_string();
        let trusted_md5 = "0123456789abcdef0123456789abcdef".to_string();
        let rows = vec![CanonicalRow {
            source_id: 1,
            ra: 0.0,
            dec: 0.0,
            parallax: 10.0,
            parallax_error: 1.0,
            phot_g_mean_mag: 12.0,
            bp_rp: 0.5,
        }];
        let counts = SourceCounts {
            rows_seen: 1,
            rows_with_positive_parallax: 1,
            rows_written: 1,
        };

        fs::create_dir_all(&output_path).unwrap();
        write_source_output(
            &output_path,
            &StagedInput {
                path: PathBuf::new(),
                input_name: input_name(&input),
                source_bulk_url: input.clone(),
                source_bulk_md5: trusted_md5.clone(),
                _tempdir: None,
            },
            &rows,
            counts,
            "2026-01-01T00:00:00Z".to_string(),
            "2026-01-01T00:00:01Z".to_string(),
        )
        .unwrap();

        let manifest_path = dir.path().join("inputs.txt");
        fs::write(&manifest_path, format!("{trusted_md5}\t{input}\n")).unwrap();
        let result = run_ingestion(
            IngestConfig::new(output_path.display().to_string())
                .with_input_manifest(Some(manifest_path.display().to_string())),
        )
        .unwrap();

        assert_eq!(result.metadata.len(), 1);
        assert_eq!(result.metadata[0].counts.rows_written, 1);
    }
}
