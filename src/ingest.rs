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
use crate::storage::{StorageClient, StorageRoot};

#[derive(Clone, Debug)]
pub struct IngestConfig {
    pub inputs: Vec<String>,
    pub output_root: String,
    pub parallax_filter_mas: Option<f32>,
}

#[derive(Clone, Debug)]
pub struct IngestResult {
    pub metadata: Vec<SourceMetadata>,
}

#[derive(Debug)]
struct StagedInput {
    path: PathBuf,
    input_name: String,
    source_bulk_url: String,
    source_bulk_md5: String,
    _tempdir: Option<tempfile::TempDir>,
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

fn stage_input(input: &str) -> Result<StagedInput> {
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
    Ok(StagedInput {
        path,
        input_name,
        source_bulk_url: input.to_string(),
        source_bulk_md5,
        _tempdir: None,
    })
}

fn read_canonical_rows(
    input_path: &Path,
    parallax_filter_mas: Option<f32>,
) -> Result<(Vec<CanonicalRow>, SourceCounts)> {
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
        rows_after_parallax_filter: 0,
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

        if let Some(minimum_parallax) = parallax_filter_mas {
            if parallax <= minimum_parallax {
                continue;
            }
        }
        counts.rows_after_parallax_filter += 1;

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
    staged_input: &StagedInput,
    parallax_filter_mas: Option<f32>,
) -> bool {
    metadata.source_bulk_url == staged_input.source_bulk_url
        && metadata.source_bulk_md5 == staged_input.source_bulk_md5
        && metadata.input_name == staged_input.input_name
        && metadata.parallax_filter_mas == parallax_filter_mas
}

fn load_existing_source(
    storage: &StorageClient,
    output_root: &StorageRoot,
    staged_input: &StagedInput,
    parallax_filter_mas: Option<f32>,
) -> Result<Option<SourceMetadata>> {
    let Some(metadata_bytes) = storage.read_optional_bytes(
        &output_root.join(&metadata_path_for_source(&staged_input.input_name)),
    )?
    else {
        return Ok(None);
    };
    let Ok(metadata) = decode_source_metadata(&metadata_bytes) else {
        return Ok(None);
    };
    if !metadata_matches(&metadata, staged_input, parallax_filter_mas) {
        return Ok(None);
    }
    if storage
        .validate_canonical_layout(output_root, &metadata)
        .is_err()
    {
        return Ok(None);
    }
    Ok(Some(metadata))
}

fn write_source_output(
    output_root: &Path,
    staged_input: &StagedInput,
    parallax_filter_mas: Option<f32>,
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
        parallax_filter_mas,
        ingestion_started_at: started_at,
        ingestion_finished_at: finished_at,
        counts,
    };
    write_source_metadata(&canonical_root.join(METADATA_FILENAME), &metadata)?;
    Ok(metadata)
}

fn ingest_one(
    storage: &StorageClient,
    output_root: &StorageRoot,
    input: &str,
    parallax_filter_mas: Option<f32>,
) -> Result<SourceMetadata> {
    let staged_input = stage_input(input)?;
    if let Some(metadata) =
        load_existing_source(storage, output_root, &staged_input, parallax_filter_mas)?
    {
        return Ok(metadata);
    }

    let started_at = Utc::now().to_rfc3339();
    let (rows, counts) = read_canonical_rows(&staged_input.path, parallax_filter_mas)?;
    let finished_at = Utc::now().to_rfc3339();

    match output_root {
        StorageRoot::Local(path) => {
            fs::create_dir_all(path)
                .with_context(|| format!("failed to create {}", path.display()))?;
            write_source_output(
                path,
                &staged_input,
                parallax_filter_mas,
                &rows,
                counts,
                started_at,
                finished_at,
            )
        }
        root @ StorageRoot::Gcs(_) => {
            let local_output = tempdir().context("failed to create temporary output directory")?;
            let metadata = write_source_output(
                local_output.path(),
                &staged_input,
                parallax_filter_mas,
                &rows,
                counts,
                started_at,
                finished_at,
            )?;
            storage.upload_directory(local_output.path(), root)?;
            storage.validate_canonical_layout(root, &metadata)?;
            Ok(metadata)
        }
    }
}

pub fn run_ingestion(config: IngestConfig) -> Result<IngestResult> {
    if config.inputs.is_empty() {
        bail!("at least one --input is required");
    }
    let output_root = StorageRoot::parse(&config.output_root)?;
    let storage = StorageClient::new()?;
    let mut metadata = Vec::with_capacity(config.inputs.len());
    for input in &config.inputs {
        metadata.push(ingest_one(
            &storage,
            &output_root,
            input,
            config.parallax_filter_mas,
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

    fn write_gzip_file(path: &Path, body: &str) {
        let file = fs::File::create(path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(body.as_bytes()).unwrap();
        encoder.finish().unwrap();
    }

    #[test]
    fn ingests_filtered_rows_and_writes_canonical_layout() {
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

        let result = run_ingestion(IngestConfig {
            inputs: vec![input_path.display().to_string()],
            output_root: output_path.display().to_string(),
            parallax_filter_mas: None,
        })
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
        assert_eq!(metadata.counts.rows_after_parallax_filter, 2);
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

        let result = run_ingestion(IngestConfig {
            inputs: vec![input_path.display().to_string()],
            output_root: output_path.display().to_string(),
            parallax_filter_mas: None,
        })
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

        let first = run_ingestion(IngestConfig {
            inputs: vec![input_path.display().to_string()],
            output_root: output_path.display().to_string(),
            parallax_filter_mas: None,
        })
        .unwrap();
        let metadata_path =
            output_path.join(metadata_path_for_source(&first.metadata[0].input_name));
        let metadata_before = fs::read(&metadata_path).unwrap();

        let second = run_ingestion(IngestConfig {
            inputs: vec![input_path.display().to_string()],
            output_root: output_path.display().to_string(),
            parallax_filter_mas: None,
        })
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
        let first = run_ingestion(IngestConfig {
            inputs: vec![input_path.display().to_string()],
            output_root: output_path.display().to_string(),
            parallax_filter_mas: None,
        })
        .unwrap();

        write_gzip_file(
            &input_path,
            "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             7,0,0,100,1,14.5,0.8\n",
        );
        let second = run_ingestion(IngestConfig {
            inputs: vec![input_path.display().to_string()],
            output_root: output_path.display().to_string(),
            parallax_filter_mas: None,
        })
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
}
