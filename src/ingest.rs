use std::collections::BTreeMap;
use std::fs;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use csv::{ReaderBuilder, StringRecord};
use flate2::read::MultiGzDecoder;
use tempfile::tempdir;

use crate::formats::{
    CanonicalRow, OctreeIndex, RunCounts, RunMetadata, ServingRow, leaf_filename,
    validate_run_layout, write_canonical_rows, write_json, write_serving_rows,
};
use crate::octree::{Bounds3, OctreeConfig};
use crate::storage::{StorageClient, StorageRoot};

pub const DEFAULT_DEPTH: u8 = 6;
pub const DEFAULT_BOUNDS: Bounds3 = Bounds3 {
    min: [-100_000.0, -100_000.0, -100_000.0],
    max: [100_000.0, 100_000.0, 100_000.0],
};

#[derive(Clone, Debug)]
pub struct IngestConfig {
    pub input: String,
    pub output_root: String,
    pub parallax_filter_mas: Option<f32>,
    pub octree_depth: u8,
    pub bounds: Bounds3,
}

#[derive(Clone, Debug)]
pub struct IngestResult {
    pub metadata: RunMetadata,
    pub index: OctreeIndex,
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

fn canonical_directory_name(input_name: &str) -> String {
    input_name
        .strip_prefix("GaiaSource_")
        .and_then(|value| value.strip_suffix(".csv.gz"))
        .unwrap_or(input_name)
        .to_string()
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

fn cartesian_coordinates(ra_deg: f32, dec_deg: f32, parallax_mas: f32) -> [f32; 3] {
    let distance_pc = 1_000.0_f64 / parallax_mas as f64;
    let ra = (ra_deg as f64).to_radians();
    let dec = (dec_deg as f64).to_radians();
    [
        (distance_pc * dec.cos() * ra.cos()) as f32,
        (distance_pc * dec.cos() * ra.sin()) as f32,
        (distance_pc * dec.sin()) as f32,
    ]
}

fn read_rows(
    config: &IngestConfig,
) -> Result<(Vec<CanonicalRow>, BTreeMap<u32, Vec<ServingRow>>, RunCounts)> {
    let octree = OctreeConfig {
        depth: config.octree_depth,
        bounds: config.bounds,
    };
    let reader = open_input(&config.input)?;
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

    let mut canonical_rows = Vec::new();
    let mut serving_rows = BTreeMap::<u32, Vec<ServingRow>>::new();
    let mut counts = RunCounts {
        rows_seen: 0,
        rows_with_positive_parallax: 0,
        rows_after_parallax_filter: 0,
        rows_in_bounds: 0,
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

        if let Some(minimum_parallax) = config.parallax_filter_mas {
            if parallax <= minimum_parallax {
                continue;
            }
        }
        counts.rows_after_parallax_filter += 1;

        let source_id = parse_required_u64(record.get(source_id_index), "source_id")?;
        let ra = parse_required_f32(record.get(ra_index), "ra")?;
        let dec = parse_required_f32(record.get(dec_index), "dec")?;
        let parallax_error =
            parse_required_f32(record.get(parallax_error_index), "parallax_error")?;
        let phot_g_mean_mag = optional_f32_or_nan(record.get(phot_g_mean_mag_index))?;
        let bp_rp = optional_f32_or_nan(record.get(bp_rp_index))?;
        let [x, y, z] = cartesian_coordinates(ra, dec, parallax);
        let Some(morton) = octree.morton_for_point([x, y, z]) else {
            continue;
        };

        counts.rows_in_bounds += 1;
        canonical_rows.push(CanonicalRow {
            source_id,
            ra,
            dec,
            parallax,
            parallax_error,
            phot_g_mean_mag,
            bp_rp,
        });
        serving_rows
            .entry(morton)
            .or_default()
            .push(ServingRow { source_id, x, y, z });
    }

    Ok((canonical_rows, serving_rows, counts))
}

fn write_output(
    output_root: &Path,
    config: &IngestConfig,
    input_name: &str,
    canonical_rows: &[CanonicalRow],
    serving_rows: &mut BTreeMap<u32, Vec<ServingRow>>,
    counts: RunCounts,
    started_at: String,
    finished_at: String,
) -> Result<IngestResult> {
    let canonical_directory = format!("canonical/{}", canonical_directory_name(input_name));
    let serving_directory = format!("serving/depth={}", config.octree_depth);
    let canonical_root = output_root.join(&canonical_directory);
    let serving_root = output_root.join(&serving_directory);
    if canonical_root.exists() {
        fs::remove_dir_all(&canonical_root)
            .with_context(|| format!("failed to clear {}", canonical_root.display()))?;
    }
    if serving_root.exists() {
        fs::remove_dir_all(&serving_root)
            .with_context(|| format!("failed to clear {}", serving_root.display()))?;
    }
    fs::create_dir_all(&canonical_root)
        .with_context(|| format!("failed to create {}", canonical_root.display()))?;
    fs::create_dir_all(&serving_root)
        .with_context(|| format!("failed to create {}", serving_root.display()))?;

    let canonical_parts = vec!["part-000.bin".to_string()];
    write_canonical_rows(&canonical_root.join(&canonical_parts[0]), canonical_rows)?;

    let mut leaves = Vec::with_capacity(serving_rows.len());
    for (morton, rows) in serving_rows {
        rows.sort_by_key(|row| row.source_id);
        write_serving_rows(&serving_root.join(leaf_filename(*morton)), rows)?;
        leaves.push(*morton);
    }

    let metadata = RunMetadata {
        source_bulk_url: config.input.clone(),
        input_name: input_name.to_string(),
        canonical_directory,
        canonical_parts,
        serving_directory,
        octree_depth: config.octree_depth,
        bounds: config.bounds,
        parallax_filter_mas: config.parallax_filter_mas,
        ingestion_started_at: started_at,
        ingestion_finished_at: finished_at,
        counts,
    };
    let index = OctreeIndex {
        depth: config.octree_depth,
        bounds: config.bounds,
        leaves,
    };

    write_json(&output_root.join("metadata.json"), &metadata)?;
    write_json(&output_root.join("index.octree"), &index)?;
    validate_run_layout(output_root, &metadata, &index)?;

    Ok(IngestResult { metadata, index })
}

pub fn run_ingestion(config: IngestConfig) -> Result<IngestResult> {
    if config.octree_depth == 0 || config.octree_depth > 10 {
        bail!("octree depth must be between 1 and 10");
    }
    let output_root = StorageRoot::parse(&config.output_root)?;
    let input_name = input_name(&config.input);

    let started_at = Utc::now().to_rfc3339();
    let (canonical_rows, mut serving_rows, counts) = read_rows(&config)?;
    let finished_at = Utc::now().to_rfc3339();
    match output_root {
        StorageRoot::Local(path) => {
            fs::create_dir_all(&path)
                .with_context(|| format!("failed to create {}", path.display()))?;
            write_output(
                &path,
                &config,
                &input_name,
                &canonical_rows,
                &mut serving_rows,
                counts,
                started_at,
                finished_at,
            )
        }
        root @ StorageRoot::Gcs(_) => {
            let local_output = tempdir().context("failed to create temporary output directory")?;
            let result = write_output(
                local_output.path(),
                &config,
                &input_name,
                &canonical_rows,
                &mut serving_rows,
                counts,
                started_at,
                finished_at,
            )?;
            let storage = StorageClient::new()?;
            storage.upload_directory(local_output.path(), &root)?;
            storage.validate_run_layout(&root, &result.metadata, &result.index)?;
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tempfile::tempdir;

    use crate::formats::{read_canonical_rows, read_json, read_serving_rows};

    use super::*;

    fn write_gzip_file(path: &Path, body: &str) {
        let file = fs::File::create(path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(body.as_bytes()).unwrap();
        encoder.finish().unwrap();
    }

    #[test]
    fn ingests_filtered_rows_and_writes_layout() {
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
            input: input_path.display().to_string(),
            output_root: output_path.display().to_string(),
            parallax_filter_mas: None,
            octree_depth: DEFAULT_DEPTH,
            bounds: DEFAULT_BOUNDS,
        })
        .unwrap();

        let metadata: RunMetadata = read_json(&output_path.join("metadata.json")).unwrap();
        let index: OctreeIndex = read_json(&output_path.join("index.octree")).unwrap();
        let canonical_rows = read_canonical_rows(
            &output_path
                .join(&metadata.canonical_directory)
                .join("part-000.bin"),
        )
        .unwrap();
        let leaf_rows: Vec<Vec<ServingRow>> = index
            .leaves
            .iter()
            .map(|leaf| {
                read_serving_rows(
                    &output_path
                        .join(&metadata.serving_directory)
                        .join(leaf_filename(*leaf)),
                )
                .unwrap()
            })
            .collect();

        assert_eq!(result.metadata, metadata);
        assert_eq!(result.index, index);
        assert_eq!(metadata.counts.rows_seen, 4);
        assert_eq!(metadata.counts.rows_with_positive_parallax, 2);
        assert_eq!(metadata.counts.rows_after_parallax_filter, 2);
        assert_eq!(metadata.counts.rows_in_bounds, 2);
        assert_eq!(canonical_rows.len(), 2);
        assert_eq!(canonical_rows[0].parallax_error, 1.0);
        assert_eq!(canonical_rows[0].phot_g_mean_mag, 12.5);
        assert_eq!(canonical_rows[0].bp_rp, 0.3);
        assert_eq!(canonical_rows[1].phot_g_mean_mag, 13.5);
        assert!(canonical_rows[1].bp_rp.is_nan());
        assert_eq!(leaf_rows.iter().map(Vec::len).sum::<usize>(), 2);
        assert!(index.leaves.len() >= 2);
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
            input: input_path.display().to_string(),
            output_root: output_path.display().to_string(),
            parallax_filter_mas: None,
            octree_depth: DEFAULT_DEPTH,
            bounds: DEFAULT_BOUNDS,
        })
        .unwrap();

        assert_eq!(result.metadata.counts.rows_seen, 2);
        assert_eq!(result.metadata.counts.rows_with_positive_parallax, 1);
        assert_eq!(result.metadata.counts.rows_in_bounds, 1);
    }
}
