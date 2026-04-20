use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};

pub const CANONICAL_ROOT: &str = "canonical";
pub const CANONICAL_ROW_SIZE: u64 = 32;
pub const METADATA_FILENAME: &str = "metadata.txt";

const METADATA_MAGIC: &str = "STARDUMP-METADATA 1";

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CanonicalRow {
    pub source_id: u64,
    pub ra: f32,
    pub dec: f32,
    pub parallax: f32,
    pub parallax_error: f32,
    pub phot_g_mean_mag: f32,
    pub bp_rp: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SourceCounts {
    pub rows_seen: u64,
    pub rows_with_positive_parallax: u64,
    pub rows_written: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SourceMetadata {
    pub source_bulk_url: String,
    pub source_bulk_md5: String,
    pub input_name: String,
    pub canonical_directory: String,
    pub canonical_parts: Vec<String>,
    pub ingestion_started_at: String,
    pub ingestion_finished_at: String,
    pub counts: SourceCounts,
}

fn canonical_row_bytes(row: &CanonicalRow) -> [u8; CANONICAL_ROW_SIZE as usize] {
    let mut bytes = [0_u8; CANONICAL_ROW_SIZE as usize];
    bytes[0..8].copy_from_slice(&row.source_id.to_le_bytes());
    bytes[8..12].copy_from_slice(&row.ra.to_le_bytes());
    bytes[12..16].copy_from_slice(&row.dec.to_le_bytes());
    bytes[16..20].copy_from_slice(&row.parallax.to_le_bytes());
    bytes[20..24].copy_from_slice(&row.parallax_error.to_le_bytes());
    bytes[24..28].copy_from_slice(&row.phot_g_mean_mag.to_le_bytes());
    bytes[28..32].copy_from_slice(&row.bp_rp.to_le_bytes());
    bytes
}

fn parse_metadata_fields(text: &str) -> Result<BTreeMap<String, String>> {
    let mut lines = text.lines();
    let magic = lines
        .next()
        .ok_or_else(|| anyhow!("metadata file is empty"))?;
    if magic != METADATA_MAGIC {
        bail!("invalid metadata magic {magic:?}");
    }

    let mut fields = BTreeMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (key, value) = line
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid metadata line {line:?}"))?;
        if fields
            .insert(key.to_string(), value.trim_start().to_string())
            .is_some()
        {
            bail!("duplicate metadata key {key}");
        }
    }
    Ok(fields)
}

fn metadata_field<'a>(fields: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str> {
    fields
        .get(key)
        .map(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing metadata key {key}"))
}

fn parse_metadata_u64(fields: &BTreeMap<String, String>, key: &str) -> Result<u64> {
    metadata_field(fields, key)?
        .parse()
        .with_context(|| format!("failed to parse metadata key {key}"))
}

fn parse_metadata_parts(fields: &BTreeMap<String, String>, key: &str) -> Result<Vec<String>> {
    let value = metadata_field(fields, key)?;
    if value.is_empty() {
        return Ok(Vec::new());
    }
    Ok(value.split(',').map(str::to_string).collect())
}

fn decode_rows<T, F>(bytes: &[u8], row_size: u64, decode: F) -> Result<Vec<T>>
where
    F: Fn(&[u8]) -> T,
{
    if bytes.len() as u64 % row_size != 0 {
        bail!(
            "buffer size {} is not a multiple of row size {row_size}",
            bytes.len()
        );
    }
    Ok(bytes
        .chunks_exact(row_size as usize)
        .map(decode)
        .collect::<Vec<_>>())
}

fn read_rows<T, F>(path: &Path, row_size: u64, decode: F) -> Result<Vec<T>>
where
    F: Fn(&[u8]) -> T,
{
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    decode_rows(&bytes, row_size, decode)
}

fn decode_canonical_row(chunk: &[u8]) -> CanonicalRow {
    CanonicalRow {
        source_id: u64::from_le_bytes(chunk[0..8].try_into().unwrap()),
        ra: f32::from_le_bytes(chunk[8..12].try_into().unwrap()),
        dec: f32::from_le_bytes(chunk[12..16].try_into().unwrap()),
        parallax: f32::from_le_bytes(chunk[16..20].try_into().unwrap()),
        parallax_error: f32::from_le_bytes(chunk[20..24].try_into().unwrap()),
        phot_g_mean_mag: f32::from_le_bytes(chunk[24..28].try_into().unwrap()),
        bp_rp: f32::from_le_bytes(chunk[28..32].try_into().unwrap()),
    }
}

pub fn canonical_directory_path(input_name: &str) -> String {
    let name = input_name
        .strip_prefix("GaiaSource_")
        .and_then(|value| value.strip_suffix(".csv.gz"))
        .unwrap_or(input_name);
    format!("{CANONICAL_ROOT}/{name}")
}

pub fn metadata_path_for_source(input_name: &str) -> String {
    format!(
        "{}/{}",
        canonical_directory_path(input_name),
        METADATA_FILENAME
    )
}

pub fn compute_luminosity(parallax_mas: f32, phot_g_mean_mag: f32) -> Option<f32> {
    if !parallax_mas.is_finite() || parallax_mas <= 0.0 {
        return None;
    }
    let distance_pc = 1000.0 / parallax_mas;
    let m_g = phot_g_mean_mag - 5.0 * (distance_pc / 10.0).log10();
    let lum = 10_f32.powf((-m_g + 4.67) / 2.5);
    if lum.is_finite() && lum > 0.0 { Some(lum) } else { None }
}

pub fn read_canonical_rows(path: &Path) -> Result<Vec<CanonicalRow>> {
    read_rows(path, CANONICAL_ROW_SIZE, decode_canonical_row)
}

pub fn decode_canonical_rows(bytes: &[u8]) -> Result<Vec<CanonicalRow>> {
    decode_rows(bytes, CANONICAL_ROW_SIZE, decode_canonical_row)
}

pub fn decode_source_metadata(bytes: &[u8]) -> Result<SourceMetadata> {
    let text = std::str::from_utf8(bytes).context("metadata is not valid UTF-8")?;
    let fields = parse_metadata_fields(text)?;
    Ok(SourceMetadata {
        source_bulk_url: metadata_field(&fields, "source_bulk_url")?.to_string(),
        source_bulk_md5: metadata_field(&fields, "source_bulk_md5")?.to_string(),
        input_name: metadata_field(&fields, "input_name")?.to_string(),
        canonical_directory: metadata_field(&fields, "canonical_directory")?.to_string(),
        canonical_parts: parse_metadata_parts(&fields, "canonical_parts")?,
        ingestion_started_at: metadata_field(&fields, "ingestion_started_at")?.to_string(),
        ingestion_finished_at: metadata_field(&fields, "ingestion_finished_at")?.to_string(),
        counts: SourceCounts {
            rows_seen: parse_metadata_u64(&fields, "rows_seen")?,
            rows_with_positive_parallax: parse_metadata_u64(
                &fields,
                "rows_with_positive_parallax",
            )?,
            rows_written: parse_metadata_u64(&fields, "rows_written")?,
        },
    })
}

pub fn encode_source_metadata(metadata: &SourceMetadata) -> Vec<u8> {
    let canonical_parts = metadata.canonical_parts.join(",");
    format!(
        "{METADATA_MAGIC}\n\
source_bulk_url: {}\n\
source_bulk_md5: {}\n\
input_name: {}\n\
canonical_directory: {}\n\
canonical_parts: {}\n\
ingestion_started_at: {}\n\
ingestion_finished_at: {}\n\
rows_seen: {}\n\
rows_with_positive_parallax: {}\n\
rows_written: {}\n",
        metadata.source_bulk_url,
        metadata.source_bulk_md5,
        metadata.input_name,
        metadata.canonical_directory,
        canonical_parts,
        metadata.ingestion_started_at,
        metadata.ingestion_finished_at,
        metadata.counts.rows_seen,
        metadata.counts.rows_with_positive_parallax,
        metadata.counts.rows_written,
    )
    .into_bytes()
}

pub fn read_source_metadata(path: &Path) -> Result<SourceMetadata> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    decode_source_metadata(&bytes).with_context(|| format!("failed to parse {}", path.display()))
}

pub fn write_source_metadata(path: &Path, metadata: &SourceMetadata) -> Result<()> {
    let file =
        fs::File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writer
        .write_all(&encode_source_metadata(metadata))
        .with_context(|| format!("failed to write {}", path.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", path.display()))
}

pub fn write_canonical_rows(path: &Path, rows: &[CanonicalRow]) -> Result<()> {
    let file =
        fs::File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    for row in rows {
        writer
            .write_all(&canonical_row_bytes(row))
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", path.display()))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn canonical_rows_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("canonical.bin");
        let rows = vec![
            CanonicalRow {
                source_id: 1,
                ra: 1.5,
                dec: -2.5,
                parallax: 4.5,
                parallax_error: 0.25,
                phot_g_mean_mag: 12.3,
                bp_rp: f32::NAN,
            },
            CanonicalRow {
                source_id: 2,
                ra: 9.5,
                dec: 8.5,
                parallax: 7.5,
                parallax_error: 0.5,
                phot_g_mean_mag: 13.4,
                bp_rp: 1.25,
            },
        ];

        write_canonical_rows(&path, &rows).unwrap();

        let round_trip = read_canonical_rows(&path).unwrap();
        assert_eq!(round_trip[0].source_id, rows[0].source_id);
        assert!(round_trip[0].bp_rp.is_nan());
        assert_eq!(round_trip[1], rows[1]);
    }

    #[test]
    fn metadata_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(METADATA_FILENAME);
        let metadata = SourceMetadata {
            source_bulk_url: "https://example.test/input.csv.gz".to_string(),
            source_bulk_md5: "abc123".to_string(),
            input_name: "input.csv.gz".to_string(),
            canonical_directory: canonical_directory_path("input.csv.gz"),
            canonical_parts: vec!["part-000.bin".to_string()],
            ingestion_started_at: "2025-01-01T00:00:00Z".to_string(),
            ingestion_finished_at: "2025-01-01T00:00:01Z".to_string(),
            counts: SourceCounts {
                rows_seen: 10,
                rows_with_positive_parallax: 8,
                rows_written: 8,
            },
        };

        write_source_metadata(&path, &metadata).unwrap();
        assert_eq!(read_source_metadata(&path).unwrap(), metadata);
    }
}
