use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};

use crate::octree::Bounds3;

pub const CANONICAL_ROW_SIZE: u64 = 32;
pub const SERVING_ROW_SIZE: u64 = 20;
pub const LEAF_FILENAME_WIDTH: usize = 8;
pub const METADATA_FILENAME: &str = "metadata.txt";
pub const OCTREE_INDEX_FILENAME: &str = "index.octree";

const METADATA_MAGIC: &str = "STARDUMP-METADATA 1";
const OCTREE_INDEX_MAGIC: [u8; 8] = *b"OCTREE\0\0";
const OCTREE_INDEX_VERSION: u32 = 1;
const OCTREE_HEADER_SIZE: usize = 44;

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

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ServingRow {
    pub source_id: u64,
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunCounts {
    pub rows_seen: u64,
    pub rows_with_positive_parallax: u64,
    pub rows_after_parallax_filter: u64,
    pub rows_in_bounds: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunMetadata {
    pub source_bulk_url: String,
    pub source_bulk_md5: String,
    pub input_name: String,
    pub canonical_directory: String,
    pub canonical_parts: Vec<String>,
    pub serving_directory: String,
    pub octree_depth: u8,
    pub bounds: Bounds3,
    pub parallax_filter_mas: Option<f32>,
    pub ingestion_started_at: String,
    pub ingestion_finished_at: String,
    pub counts: RunCounts,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OctreeIndex {
    pub depth: u8,
    pub bounds: Bounds3,
    pub leaves: Vec<u32>,
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

fn serving_row_bytes(row: &ServingRow) -> [u8; SERVING_ROW_SIZE as usize] {
    let mut bytes = [0_u8; SERVING_ROW_SIZE as usize];
    bytes[0..8].copy_from_slice(&row.source_id.to_le_bytes());
    bytes[8..12].copy_from_slice(&row.x.to_le_bytes());
    bytes[12..16].copy_from_slice(&row.y.to_le_bytes());
    bytes[16..20].copy_from_slice(&row.z.to_le_bytes());
    bytes
}

fn read_rows<T, F>(path: &Path, row_size: u64, decode: F) -> Result<Vec<T>>
where
    F: Fn(&[u8]) -> T,
{
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    decode_rows(&bytes, row_size, decode)
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

fn parse_metadata_u8(fields: &BTreeMap<String, String>, key: &str) -> Result<u8> {
    metadata_field(fields, key)?
        .parse()
        .with_context(|| format!("failed to parse metadata key {key}"))
}

fn parse_metadata_f32(fields: &BTreeMap<String, String>, key: &str) -> Result<f32> {
    metadata_field(fields, key)?
        .parse()
        .with_context(|| format!("failed to parse metadata key {key}"))
}

fn parse_metadata_optional_f32(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<f32>> {
    match metadata_field(fields, key)? {
        "none" => Ok(None),
        _ => parse_metadata_f32(fields, key).map(Some),
    }
}

fn parse_metadata_triplet(fields: &BTreeMap<String, String>, key: &str) -> Result<[f32; 3]> {
    let value = metadata_field(fields, key)?;
    let mut parts = value.split(',');
    let x = parts
        .next()
        .ok_or_else(|| anyhow!("missing first component for metadata key {key}"))?
        .parse()
        .with_context(|| format!("failed to parse metadata key {key}"))?;
    let y = parts
        .next()
        .ok_or_else(|| anyhow!("missing second component for metadata key {key}"))?
        .parse()
        .with_context(|| format!("failed to parse metadata key {key}"))?;
    let z = parts
        .next()
        .ok_or_else(|| anyhow!("missing third component for metadata key {key}"))?
        .parse()
        .with_context(|| format!("failed to parse metadata key {key}"))?;
    if parts.next().is_some() {
        bail!("too many components for metadata key {key}");
    }
    Ok([x, y, z])
}

fn parse_metadata_parts(fields: &BTreeMap<String, String>, key: &str) -> Result<Vec<String>> {
    let value = metadata_field(fields, key)?;
    if value.is_empty() {
        return Ok(Vec::new());
    }
    Ok(value.split(',').map(str::to_string).collect())
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = offset + 4;
    let chunk = bytes
        .get(offset..end)
        .ok_or_else(|| anyhow!("buffer too short at offset {offset}"))?;
    Ok(u32::from_le_bytes(chunk.try_into().unwrap()))
}

fn read_f32(bytes: &[u8], offset: usize) -> Result<f32> {
    let end = offset + 4;
    let chunk = bytes
        .get(offset..end)
        .ok_or_else(|| anyhow!("buffer too short at offset {offset}"))?;
    Ok(f32::from_le_bytes(chunk.try_into().unwrap()))
}

pub fn leaf_filename(morton: u32) -> String {
    format!("leaf-{morton:0width$}.bin", width = LEAF_FILENAME_WIDTH)
}

pub fn read_canonical_rows(path: &Path) -> Result<Vec<CanonicalRow>> {
    read_rows(path, CANONICAL_ROW_SIZE, decode_canonical_row)
}

pub fn read_serving_rows(path: &Path) -> Result<Vec<ServingRow>> {
    read_rows(path, SERVING_ROW_SIZE, decode_serving_row)
}

pub fn decode_canonical_rows(bytes: &[u8]) -> Result<Vec<CanonicalRow>> {
    decode_rows(bytes, CANONICAL_ROW_SIZE, decode_canonical_row)
}

pub fn decode_serving_rows(bytes: &[u8]) -> Result<Vec<ServingRow>> {
    decode_rows(bytes, SERVING_ROW_SIZE, decode_serving_row)
}

pub fn decode_run_metadata(bytes: &[u8]) -> Result<RunMetadata> {
    let text = std::str::from_utf8(bytes).context("metadata is not valid UTF-8")?;
    let fields = parse_metadata_fields(text)?;
    Ok(RunMetadata {
        source_bulk_url: metadata_field(&fields, "source_bulk_url")?.to_string(),
        source_bulk_md5: metadata_field(&fields, "source_bulk_md5")?.to_string(),
        input_name: metadata_field(&fields, "input_name")?.to_string(),
        canonical_directory: metadata_field(&fields, "canonical_directory")?.to_string(),
        canonical_parts: parse_metadata_parts(&fields, "canonical_parts")?,
        serving_directory: metadata_field(&fields, "serving_directory")?.to_string(),
        octree_depth: parse_metadata_u8(&fields, "octree_depth")?,
        bounds: Bounds3 {
            min: parse_metadata_triplet(&fields, "bounds_min")?,
            max: parse_metadata_triplet(&fields, "bounds_max")?,
        },
        parallax_filter_mas: parse_metadata_optional_f32(&fields, "parallax_filter_mas")?,
        ingestion_started_at: metadata_field(&fields, "ingestion_started_at")?.to_string(),
        ingestion_finished_at: metadata_field(&fields, "ingestion_finished_at")?.to_string(),
        counts: RunCounts {
            rows_seen: parse_metadata_u64(&fields, "rows_seen")?,
            rows_with_positive_parallax: parse_metadata_u64(
                &fields,
                "rows_with_positive_parallax",
            )?,
            rows_after_parallax_filter: parse_metadata_u64(&fields, "rows_after_parallax_filter")?,
            rows_in_bounds: parse_metadata_u64(&fields, "rows_in_bounds")?,
        },
    })
}

pub fn encode_run_metadata(metadata: &RunMetadata) -> Vec<u8> {
    let canonical_parts = metadata.canonical_parts.join(",");
    let parallax_filter_mas = metadata
        .parallax_filter_mas
        .map_or_else(|| "none".to_string(), |value| value.to_string());
    format!(
        "{METADATA_MAGIC}\n\
source_bulk_url: {}\n\
source_bulk_md5: {}\n\
input_name: {}\n\
canonical_directory: {}\n\
canonical_parts: {}\n\
serving_directory: {}\n\
octree_depth: {}\n\
bounds_min: {},{},{}\n\
bounds_max: {},{},{}\n\
parallax_filter_mas: {}\n\
ingestion_started_at: {}\n\
ingestion_finished_at: {}\n\
rows_seen: {}\n\
rows_with_positive_parallax: {}\n\
rows_after_parallax_filter: {}\n\
rows_in_bounds: {}\n",
        metadata.source_bulk_url,
        metadata.source_bulk_md5,
        metadata.input_name,
        metadata.canonical_directory,
        canonical_parts,
        metadata.serving_directory,
        metadata.octree_depth,
        metadata.bounds.min[0],
        metadata.bounds.min[1],
        metadata.bounds.min[2],
        metadata.bounds.max[0],
        metadata.bounds.max[1],
        metadata.bounds.max[2],
        parallax_filter_mas,
        metadata.ingestion_started_at,
        metadata.ingestion_finished_at,
        metadata.counts.rows_seen,
        metadata.counts.rows_with_positive_parallax,
        metadata.counts.rows_after_parallax_filter,
        metadata.counts.rows_in_bounds,
    )
    .into_bytes()
}

pub fn read_run_metadata(path: &Path) -> Result<RunMetadata> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    decode_run_metadata(&bytes).with_context(|| format!("failed to parse {}", path.display()))
}

pub fn write_run_metadata(path: &Path, metadata: &RunMetadata) -> Result<()> {
    let file =
        fs::File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writer
        .write_all(&encode_run_metadata(metadata))
        .with_context(|| format!("failed to write {}", path.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", path.display()))?;
    Ok(())
}

pub fn decode_octree_index(bytes: &[u8]) -> Result<OctreeIndex> {
    if bytes.len() < OCTREE_HEADER_SIZE {
        bail!(
            "octree index is too short: expected at least {OCTREE_HEADER_SIZE} bytes, got {}",
            bytes.len()
        );
    }
    if bytes[0..8] != OCTREE_INDEX_MAGIC {
        bail!("invalid octree index magic");
    }

    let version = read_u32(bytes, 8)?;
    if version != OCTREE_INDEX_VERSION {
        bail!("unsupported octree index version {version}");
    }

    let depth = read_u32(bytes, 12)?;
    let depth = u8::try_from(depth).context("octree depth does not fit in u8")?;
    let bounds = Bounds3 {
        min: [
            read_f32(bytes, 16)?,
            read_f32(bytes, 20)?,
            read_f32(bytes, 24)?,
        ],
        max: [
            read_f32(bytes, 28)?,
            read_f32(bytes, 32)?,
            read_f32(bytes, 36)?,
        ],
    };
    let leaf_count = read_u32(bytes, 40)? as usize;
    let expected_len = OCTREE_HEADER_SIZE + leaf_count * 4;
    if bytes.len() != expected_len {
        bail!(
            "octree index length {} does not match expected length {}",
            bytes.len(),
            expected_len
        );
    }

    let mut leaves = Vec::with_capacity(leaf_count);
    for chunk in bytes[OCTREE_HEADER_SIZE..].chunks_exact(4) {
        leaves.push(u32::from_le_bytes(chunk.try_into().unwrap()));
    }

    Ok(OctreeIndex {
        depth,
        bounds,
        leaves,
    })
}

pub fn encode_octree_index(index: &OctreeIndex) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(OCTREE_HEADER_SIZE + index.leaves.len() * 4);
    bytes.extend_from_slice(&OCTREE_INDEX_MAGIC);
    bytes.extend_from_slice(&OCTREE_INDEX_VERSION.to_le_bytes());
    bytes.extend_from_slice(&(index.depth as u32).to_le_bytes());
    for value in index.bounds.min {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    for value in index.bounds.max {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes.extend_from_slice(&(index.leaves.len() as u32).to_le_bytes());
    for morton in &index.leaves {
        bytes.extend_from_slice(&morton.to_le_bytes());
    }
    bytes
}

pub fn read_octree_index(path: &Path) -> Result<OctreeIndex> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    decode_octree_index(&bytes).with_context(|| format!("failed to parse {}", path.display()))
}

pub fn write_octree_index(path: &Path, index: &OctreeIndex) -> Result<()> {
    let file =
        fs::File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writer
        .write_all(&encode_octree_index(index))
        .with_context(|| format!("failed to write {}", path.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", path.display()))?;
    Ok(())
}

pub fn row_count(path: &Path, row_size: u64) -> Result<u64> {
    let len = fs::metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?
        .len();
    if len % row_size != 0 {
        bail!(
            "file {} length {} is not a multiple of row size {}",
            path.display(),
            len,
            row_size
        );
    }
    Ok(len / row_size)
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
        .with_context(|| format!("failed to flush {}", path.display()))?;
    Ok(())
}

pub fn write_serving_rows(path: &Path, rows: &[ServingRow]) -> Result<()> {
    let file =
        fs::File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    for row in rows {
        writer
            .write_all(&serving_row_bytes(row))
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", path.display()))?;
    Ok(())
}

pub fn validate_run_layout(root: &Path, metadata: &RunMetadata, index: &OctreeIndex) -> Result<()> {
    if metadata.octree_depth != index.depth {
        bail!(
            "metadata depth {} does not match index depth {}",
            metadata.octree_depth,
            index.depth
        );
    }
    if metadata.bounds != index.bounds {
        bail!("metadata bounds do not match index bounds");
    }

    let canonical_root = root.join(&metadata.canonical_directory);
    let serving_root = root.join(&metadata.serving_directory);

    let mut canonical_rows = 0;
    for part in &metadata.canonical_parts {
        canonical_rows += row_count(&canonical_root.join(part), CANONICAL_ROW_SIZE)?;
    }

    let mut serving_rows = 0;
    let mut expected_files = std::collections::BTreeSet::new();
    for morton in &index.leaves {
        let filename = leaf_filename(*morton);
        expected_files.insert(filename.clone());
        serving_rows += row_count(&serving_root.join(filename), SERVING_ROW_SIZE)?;
    }

    let mut actual_files = std::collections::BTreeSet::new();
    for entry in fs::read_dir(&serving_root)
        .with_context(|| format!("failed to read {}", serving_root.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read {}", serving_root.display()))?;
        if entry
            .file_type()
            .with_context(|| format!("failed to stat {}", entry.path().display()))?
            .is_file()
        {
            actual_files.insert(entry.file_name().to_string_lossy().into_owned());
        }
    }

    if actual_files != expected_files {
        bail!("serving directory contents do not match index leaves");
    }
    if canonical_rows != serving_rows {
        bail!(
            "canonical rows {} do not match serving rows {}",
            canonical_rows,
            serving_rows
        );
    }
    if canonical_rows != metadata.counts.rows_in_bounds {
        bail!(
            "metadata rows_in_bounds {} does not match stored rows {}",
            metadata.counts.rows_in_bounds,
            canonical_rows
        );
    }

    Ok(())
}

fn decode_rows<T, F>(bytes: &[u8], row_size: u64, decode: F) -> Result<Vec<T>>
where
    F: Fn(&[u8]) -> T,
{
    if bytes.len() as u64 % row_size != 0 {
        bail!(
            "buffer length {} is not a multiple of row size {}",
            bytes.len(),
            row_size
        );
    }

    let mut rows = Vec::with_capacity(bytes.len() / row_size as usize);
    for chunk in bytes.chunks_exact(row_size as usize) {
        rows.push(decode(chunk));
    }
    Ok(rows)
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

fn decode_serving_row(chunk: &[u8]) -> ServingRow {
    ServingRow {
        source_id: u64::from_le_bytes(chunk[0..8].try_into().unwrap()),
        x: f32::from_le_bytes(chunk[8..12].try_into().unwrap()),
        y: f32::from_le_bytes(chunk[12..16].try_into().unwrap()),
        z: f32::from_le_bytes(chunk[16..20].try_into().unwrap()),
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn round_trips_binary_rows() {
        let dir = tempdir().unwrap();
        let canonical_path = dir.path().join("canonical.bin");
        let serving_path = dir.path().join("serving.bin");

        let canonical_rows = vec![
            CanonicalRow {
                source_id: 1,
                ra: 2.0,
                dec: 3.0,
                parallax: 4.0,
                parallax_error: 0.5,
                phot_g_mean_mag: 12.5,
                bp_rp: 0.25,
            },
            CanonicalRow {
                source_id: 5,
                ra: 6.0,
                dec: 7.0,
                parallax: 8.0,
                parallax_error: 1.5,
                phot_g_mean_mag: 13.5,
                bp_rp: 0.75,
            },
        ];
        let serving_rows = vec![
            ServingRow {
                source_id: 9,
                x: 1.5,
                y: 2.5,
                z: 3.5,
            },
            ServingRow {
                source_id: 10,
                x: 4.5,
                y: 5.5,
                z: 6.5,
            },
        ];

        write_canonical_rows(&canonical_path, &canonical_rows).unwrap();
        write_serving_rows(&serving_path, &serving_rows).unwrap();

        assert_eq!(
            read_canonical_rows(&canonical_path).unwrap(),
            canonical_rows
        );
        assert_eq!(read_serving_rows(&serving_path).unwrap(), serving_rows);
        assert_eq!(row_count(&canonical_path, CANONICAL_ROW_SIZE).unwrap(), 2);
        assert_eq!(row_count(&serving_path, SERVING_ROW_SIZE).unwrap(), 2);
    }

    #[test]
    fn round_trips_control_files() {
        let dir = tempdir().unwrap();
        let metadata_path = dir.path().join(METADATA_FILENAME);
        let index_path = dir.path().join(OCTREE_INDEX_FILENAME);
        let metadata = RunMetadata {
            source_bulk_url: "https://example.test/input.csv.gz".to_string(),
            source_bulk_md5: "0123456789abcdef0123456789abcdef".to_string(),
            input_name: "input.csv.gz".to_string(),
            canonical_directory: "canonical/786097-786431".to_string(),
            canonical_parts: vec!["part-000.bin".to_string()],
            serving_directory: "serving/depth=6".to_string(),
            octree_depth: 6,
            bounds: Bounds3 {
                min: [-1.0, -2.0, -3.0],
                max: [1.0, 2.0, 3.0],
            },
            parallax_filter_mas: Some(10.0),
            ingestion_started_at: "2026-04-10T00:00:00Z".to_string(),
            ingestion_finished_at: "2026-04-10T00:00:01Z".to_string(),
            counts: RunCounts {
                rows_seen: 100,
                rows_with_positive_parallax: 80,
                rows_after_parallax_filter: 7,
                rows_in_bounds: 6,
            },
        };
        let index = OctreeIndex {
            depth: 6,
            bounds: metadata.bounds,
            leaves: vec![1, 7, 42],
        };

        write_run_metadata(&metadata_path, &metadata).unwrap();
        write_octree_index(&index_path, &index).unwrap();

        assert_eq!(read_run_metadata(&metadata_path).unwrap(), metadata);
        assert_eq!(read_octree_index(&index_path).unwrap(), index);
    }
}
