use std::fs;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::octree::Bounds3;

pub const CANONICAL_ROW_SIZE: u64 = 32;
pub const SERVING_ROW_SIZE: u64 = 20;
pub const LEAF_FILENAME_WIDTH: usize = 8;

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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunCounts {
    pub rows_seen: u64,
    pub rows_with_positive_parallax: u64,
    pub rows_after_parallax_filter: u64,
    pub rows_in_bounds: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

pub fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let file =
        fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).with_context(|| format!("failed to parse {}", path.display()))
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let file =
        fs::File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)
        .with_context(|| format!("failed to write {}", path.display()))?;
    writer
        .write_all(b"\n")
        .with_context(|| format!("failed to finalize {}", path.display()))?;
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
}
