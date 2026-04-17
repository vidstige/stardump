use std::collections::BTreeMap;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};

use crate::octree::Bounds3;
use crate::vec3::Vec3;

pub const CANONICAL_ROOT: &str = "canonical";
pub const CANONICAL_ROW_SIZE: u64 = 32;
pub const METADATA_FILENAME: &str = "metadata.txt";
pub const OCTREE_INDEX_FILENAME: &str = "index.octree";
pub const PACKED_OCTREE_NODE_SIZE: u64 = 12;
pub const PACKED_POINT_SIZE: u64 = 14;

const METADATA_MAGIC: &str = "STARDUMP-METADATA 1";
const PACKED_OCTREE_MAGIC: [u8; 8] = *b"OCTPACK\0";
const PACKED_OCTREE_VERSION: u16 = 1;
const PACKED_OCTREE_HEADER_SIZE: usize = 28;
const QUANTIZATION_SCALE: f32 = 65_535.0;

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

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PackedPoint {
    pub source_id: u64,
    pub x_local: u16,
    pub y_local: u16,
    pub z_local: u16,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PackedOctreeNode {
    pub child_mask: u8,
    pub first: u32,
    pub count: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PackedOctreeIndex {
    pub depth: u8,
    pub half_extent_pc: f32,
    pub point_count: u64,
    pub nodes: Vec<PackedOctreeNode>,
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

fn packed_point_bytes(point: &PackedPoint) -> [u8; PACKED_POINT_SIZE as usize] {
    let mut bytes = [0_u8; PACKED_POINT_SIZE as usize];
    bytes[0..8].copy_from_slice(&point.source_id.to_le_bytes());
    bytes[8..10].copy_from_slice(&point.x_local.to_le_bytes());
    bytes[10..12].copy_from_slice(&point.y_local.to_le_bytes());
    bytes[12..14].copy_from_slice(&point.z_local.to_le_bytes());
    bytes
}

fn packed_octree_node_bytes(node: &PackedOctreeNode) -> [u8; PACKED_OCTREE_NODE_SIZE as usize] {
    let mut bytes = [0_u8; PACKED_OCTREE_NODE_SIZE as usize];
    bytes[0] = node.child_mask;
    bytes[4..8].copy_from_slice(&node.first.to_le_bytes());
    bytes[8..12].copy_from_slice(&node.count.to_le_bytes());
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

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    let end = offset + 2;
    let chunk = bytes
        .get(offset..end)
        .ok_or_else(|| anyhow!("buffer too short at offset {offset}"))?;
    Ok(u16::from_le_bytes(chunk.try_into().unwrap()))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = offset + 4;
    let chunk = bytes
        .get(offset..end)
        .ok_or_else(|| anyhow!("buffer too short at offset {offset}"))?;
    Ok(u32::from_le_bytes(chunk.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset + 8;
    let chunk = bytes
        .get(offset..end)
        .ok_or_else(|| anyhow!("buffer too short at offset {offset}"))?;
    Ok(u64::from_le_bytes(chunk.try_into().unwrap()))
}

fn read_f32(bytes: &[u8], offset: usize) -> Result<f32> {
    let end = offset + 4;
    let chunk = bytes
        .get(offset..end)
        .ok_or_else(|| anyhow!("buffer too short at offset {offset}"))?;
    Ok(f32::from_le_bytes(chunk.try_into().unwrap()))
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

fn decode_packed_point(chunk: &[u8]) -> PackedPoint {
    PackedPoint {
        source_id: u64::from_le_bytes(chunk[0..8].try_into().unwrap()),
        x_local: u16::from_le_bytes(chunk[8..10].try_into().unwrap()),
        y_local: u16::from_le_bytes(chunk[10..12].try_into().unwrap()),
        z_local: u16::from_le_bytes(chunk[12..14].try_into().unwrap()),
    }
}

fn decode_packed_octree_node(chunk: &[u8]) -> PackedOctreeNode {
    PackedOctreeNode {
        child_mask: chunk[0],
        first: u32::from_le_bytes(chunk[4..8].try_into().unwrap()),
        count: u32::from_le_bytes(chunk[8..12].try_into().unwrap()),
    }
}

fn parse_packed_octree_header(bytes: &[u8]) -> Result<(u8, f32, u32, u64)> {
    if bytes.len() < PACKED_OCTREE_HEADER_SIZE {
        bail!(
            "packed octree header is too short: {} < {}",
            bytes.len(),
            PACKED_OCTREE_HEADER_SIZE
        );
    }

    if bytes[0..8] != PACKED_OCTREE_MAGIC {
        bail!("invalid packed octree magic");
    }

    let version = read_u16(bytes, 8)?;
    if version != PACKED_OCTREE_VERSION {
        bail!("unsupported packed octree version {version}");
    }

    Ok((
        bytes[10],
        read_f32(bytes, 12)?,
        read_u32(bytes, 16)?,
        read_u64(bytes, 20)?,
    ))
}

fn quantize_axis(value: f32, min: f32, size: f32) -> u16 {
    let normalized = if size == 0.0 {
        0.0
    } else {
        ((value - min) / size).clamp(0.0, 1.0)
    };
    (normalized * QUANTIZATION_SCALE).round() as u16
}

fn dequantize_axis(value: u16, min: f32, size: f32) -> f32 {
    min + size * (value as f32 / QUANTIZATION_SCALE)
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

impl PackedOctreeIndex {
    pub fn bounds(&self) -> Bounds3 {
        let e = self.half_extent_pc;
        Bounds3 {
            min: Vec3 { x: -e, y: -e, z: -e },
            max: Vec3 { x: e, y: e, z: e },
        }
    }

    pub fn point_data_offset(&self) -> u64 {
        PACKED_OCTREE_HEADER_SIZE as u64 + self.nodes.len() as u64 * PACKED_OCTREE_NODE_SIZE
    }

    pub fn file_size(&self) -> u64 {
        self.point_data_offset() + self.point_count * PACKED_POINT_SIZE
    }
}

pub fn quantize_point(bounds: Bounds3, point: Vec3, source_id: u64) -> PackedPoint {
    let size = bounds.max.x - bounds.min.x;
    PackedPoint {
        source_id,
        x_local: quantize_axis(point.x, bounds.min.x, size),
        y_local: quantize_axis(point.y, bounds.min.y, size),
        z_local: quantize_axis(point.z, bounds.min.z, size),
    }
}

pub fn dequantize_point(bounds: Bounds3, point: &PackedPoint) -> Vec3 {
    let size = bounds.max.x - bounds.min.x;
    Vec3 {
        x: dequantize_axis(point.x_local, bounds.min.x, size),
        y: dequantize_axis(point.y_local, bounds.min.y, size),
        z: dequantize_axis(point.z_local, bounds.min.z, size),
    }
}

pub fn read_canonical_rows(path: &Path) -> Result<Vec<CanonicalRow>> {
    read_rows(path, CANONICAL_ROW_SIZE, decode_canonical_row)
}

pub fn decode_canonical_rows(bytes: &[u8]) -> Result<Vec<CanonicalRow>> {
    decode_rows(bytes, CANONICAL_ROW_SIZE, decode_canonical_row)
}

pub fn decode_packed_points(bytes: &[u8]) -> Result<Vec<PackedPoint>> {
    decode_rows(bytes, PACKED_POINT_SIZE, decode_packed_point)
}

pub fn encode_packed_points(points: &[PackedPoint]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(points.len() * PACKED_POINT_SIZE as usize);
    for point in points {
        bytes.extend_from_slice(&packed_point_bytes(point));
    }
    bytes
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

pub fn encode_packed_octree(index: &PackedOctreeIndex) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(index.point_data_offset() as usize);
    bytes.extend_from_slice(&PACKED_OCTREE_MAGIC);
    bytes.extend_from_slice(&PACKED_OCTREE_VERSION.to_le_bytes());
    bytes.push(index.depth);
    bytes.push(0);
    bytes.extend_from_slice(&index.half_extent_pc.to_le_bytes());
    bytes.extend_from_slice(&(index.nodes.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&index.point_count.to_le_bytes());
    for node in &index.nodes {
        bytes.extend_from_slice(&packed_octree_node_bytes(node));
    }
    bytes
}

pub fn decode_packed_octree(bytes: &[u8]) -> Result<PackedOctreeIndex> {
    let (depth, half_extent_pc, node_count, point_count) = parse_packed_octree_header(bytes)?;
    let nodes_start = PACKED_OCTREE_HEADER_SIZE;
    let nodes_end = nodes_start + node_count as usize * PACKED_OCTREE_NODE_SIZE as usize;
    let node_bytes = bytes
        .get(nodes_start..nodes_end)
        .ok_or_else(|| anyhow!("packed octree node table is truncated"))?;
    let nodes = decode_rows(
        node_bytes,
        PACKED_OCTREE_NODE_SIZE,
        decode_packed_octree_node,
    )?;
    let index = PackedOctreeIndex {
        depth,
        half_extent_pc,
        point_count,
        nodes,
    };
    if bytes.len() as u64 != index.file_size() {
        bail!(
            "packed octree size {} does not match expected {}",
            bytes.len(),
            index.file_size()
        );
    }
    Ok(index)
}

pub fn read_source_metadata(path: &Path) -> Result<SourceMetadata> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    decode_source_metadata(&bytes).with_context(|| format!("failed to parse {}", path.display()))
}

pub fn read_packed_octree(path: &Path) -> Result<PackedOctreeIndex> {
    let file =
        fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut header = [0_u8; PACKED_OCTREE_HEADER_SIZE];
    reader
        .read_exact(&mut header)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let (_, _, node_count, _) = parse_packed_octree_header(&header)?;
    let mut bytes = header.to_vec();
    let mut node_bytes = vec![0_u8; node_count as usize * PACKED_OCTREE_NODE_SIZE as usize];
    reader
        .read_exact(&mut node_bytes)
        .with_context(|| format!("failed to read {}", path.display()))?;
    bytes.extend_from_slice(&node_bytes);
    let mut index = decode_packed_octree_prefix(&bytes)?;
    let actual_size = fs::metadata(path)
        .with_context(|| format!("failed to stat {}", path.display()))?
        .len();
    if actual_size != index.file_size() {
        bail!(
            "packed octree size {} does not match expected {}",
            actual_size,
            index.file_size()
        );
    }
    index.nodes.shrink_to_fit();
    Ok(index)
}

pub fn decode_packed_octree_prefix(bytes: &[u8]) -> Result<PackedOctreeIndex> {
    let (depth, half_extent_pc, node_count, point_count) = parse_packed_octree_header(bytes)?;
    let nodes_start = PACKED_OCTREE_HEADER_SIZE;
    let nodes_end = nodes_start + node_count as usize * PACKED_OCTREE_NODE_SIZE as usize;
    let node_bytes = bytes
        .get(nodes_start..nodes_end)
        .ok_or_else(|| anyhow!("packed octree node table is truncated"))?;
    let nodes = decode_rows(
        node_bytes,
        PACKED_OCTREE_NODE_SIZE,
        decode_packed_octree_node,
    )?;
    Ok(PackedOctreeIndex {
        depth,
        half_extent_pc,
        point_count,
        nodes,
    })
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

    #[test]
    fn packed_octree_round_trip() {
        let index = PackedOctreeIndex {
            depth: 7,
            half_extent_pc: 4_000.0,
            point_count: 3,
            nodes: vec![
                PackedOctreeNode {
                    child_mask: 0b0000_0011,
                    first: 1,
                    count: 0,
                },
                PackedOctreeNode {
                    child_mask: 0,
                    first: 0,
                    count: 2,
                },
                PackedOctreeNode {
                    child_mask: 0,
                    first: 2,
                    count: 1,
                },
            ],
        };
        let mut bytes = encode_packed_octree(&index);
        bytes.extend_from_slice(&encode_packed_points(&[
            PackedPoint {
                source_id: 1,
                x_local: 2,
                y_local: 3,
                z_local: 4,
            },
            PackedPoint {
                source_id: 5,
                x_local: 6,
                y_local: 7,
                z_local: 8,
            },
            PackedPoint {
                source_id: 9,
                x_local: 10,
                y_local: 11,
                z_local: 12,
            },
        ]));

        assert_eq!(decode_packed_octree(&bytes).unwrap(), index);
    }

    #[test]
    fn packed_point_quantization_round_trip_stays_within_one_step() {
        let bounds = Bounds3 {
            min: Vec3 { x: 0.0, y: 0.0, z: 0.0 },
            max: Vec3 { x: 62.5, y: 62.5, z: 62.5 },
        };
        let original = Vec3 { x: 12.345, y: 23.456, z: 34.567 };
        let quantized = quantize_point(bounds, original, 7);
        let round_trip = dequantize_point(bounds, &quantized);
        let step = (bounds.max.x - bounds.min.x) / QUANTIZATION_SCALE;

        assert_eq!(quantized.source_id, 7);
        for (orig, rt) in [original.x, original.y, original.z]
            .iter()
            .zip([round_trip.x, round_trip.y, round_trip.z].iter())
        {
            assert!((rt - orig).abs() <= step);
        }
    }
}
