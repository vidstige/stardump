// starcloud.bin — LOD star cloud artifact served alongside the legacy
// index.octree. Every node (leaf and internal) owns a range in a shared point
// table. Leaves hold all their stars at true luminosity; internal nodes hold a
// uniform random subsample of K descendants with luminosities boosted by
// |D|/K, so the subsample's total flux equals the descendants' total flux.
//
// File layout:
//   header (32 bytes)
//   node table (20 bytes × node_count)
//   point table (20 bytes × point_count)

use anyhow::{Result, anyhow, bail};

use crate::octree::Bounds3;
use crate::vec3::Vec3;

pub const STARCLOUD_FILENAME: &str = "starcloud.bin";
pub const STARCLOUD_MAGIC: [u8; 8] = *b"STRCLD\0\0";
pub const STARCLOUD_VERSION: u16 = 1;
pub const STARCLOUD_HEADER_SIZE: usize = 32;
pub const STARCLOUD_NODE_SIZE: u64 = 20;
pub const STARCLOUD_POINT_SIZE: u64 = 20;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StarcloudNode {
    pub child_mask: u8,
    pub first_child: u32,
    pub point_first: u32,
    pub point_count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StarcloudPoint {
    pub position: Vec3,
    pub luminosity: f32,
    pub bp_rp: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StarcloudIndex {
    pub depth: u8,
    pub half_extent_pc: f32,
    pub nodes: Vec<StarcloudNode>,
    pub points: Vec<StarcloudPoint>,
}

impl StarcloudIndex {
    pub fn bounds(&self) -> Bounds3 {
        let e = self.half_extent_pc;
        Bounds3 {
            min: Vec3 { x: -e, y: -e, z: -e },
            max: Vec3 { x: e, y: e, z: e },
        }
    }

    pub fn file_size(&self) -> u64 {
        STARCLOUD_HEADER_SIZE as u64
            + self.nodes.len() as u64 * STARCLOUD_NODE_SIZE
            + self.points.len() as u64 * STARCLOUD_POINT_SIZE
    }
}

fn node_bytes(node: &StarcloudNode) -> [u8; STARCLOUD_NODE_SIZE as usize] {
    let mut bytes = [0_u8; STARCLOUD_NODE_SIZE as usize];
    bytes[0] = node.child_mask;
    // bytes 1..4: padding
    bytes[4..8].copy_from_slice(&node.first_child.to_le_bytes());
    bytes[8..12].copy_from_slice(&node.point_first.to_le_bytes());
    bytes[12..16].copy_from_slice(&node.point_count.to_le_bytes());
    // bytes 16..20: reserved
    bytes
}

fn point_bytes(point: &StarcloudPoint) -> [u8; STARCLOUD_POINT_SIZE as usize] {
    let mut bytes = [0_u8; STARCLOUD_POINT_SIZE as usize];
    bytes[0..4].copy_from_slice(&point.position.x.to_le_bytes());
    bytes[4..8].copy_from_slice(&point.position.y.to_le_bytes());
    bytes[8..12].copy_from_slice(&point.position.z.to_le_bytes());
    bytes[12..16].copy_from_slice(&point.luminosity.to_le_bytes());
    bytes[16..20].copy_from_slice(&point.bp_rp.to_le_bytes());
    bytes
}

fn decode_node(chunk: &[u8]) -> StarcloudNode {
    StarcloudNode {
        child_mask: chunk[0],
        first_child: u32::from_le_bytes(chunk[4..8].try_into().unwrap()),
        point_first: u32::from_le_bytes(chunk[8..12].try_into().unwrap()),
        point_count: u32::from_le_bytes(chunk[12..16].try_into().unwrap()),
    }
}

fn decode_point(chunk: &[u8]) -> StarcloudPoint {
    StarcloudPoint {
        position: Vec3 {
            x: f32::from_le_bytes(chunk[0..4].try_into().unwrap()),
            y: f32::from_le_bytes(chunk[4..8].try_into().unwrap()),
            z: f32::from_le_bytes(chunk[8..12].try_into().unwrap()),
        },
        luminosity: f32::from_le_bytes(chunk[12..16].try_into().unwrap()),
        bp_rp: f32::from_le_bytes(chunk[16..20].try_into().unwrap()),
    }
}

pub fn encode_starcloud(index: &StarcloudIndex) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(index.file_size() as usize);
    bytes.extend_from_slice(&STARCLOUD_MAGIC);
    bytes.extend_from_slice(&STARCLOUD_VERSION.to_le_bytes());
    bytes.push(index.depth);
    bytes.push(0);
    bytes.extend_from_slice(&index.half_extent_pc.to_le_bytes());
    bytes.extend_from_slice(&(index.nodes.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(index.points.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&[0_u8; 4]); // reserved
    for node in &index.nodes {
        bytes.extend_from_slice(&node_bytes(node));
    }
    for point in &index.points {
        bytes.extend_from_slice(&point_bytes(point));
    }
    bytes
}

pub fn decode_starcloud(bytes: &[u8]) -> Result<StarcloudIndex> {
    if bytes.len() < STARCLOUD_HEADER_SIZE {
        bail!("starcloud header is too short: {} < {STARCLOUD_HEADER_SIZE}", bytes.len());
    }
    if bytes[0..8] != STARCLOUD_MAGIC {
        bail!("invalid starcloud magic");
    }
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    if version != STARCLOUD_VERSION {
        bail!("unsupported starcloud version {version}");
    }
    let depth = bytes[10];
    let half_extent_pc = f32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let node_count = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let point_count = u64::from_le_bytes(bytes[20..28].try_into().unwrap()) as usize;

    let nodes_start = STARCLOUD_HEADER_SIZE;
    let nodes_end = nodes_start + node_count * STARCLOUD_NODE_SIZE as usize;
    let points_end = nodes_end + point_count * STARCLOUD_POINT_SIZE as usize;
    if bytes.len() != points_end {
        bail!("starcloud size {} does not match expected {}", bytes.len(), points_end);
    }
    let node_bytes = bytes.get(nodes_start..nodes_end)
        .ok_or_else(|| anyhow!("starcloud node table is truncated"))?;
    let point_bytes = bytes.get(nodes_end..points_end)
        .ok_or_else(|| anyhow!("starcloud point table is truncated"))?;
    let nodes = node_bytes
        .chunks_exact(STARCLOUD_NODE_SIZE as usize)
        .map(decode_node)
        .collect();
    let points = point_bytes
        .chunks_exact(STARCLOUD_POINT_SIZE as usize)
        .map(decode_point)
        .collect();
    Ok(StarcloudIndex { depth, half_extent_pc, nodes, points })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starcloud_round_trip() {
        let index = StarcloudIndex {
            depth: 5,
            half_extent_pc: 1234.5,
            nodes: vec![
                StarcloudNode { child_mask: 0b0000_0011, first_child: 1, point_first: 0, point_count: 2 },
                StarcloudNode { child_mask: 0, first_child: 0, point_first: 2, point_count: 1 },
                StarcloudNode { child_mask: 0, first_child: 0, point_first: 3, point_count: 1 },
            ],
            points: vec![
                StarcloudPoint { position: Vec3 { x: 1.0, y: 2.0, z: 3.0 }, luminosity: 4.5, bp_rp: 0.5 },
                StarcloudPoint { position: Vec3 { x: -1.0, y: -2.0, z: -3.0 }, luminosity: 2.5, bp_rp: f32::NAN },
                StarcloudPoint { position: Vec3 { x: 0.0, y: 0.0, z: 0.0 }, luminosity: 1.0, bp_rp: 1.5 },
                StarcloudPoint { position: Vec3 { x: 5.0, y: 5.0, z: 5.0 }, luminosity: 7.0, bp_rp: 2.0 },
            ],
        };
        let bytes = encode_starcloud(&index);
        assert_eq!(bytes.len() as u64, index.file_size());
        let decoded = decode_starcloud(&bytes).unwrap();
        assert_eq!(decoded.depth, index.depth);
        assert_eq!(decoded.half_extent_pc, index.half_extent_pc);
        assert_eq!(decoded.nodes, index.nodes);
        assert_eq!(decoded.points.len(), index.points.len());
        for (a, b) in decoded.points.iter().zip(index.points.iter()) {
            assert_eq!(a.position, b.position);
            assert_eq!(a.luminosity, b.luminosity);
            assert_eq!(a.bp_rp.is_nan(), b.bp_rp.is_nan());
            if !a.bp_rp.is_nan() {
                assert_eq!(a.bp_rp, b.bp_rp);
            }
        }
    }

    #[test]
    fn rejects_wrong_magic() {
        let mut bytes = vec![0_u8; STARCLOUD_HEADER_SIZE];
        bytes[0..8].copy_from_slice(b"BOGUSMAG");
        assert!(decode_starcloud(&bytes).is_err());
    }
}
