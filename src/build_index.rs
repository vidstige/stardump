use std::array;
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::formats::{
    CANONICAL_ROOT, CanonicalRow, METADATA_FILENAME, OCTREE_INDEX_FILENAME, PackedOctreeIndex,
    PackedOctreeNode, PackedPoint, SourceMetadata, decode_canonical_rows, decode_source_metadata,
    encode_packed_octree, encode_packed_points, quantize_point,
};
use crate::octree::{Bounds3, OctreeConfig};
use crate::vec3::Vec3;
use crate::quality::{DEFAULT_PARALLAX_QUALITY_THRESHOLD, maximum_distance_pc_for_quality};
use crate::storage::{
    list_relative_files_recursive, local_path, validate_canonical_layout,
    validate_packed_index_layout,
};

const TEMP_POINT_SIZE: u64 = 20;
const TEMP_LEAF_ROOT: &str = ".tmp-leaves";

pub const DEFAULT_DEPTH: u8 = 7;
pub const DEFAULT_BOUND_PC: f32 = 1_000.0 / (DEFAULT_PARALLAX_QUALITY_THRESHOLD * 0.025);
pub const DEFAULT_BOUNDS: Bounds3 = Bounds3 {
    min: Vec3 { x: -DEFAULT_BOUND_PC, y: -DEFAULT_BOUND_PC, z: -DEFAULT_BOUND_PC },
    max: Vec3 { x: DEFAULT_BOUND_PC, y: DEFAULT_BOUND_PC, z: DEFAULT_BOUND_PC },
};

#[derive(Clone, Debug)]
pub struct BuildIndexConfig {
    pub data_root: String,
    pub output_root: String,
    pub octree_depth: u8,
    pub bounds: Bounds3,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BuildIndexResult {
    pub index: PackedOctreeIndex,
    pub sources_seen: usize,
    pub rows_in_bounds: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TempPoint {
    source_id: u64,
    position: Vec3,
}

#[derive(Clone, Copy, Debug)]
struct LeafInfo {
    morton: u32,
    point_start: u32,
    point_count: u32,
}

#[derive(Debug)]
struct TreeNode {
    children: [Option<Box<TreeNode>>; 8],
    point_start: u32,
    point_count: u32,
}

fn temp_point_bytes(point: &TempPoint) -> [u8; TEMP_POINT_SIZE as usize] {
    let mut bytes = [0_u8; TEMP_POINT_SIZE as usize];
    bytes[0..8].copy_from_slice(&point.source_id.to_le_bytes());
    bytes[8..12].copy_from_slice(&point.position.x.to_le_bytes());
    bytes[12..16].copy_from_slice(&point.position.y.to_le_bytes());
    bytes[16..20].copy_from_slice(&point.position.z.to_le_bytes());
    bytes
}

fn decode_temp_point(chunk: &[u8]) -> TempPoint {
    TempPoint {
        source_id: u64::from_le_bytes(chunk[0..8].try_into().unwrap()),
        position: Vec3 {
            x: f32::from_le_bytes(chunk[8..12].try_into().unwrap()),
            y: f32::from_le_bytes(chunk[12..16].try_into().unwrap()),
            z: f32::from_le_bytes(chunk[16..20].try_into().unwrap()),
        },
    }
}

fn centered_half_extent(bounds: Bounds3) -> Result<f32> {
    let half_extent = bounds.max.x;
    if half_extent <= 0.0
        || bounds.min.x != -half_extent
        || bounds.min.y != -half_extent
        || bounds.min.z != -half_extent
        || bounds.max.y != half_extent
        || bounds.max.z != half_extent
    {
        bail!("bounds must be a root-centered cube");
    }
    Ok(half_extent)
}

fn cartesian_coordinates(ra_deg: f32, dec_deg: f32, parallax_mas: f32) -> Vec3 {
    let distance_pc = 1_000.0_f64 / parallax_mas as f64;
    let ra = (ra_deg as f64).to_radians();
    let dec = (dec_deg as f64).to_radians();
    Vec3 {
        x: (distance_pc * dec.cos() * ra.cos()) as f32,
        y: (distance_pc * dec.cos() * ra.sin()) as f32,
        z: (distance_pc * dec.sin()) as f32,
    }
}

fn temp_leaf_root(output_root: &Path) -> PathBuf {
    output_root.join(TEMP_LEAF_ROOT)
}

fn temp_leaf_path(output_root: &Path, morton: u32) -> PathBuf {
    temp_leaf_root(output_root).join(format!("leaf-{morton:08}.bin"))
}

fn append_temp_points(path: &Path, points: &[TempPoint]) -> Result<()> {
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    for point in points {
        writer
            .write_all(&temp_point_bytes(point))
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", path.display()))
}

fn read_temp_points(path: &Path) -> Result<Vec<TempPoint>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    if bytes.len() as u64 % TEMP_POINT_SIZE != 0 {
        bail!(
            "temporary leaf size {} is not a multiple of row size {}",
            bytes.len(),
            TEMP_POINT_SIZE
        );
    }
    Ok(bytes
        .chunks_exact(TEMP_POINT_SIZE as usize)
        .map(decode_temp_point)
        .collect())
}

fn is_leaf(node: &TreeNode) -> bool {
    node.children.iter().all(Option::is_none)
}

fn insert_leaf(node: &mut TreeNode, depth: u8, morton: u32, point_start: u32, point_count: u32) {
    let mut current = node;
    for level in (0..depth).rev() {
        let child = ((morton >> (level * 3)) & 0b111) as usize;
        current = current.children[child]
            .get_or_insert_with(|| Box::new(TreeNode::default()))
            .as_mut();
    }
    current.point_start = point_start;
    current.point_count = point_count;
}

fn flatten_tree_at(node: &TreeNode, index: usize, nodes: &mut Vec<PackedOctreeNode>) {
    if is_leaf(node) {
        nodes[index] = PackedOctreeNode {
            child_mask: 0,
            first: node.point_start,
            count: node.point_count,
        };
        return;
    }

    let mut child_mask = 0_u8;
    let first_child = nodes.len() as u32;
    let mut child_indices = [usize::MAX; 8];
    for child in 0..8 {
        let Some(_) = node.children[child].as_ref() else {
            continue;
        };
        child_mask |= 1 << child;
        child_indices[child] = nodes.len();
        nodes.push(PackedOctreeNode {
            child_mask: 0,
            first: 0,
            count: 0,
        });
    }

    nodes[index] = PackedOctreeNode {
        child_mask,
        first: first_child,
        count: 0,
    };

    for child in 0..8 {
        let Some(next) = node.children[child].as_ref() else {
            continue;
        };
        flatten_tree_at(next, child_indices[child], nodes);
    }
}

fn flatten_tree(node: &TreeNode) -> Vec<PackedOctreeNode> {
    let mut nodes = vec![PackedOctreeNode {
        child_mask: 0,
        first: 0,
        count: 0,
    }];
    flatten_tree_at(node, 0, &mut nodes);
    nodes
}

fn build_octree_nodes(leaf_infos: &[LeafInfo], depth: u8) -> Vec<PackedOctreeNode> {
    let mut root = TreeNode::default();
    for leaf in leaf_infos {
        insert_leaf(
            &mut root,
            depth,
            leaf.morton,
            leaf.point_start,
            leaf.point_count,
        );
    }
    flatten_tree(&root)
}

fn leaf_infos(leaf_counts: &BTreeMap<u32, u32>) -> Result<Vec<LeafInfo>> {
    let mut next_point = 0_u32;
    let mut leaves = Vec::with_capacity(leaf_counts.len());
    for (&morton, &point_count) in leaf_counts {
        let point_start = next_point;
        next_point = next_point
            .checked_add(point_count)
            .ok_or_else(|| anyhow::anyhow!("point table exceeds u32 indexing"))?;
        leaves.push(LeafInfo {
            morton,
            point_start,
            point_count,
        });
    }
    Ok(leaves)
}

fn points_for_canonical_part(
    octree: OctreeConfig,
    rows: Vec<CanonicalRow>,
) -> (BTreeMap<u32, Vec<TempPoint>>, u64) {
    let mut points_by_leaf = BTreeMap::<u32, Vec<TempPoint>>::new();
    let mut rows_in_bounds = 0_u64;

    for row in rows {
        let position = cartesian_coordinates(row.ra, row.dec, row.parallax);
        let Some(morton) = octree.morton_for_point(position) else {
            continue;
        };
        rows_in_bounds += 1;
        points_by_leaf.entry(morton).or_default().push(TempPoint {
            source_id: row.source_id,
            position,
        });
    }

    (points_by_leaf, rows_in_bounds)
}

fn prepare_output_root(output_root: &Path) -> Result<()> {
    fs::create_dir_all(output_root)
        .with_context(|| format!("failed to create {}", output_root.display()))?;

    let index_path = output_root.join(OCTREE_INDEX_FILENAME);
    if index_path.exists() {
        fs::remove_file(&index_path)
            .with_context(|| format!("failed to remove {}", index_path.display()))?;
    }

    let temp_root = temp_leaf_root(output_root);
    if temp_root.exists() {
        fs::remove_dir_all(&temp_root)
            .with_context(|| format!("failed to remove {}", temp_root.display()))?;
    }
    fs::create_dir_all(&temp_root)
        .with_context(|| format!("failed to create {}", temp_root.display()))
}

fn write_packed_index(
    output_root: &Path,
    octree: OctreeConfig,
    half_extent_pc: f32,
    leaf_infos: &[LeafInfo],
) -> Result<PackedOctreeIndex> {
    let nodes = build_octree_nodes(leaf_infos, octree.depth);
    let point_count = leaf_infos
        .iter()
        .map(|leaf| leaf.point_count as u64)
        .sum::<u64>();
    let index = PackedOctreeIndex {
        depth: octree.depth,
        half_extent_pc,
        point_count,
        nodes,
    };

    let output_path = output_root.join(OCTREE_INDEX_FILENAME);
    let file = fs::File::create(&output_path)
        .with_context(|| format!("failed to create {}", output_path.display()))?;
    let mut writer = BufWriter::new(file);
    writer
        .write_all(&encode_packed_octree(&index))
        .with_context(|| format!("failed to write {}", output_path.display()))?;

    for leaf in leaf_infos {
        let temp_path = temp_leaf_path(output_root, leaf.morton);
        let mut points = read_temp_points(&temp_path)?;
        points.sort_by_key(|point| point.source_id);
        let leaf_bounds = octree.bounds.leaf_bounds(octree.depth, leaf.morton);
        let packed = points
            .iter()
            .map(|point| quantize_point(leaf_bounds, point.position, point.source_id))
            .collect::<Vec<PackedPoint>>();
        writer
            .write_all(&encode_packed_points(&packed))
            .with_context(|| format!("failed to write {}", output_path.display()))?;
    }

    writer
        .flush()
        .with_context(|| format!("failed to flush {}", output_path.display()))?;
    fs::remove_dir_all(temp_leaf_root(output_root))
        .with_context(|| format!("failed to remove {}", temp_leaf_root(output_root).display()))?;
    Ok(index)
}

pub fn bounds_for_quality_threshold(minimum_quality: f32) -> Bounds3 {
    let bound = maximum_distance_pc_for_quality(minimum_quality);
    Bounds3 {
        min: Vec3 { x: -bound, y: -bound, z: -bound },
        max: Vec3 { x: bound, y: bound, z: bound },
    }
}

pub fn load_source_metadata(data_root: &Path) -> Result<Vec<SourceMetadata>> {
    let canonical_root = data_root.join(CANONICAL_ROOT);
    let mut metadata = Vec::new();
    for relative in list_relative_files_recursive(&canonical_root)? {
        if !relative.ends_with(&format!("/{METADATA_FILENAME}")) {
            continue;
        }

        let path = canonical_root.join(&relative);
        let source_metadata = decode_source_metadata(
            &fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", path.display()))?;
        validate_canonical_layout(data_root, &source_metadata)?;
        metadata.push(source_metadata);
    }

    metadata.sort_by(|left, right| left.input_name.cmp(&right.input_name));
    Ok(metadata)
}

pub fn read_canonical_part_rows(
    data_root: &Path,
    source: &SourceMetadata,
    part: &str,
) -> Result<Vec<CanonicalRow>> {
    let canonical_root = data_root.join(&source.canonical_directory);
    decode_canonical_rows(
        &fs::read(canonical_root.join(part))
            .with_context(|| format!("failed to read {}", canonical_root.join(part).display()))?,
    )
}

pub struct IndexBuilder {
    output_root: PathBuf,
    octree: OctreeConfig,
    half_extent_pc: f32,
    leaf_counts: BTreeMap<u32, u32>,
    rows_in_bounds: u64,
}

impl TreeNode {
    fn default_children() -> [Option<Box<TreeNode>>; 8] {
        array::from_fn(|_| None)
    }
}

impl Default for TreeNode {
    fn default() -> Self {
        Self {
            children: Self::default_children(),
            point_start: 0,
            point_count: 0,
        }
    }
}

impl IndexBuilder {
    pub fn new(output_root: &Path, octree_depth: u8, bounds: Bounds3) -> Result<Self> {
        prepare_output_root(output_root)?;
        Ok(Self {
            output_root: output_root.to_path_buf(),
            octree: OctreeConfig {
                depth: octree_depth,
                bounds,
            },
            half_extent_pc: centered_half_extent(bounds)?,
            leaf_counts: BTreeMap::new(),
            rows_in_bounds: 0,
        })
    }

    pub fn append_rows(&mut self, rows: Vec<CanonicalRow>) -> Result<()> {
        let (mut points_by_leaf, rows_in_bounds) = points_for_canonical_part(self.octree, rows);
        self.rows_in_bounds += rows_in_bounds;

        for (morton, points) in &mut points_by_leaf {
            append_temp_points(&temp_leaf_path(&self.output_root, *morton), points)?;
            *self.leaf_counts.entry(*morton).or_insert(0) += points.len() as u32;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<(PackedOctreeIndex, u64)> {
        let leaf_infos = leaf_infos(&self.leaf_counts)?;
        let index = write_packed_index(
            &self.output_root,
            self.octree,
            self.half_extent_pc,
            &leaf_infos,
        )?;
        Ok((index, self.rows_in_bounds))
    }
}

pub fn run_build_index(config: BuildIndexConfig) -> Result<BuildIndexResult> {
    if config.octree_depth == 0 || config.octree_depth > 10 {
        bail!("octree depth must be between 1 and 10");
    }

    let data_root = local_path(&config.data_root)?;
    let output_root = local_path(&config.output_root)?;
    let metadata = load_source_metadata(&data_root)?;
    if metadata.is_empty() {
        bail!("no canonical source metadata found under {CANONICAL_ROOT}");
    }

    let mut builder = IndexBuilder::new(&output_root, config.octree_depth, config.bounds)?;
    for source in &metadata {
        for part in &source.canonical_parts {
            builder.append_rows(read_canonical_part_rows(&data_root, source, part)?)?;
        }
    }
    let (index, rows_in_bounds) = builder.finish()?;
    validate_packed_index_layout(&output_root, &index)?;
    Ok(BuildIndexResult {
        index,
        sources_seen: metadata.len(),
        rows_in_bounds,
    })
}
