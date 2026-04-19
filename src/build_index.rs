use std::array;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::formats::{
    CANONICAL_ROOT, CanonicalRow, METADATA_FILENAME, OCTREE_INDEX_FILENAME,
    PACKED_OCTREE_HEADER_SIZE, PackedOctreeIndex, PackedOctreeNode, PackedPoint, SourceMetadata,
    compute_luminosity, decode_canonical_rows, decode_source_metadata, encode_packed_octree,
    encode_packed_octree_node_table, encode_packed_points, quantize_point,
};
use crate::octree::{Bounds3, OctreeConfig};
use crate::vec3::Vec3;
use crate::quality::{DEFAULT_PARALLAX_QUALITY_THRESHOLD, maximum_distance_pc_for_quality};
use crate::storage::{
    list_relative_files_recursive, local_path, validate_canonical_layout,
    validate_packed_index_layout,
};

const TEMP_POINT_SIZE: u64 = 28;
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
    bp_rp: f32,
    luminosity: f32,
}

struct AggregateData {
    total_luminosity: f32,
    weighted_bp_rp_sum: f32,
    weighted_pos: Vec3,
    star_count: u32,
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
    bytes[20..24].copy_from_slice(&point.bp_rp.to_le_bytes());
    bytes[24..28].copy_from_slice(&point.luminosity.to_le_bytes());
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
        bp_rp: f32::from_le_bytes(chunk[20..24].try_into().unwrap()),
        luminosity: f32::from_le_bytes(chunk[24..28].try_into().unwrap()),
    }
}

fn compute_leaf_aggregate(points: &[TempPoint]) -> AggregateData {
    let mut total_luminosity = 0_f32;
    let mut weighted_bp_rp_sum = 0_f32;
    let mut weighted_pos = Vec3 { x: 0.0, y: 0.0, z: 0.0 };
    for p in points {
        total_luminosity += p.luminosity;
        if !p.bp_rp.is_nan() {
            weighted_bp_rp_sum += p.bp_rp * p.luminosity;
        }
        weighted_pos = weighted_pos + p.position * p.luminosity;
    }
    AggregateData {
        total_luminosity,
        weighted_bp_rp_sum,
        weighted_pos,
        star_count: points.len() as u32,
    }
}

fn apply_aggregates_to_node(node: &mut PackedOctreeNode, agg: &AggregateData) {
    node.total_luminosity = agg.total_luminosity;
    node.mean_bp_rp = if agg.total_luminosity > 0.0 {
        agg.weighted_bp_rp_sum / agg.total_luminosity
    } else {
        f32::NAN
    };
    node.centroid = if agg.total_luminosity > 0.0 {
        agg.weighted_pos * (1.0 / agg.total_luminosity)
    } else {
        Vec3 { x: 0.0, y: 0.0, z: 0.0 }
    };
    node.star_count = agg.star_count;
}

// Process nodes in reverse index order so children (higher indices) are
// computed before their parents (lower indices).
fn apply_aggregates_bottom_up(
    nodes: &mut Vec<PackedOctreeNode>,
    leaf_aggregates: &HashMap<u32, AggregateData>,
) {
    for i in (0..nodes.len()).rev() {
        if nodes[i].child_mask == 0 {
            if let Some(agg) = leaf_aggregates.get(&nodes[i].first) {
                apply_aggregates_to_node(&mut nodes[i], agg);
            }
        } else {
            let mut total_luminosity = 0_f32;
            let mut weighted_bp_rp_sum = 0_f32;
            let mut weighted_pos = Vec3 { x: 0.0, y: 0.0, z: 0.0 };
            let mut star_count = 0_u32;
            let mut child_flat_idx = nodes[i].first as usize;
            for bit in 0..8_u8 {
                if nodes[i].child_mask & (1 << bit) == 0 {
                    continue;
                }
                let child = &nodes[child_flat_idx];
                total_luminosity += child.total_luminosity;
                if !child.mean_bp_rp.is_nan() {
                    weighted_bp_rp_sum += child.mean_bp_rp * child.total_luminosity;
                }
                weighted_pos = weighted_pos + child.centroid * child.total_luminosity;
                star_count += child.star_count;
                child_flat_idx += 1;
            }
            apply_aggregates_to_node(
                &mut nodes[i],
                &AggregateData { total_luminosity, weighted_bp_rp_sum, weighted_pos, star_count },
            );
        }
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

const ZERO_NODE: PackedOctreeNode = PackedOctreeNode {
    child_mask: 0,
    first: 0,
    count: 0,
    total_luminosity: 0.0,
    mean_bp_rp: f32::NAN,
    centroid: Vec3 { x: 0.0, y: 0.0, z: 0.0 },
    star_count: 0,
};

fn flatten_tree_at(node: &TreeNode, index: usize, nodes: &mut Vec<PackedOctreeNode>) {
    if is_leaf(node) {
        nodes[index] = PackedOctreeNode {
            child_mask: 0,
            first: node.point_start,
            count: node.point_count,
            ..ZERO_NODE
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
        nodes.push(ZERO_NODE);
    }

    nodes[index] = PackedOctreeNode { child_mask, first: first_child, count: 0, ..ZERO_NODE };

    for child in 0..8 {
        let Some(next) = node.children[child].as_ref() else {
            continue;
        };
        flatten_tree_at(next, child_indices[child], nodes);
    }
}

fn flatten_tree(node: &TreeNode) -> Vec<PackedOctreeNode> {
    let mut nodes = vec![ZERO_NODE];
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
        let luminosity = compute_luminosity(row.parallax, row.phot_g_mean_mag).unwrap_or(0.0);
        points_by_leaf.entry(morton).or_default().push(TempPoint {
            source_id: row.source_id,
            position,
            bp_rp: row.bp_rp,
            luminosity,
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
    let mut nodes = build_octree_nodes(leaf_infos, octree.depth);
    let point_count = leaf_infos.iter().map(|leaf| leaf.point_count as u64).sum::<u64>();
    let index_header = PackedOctreeIndex {
        depth: octree.depth,
        half_extent_pc,
        point_count,
        nodes: nodes.clone(),
    };

    let output_path = output_root.join(OCTREE_INDEX_FILENAME);
    let mut file = fs::File::create(&output_path)
        .with_context(|| format!("failed to create {}", output_path.display()))?;

    // Write header + placeholder node table (aggregate fields are zeroed).
    file.write_all(&encode_packed_octree(&index_header))
        .with_context(|| format!("failed to write {}", output_path.display()))?;

    // Write point data leaf by leaf; collect aggregate data keyed by point_start.
    let mut leaf_aggregates: HashMap<u32, AggregateData> = HashMap::new();
    for leaf in leaf_infos {
        let temp_path = temp_leaf_path(output_root, leaf.morton);
        let mut points = read_temp_points(&temp_path)?;
        // Delete the temp file immediately to free tmpfs space before writing output.
        fs::remove_file(&temp_path)
            .with_context(|| format!("failed to remove {}", temp_path.display()))?;
        points.sort_by_key(|point| point.source_id);
        let leaf_bounds = octree.bounds.leaf_bounds(octree.depth, leaf.morton);
        leaf_aggregates.insert(leaf.point_start, compute_leaf_aggregate(&points));
        let packed = points
            .iter()
            .map(|p| quantize_point(leaf_bounds, p.position, p.source_id, p.bp_rp, p.luminosity))
            .collect::<Vec<PackedPoint>>();
        file.write_all(&encode_packed_points(&packed))
            .with_context(|| format!("failed to write {}", output_path.display()))?;
    }

    // Propagate aggregates bottom-up through the node tree.
    apply_aggregates_bottom_up(&mut nodes, &leaf_aggregates);

    // Seek back to the node table and rewrite it with aggregate data.
    file.seek(SeekFrom::Start(PACKED_OCTREE_HEADER_SIZE as u64))
        .with_context(|| format!("failed to seek {}", output_path.display()))?;
    file.write_all(&encode_packed_octree_node_table(&nodes))
        .with_context(|| format!("failed to rewrite node table in {}", output_path.display()))?;
    file.flush()
        .with_context(|| format!("failed to flush {}", output_path.display()))?;

    fs::remove_dir_all(temp_leaf_root(output_root))
        .with_context(|| format!("failed to remove {}", temp_leaf_root(output_root).display()))?;

    Ok(PackedOctreeIndex { depth: octree.depth, half_extent_pc, point_count, nodes })
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
