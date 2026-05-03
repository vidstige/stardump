// Builds the starcloud.bin artifact from canonical rows.
//
// Every node (leaf and internal) owns a range in the shared point table.
// Leaves emit all their stars at true luminosity. Internal nodes emit a
// uniform random subsample of K descendants with luminosities boosted by
// |D|/K so the subsample's total flux equals the descendants' total flux in
// expectation.
//
// Memory strategy: process one level-1 octant at a time (~N/8 stars per pass),
// streaming output points directly to a temp file. Total peak RAM ≈ N/8 × 32 B
// for the octant input plus nodes (~48 MB). The root subsample is selected via
// a streaming max-heap reservoir requiring only O(K) memory.

use std::collections::{BinaryHeap, HashMap};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::formats::{
    CANONICAL_ROOT, METADATA_FILENAME, SourceMetadata, compute_luminosity, read_canonical_rows,
    read_source_metadata,
};
use crate::octree::{Bounds3, OctreeConfig};
use crate::quality::{
    DEFAULT_PARALLAX_QUALITY_THRESHOLD, PARALLAX_SYSTEMATIC_FLOOR_MAS, passes_parallax_quality,
};
use crate::starcloud::{
    STARCLOUD_FILENAME, StarcloudNode, StarcloudPoint,
    write_starcloud_header_and_nodes, write_starcloud_point,
};
use crate::storage::local_path;
use crate::vec3::Vec3;

pub const DEFAULT_DEPTH: u8 = 7;
pub const DEFAULT_SAMPLE_BUDGET: usize = 256;

#[derive(Clone, Debug)]
pub struct BuildStarcloudConfig {
    pub data_root: String,
    pub output_root: String,
    pub octree_depth: u8,
    pub quality_threshold: f32,
    pub sample_budget: usize,
}

impl Default for BuildStarcloudConfig {
    fn default() -> Self {
        Self {
            data_root: String::new(),
            output_root: String::new(),
            octree_depth: DEFAULT_DEPTH,
            quality_threshold: DEFAULT_PARALLAX_QUALITY_THRESHOLD,
            sample_budget: DEFAULT_SAMPLE_BUDGET,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BuildStarcloudResult {
    pub sources_seen: usize,
    pub rows_in_bounds: u64,
    pub quality_passed_out_of_bounds: u64,
    pub node_count: usize,
    pub point_count: usize,
    pub output_path: PathBuf,
}

#[derive(Clone, Copy, Debug)]
struct Point {
    source_id: u64,
    position: Vec3,
    luminosity: f32,
    bp_rp: f32,
    morton: u32,
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

fn bounds_for_quality_threshold(minimum_quality: f32) -> Bounds3 {
    let bound = 1_000.0 / (minimum_quality * PARALLAX_SYSTEMATIC_FLOOR_MAS);
    Bounds3 {
        min: Vec3 { x: -bound, y: -bound, z: -bound },
        max: Vec3 { x: bound, y: bound, z: bound },
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58476D1CE4E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D049BB133111EB);
    x ^= x >> 31;
    x
}

fn load_source_metadata(data_root: &Path) -> Result<Vec<SourceMetadata>> {
    use crate::storage::list_relative_files_recursive;
    let canonical_root = data_root.join(CANONICAL_ROOT);
    let mut result = Vec::new();
    for relative in list_relative_files_recursive(&canonical_root)? {
        if relative.ends_with(METADATA_FILENAME) {
            result.push(read_source_metadata(&canonical_root.join(&relative))?);
        }
    }
    result.sort_by(|a, b| a.input_name.cmp(&b.input_name));
    Ok(result)
}

fn read_canonical_part_rows(
    data_root: &Path,
    source: &SourceMetadata,
    part: &str,
) -> Result<Vec<crate::formats::CanonicalRow>> {
    read_canonical_rows(&data_root.join(&source.canonical_directory).join(part))
}

// Streaming reservoir for the root node subsample.
// Keeps the K points with the smallest splitmix64(source_id ^ seed) hash values.
struct RootReservoir {
    heap: BinaryHeap<(u64, u64)>, // max-heap: (hash, source_id)
    data: HashMap<u64, Point>,    // source_id → point
    capacity: usize,
    seed: u64,
}

impl RootReservoir {
    fn new(capacity: usize) -> Self {
        // Root: depth=0, morton_prefix=0 → seed = splitmix64(0 ^ (0 << 56)) = splitmix64(0)
        let seed = splitmix64(0);
        Self {
            heap: BinaryHeap::with_capacity(capacity + 1),
            data: HashMap::with_capacity(capacity + 1),
            capacity,
            seed,
        }
    }

    fn add(&mut self, p: Point) {
        if self.capacity == 0 {
            return;
        }
        let hash = splitmix64(p.source_id ^ self.seed);
        if self.heap.len() < self.capacity {
            self.heap.push((hash, p.source_id));
            self.data.insert(p.source_id, p);
        } else {
            let &(top_hash, top_id) = self.heap.peek().unwrap();
            if hash < top_hash || (hash == top_hash && p.source_id < top_id) {
                self.heap.pop();
                self.data.remove(&top_id);
                self.heap.push((hash, p.source_id));
                self.data.insert(p.source_id, p);
            }
        }
    }

    // Writes the selected subsample to `out` with flux boost, returns count written.
    // Returns 0 if total_count <= capacity (renderer must always descend).
    fn emit<W: Write>(self, total_count: u64, out: &mut W) -> Result<u32> {
        if total_count <= self.capacity as u64 {
            return Ok(0);
        }
        let boost = total_count as f32 / self.capacity as f32;
        let mut selected: Vec<(u64, Point)> = self.heap
            .into_iter()
            .map(|(_, id)| (id, *self.data.get(&id).unwrap()))
            .collect();
        selected.sort_by_key(|(id, _)| *id);
        for (_, p) in &selected {
            write_starcloud_point(out, &StarcloudPoint {
                position: p.position,
                luminosity: p.luminosity * boost,
                bp_rp: p.bp_rp,
            })?;
        }
        Ok(selected.len() as u32)
    }
}

// Single pass: build root reservoir, discover which octants contain stars, gather stats.
fn scan_for_root(
    metadata: &[SourceMetadata],
    data_root: &Path,
    octree: OctreeConfig,
    quality_threshold: f32,
    sample_budget: usize,
    octant_shift: u32,
) -> Result<(RootReservoir, [bool; 8], u64, u64)> {
    let mut reservoir = RootReservoir::new(sample_budget);
    let mut octant_has_stars = [false; 8];
    let mut rows_in_bounds = 0u64;
    let mut quality_passed_out_of_bounds = 0u64;

    for source in metadata {
        for part in &source.canonical_parts {
            for row in read_canonical_part_rows(data_root, source, part)? {
                if !passes_parallax_quality(row.parallax, row.parallax_error, quality_threshold) {
                    continue;
                }
                let position = cartesian_coordinates(row.ra, row.dec, row.parallax);
                let Some(morton) = octree.morton_for_point(position) else {
                    quality_passed_out_of_bounds += 1;
                    continue;
                };
                let luminosity =
                    compute_luminosity(row.parallax, row.phot_g_mean_mag).unwrap_or(0.0);
                if !(luminosity > 0.0) {
                    continue;
                }
                rows_in_bounds += 1;
                let p = Point {
                    source_id: row.source_id,
                    position,
                    luminosity,
                    bp_rp: row.bp_rp,
                    morton,
                };
                octant_has_stars[((morton >> octant_shift) & 7) as usize] = true;
                reservoir.add(p);
            }
        }
    }

    Ok((reservoir, octant_has_stars, rows_in_bounds, quality_passed_out_of_bounds))
}

// One pass over canonical data collecting only stars whose top Morton bits equal target_octant.
fn collect_octant(
    metadata: &[SourceMetadata],
    data_root: &Path,
    octree: OctreeConfig,
    quality_threshold: f32,
    target_octant: u32,
    octant_shift: u32,
) -> Result<Vec<Point>> {
    let mut points = Vec::new();
    for source in metadata {
        for part in &source.canonical_parts {
            for row in read_canonical_part_rows(data_root, source, part)? {
                if !passes_parallax_quality(row.parallax, row.parallax_error, quality_threshold) {
                    continue;
                }
                let position = cartesian_coordinates(row.ra, row.dec, row.parallax);
                let Some(morton) = octree.morton_for_point(position) else { continue };
                if (morton >> octant_shift) & 7 != target_octant {
                    continue;
                }
                let luminosity =
                    compute_luminosity(row.parallax, row.phot_g_mean_mag).unwrap_or(0.0);
                if !(luminosity > 0.0) {
                    continue;
                }
                points.push(Point {
                    source_id: row.source_id,
                    position,
                    luminosity,
                    bp_rp: row.bp_rp,
                    morton,
                });
            }
        }
    }
    points.sort_by_key(|p| (p.morton, p.source_id));
    Ok(points)
}

fn stream_leaf_points<W: Write>(
    points: &[Point],
    out: &mut W,
    cursor: &mut u32,
) -> Result<()> {
    for p in points {
        write_starcloud_point(out, &StarcloudPoint {
            position: p.position,
            luminosity: p.luminosity,
            bp_rp: p.bp_rp,
        })?;
        *cursor += 1;
    }
    Ok(())
}

fn stream_subsample<W: Write>(
    points: &[Point],
    k: usize,
    morton_prefix: u64,
    depth: u8,
    out: &mut W,
    cursor: &mut u32,
) -> Result<bool> {
    if points.len() <= k {
        return Ok(false);
    }
    let seed = splitmix64(morton_prefix ^ ((depth as u64) << 56));
    let mut ranked: Vec<(u64, usize)> = points
        .iter()
        .enumerate()
        .map(|(i, p)| (splitmix64(p.source_id ^ seed), i))
        .collect();
    ranked.select_nth_unstable_by_key(k - 1, |(h, _)| *h);
    let boost = points.len() as f32 / k as f32;
    let mut selected: Vec<&(u64, usize)> = ranked[..k].iter().collect();
    selected.sort_by_key(|(_, i)| points[*i].source_id);
    for (_, i) in selected {
        let p = &points[*i];
        write_starcloud_point(out, &StarcloudPoint {
            position: p.position,
            luminosity: p.luminosity * boost,
            bp_rp: p.bp_rp,
        })?;
        *cursor += 1;
    }
    Ok(true)
}

fn partition_by_child(points: &[Point], shift: u32) -> [usize; 9] {
    let mut bounds = [points.len(); 9];
    bounds[0] = 0;
    let mut cursor = 0;
    for child in 0..8_u32 {
        while cursor < points.len() && ((points[cursor].morton >> shift) & 7) == child {
            cursor += 1;
        }
        bounds[(child + 1) as usize] = cursor;
    }
    bounds
}

fn build_recursive_stream<W: Write>(
    node_index: usize,
    points: &[Point],
    current_depth: u8,
    max_depth: u8,
    morton_prefix: u64,
    sample_budget: usize,
    nodes: &mut Vec<StarcloudNode>,
    out: &mut W,
    cursor: &mut u32,
) -> Result<()> {
    if current_depth == max_depth || points.is_empty() {
        let point_first = *cursor;
        stream_leaf_points(points, out, cursor)?;
        nodes[node_index] = StarcloudNode {
            child_mask: 0,
            first_child: 0,
            point_first,
            point_count: *cursor - point_first,
        };
        return Ok(());
    }

    let point_first = *cursor;
    let sampled =
        stream_subsample(points, sample_budget, morton_prefix, current_depth, out, cursor)?;
    let point_count = if sampled { *cursor - point_first } else { 0 };

    let shift = (max_depth - current_depth - 1) as u32 * 3;
    let child_ranges = partition_by_child(points, shift);

    let mut child_mask = 0_u8;
    let mut nonempty: Vec<u32> = Vec::new();
    for child in 0..8_u32 {
        if child_ranges[(child + 1) as usize] > child_ranges[child as usize] {
            child_mask |= 1 << child;
            nonempty.push(child);
        }
    }
    let first_child = nodes.len() as u32;
    for _ in 0..nonempty.len() {
        nodes.push(StarcloudNode {
            child_mask: 0,
            first_child: 0,
            point_first: 0,
            point_count: 0,
        });
    }
    nodes[node_index] = StarcloudNode { child_mask, first_child, point_first, point_count };

    for (idx, &child) in nonempty.iter().enumerate() {
        let child_node_index = first_child as usize + idx;
        let s = child_ranges[child as usize];
        let e = child_ranges[(child + 1) as usize];
        let child_prefix = (morton_prefix << 3) | (child as u64);
        build_recursive_stream(
            child_node_index,
            &points[s..e],
            current_depth + 1,
            max_depth,
            child_prefix,
            sample_budget,
            nodes,
            out,
            cursor,
        )?;
    }
    Ok(())
}

pub fn run_build_starcloud(config: BuildStarcloudConfig) -> Result<BuildStarcloudResult> {
    if config.octree_depth == 0 || config.octree_depth > 10 {
        bail!("octree depth must be between 1 and 10");
    }
    if config.sample_budget == 0 {
        bail!("sample budget must be positive");
    }

    let data_root = local_path(&config.data_root)?;
    let output_root = local_path(&config.output_root)?;
    if !data_root.join(CANONICAL_ROOT).exists() {
        bail!(
            "no canonical data under {}",
            data_root.join(CANONICAL_ROOT).display()
        );
    }

    fs::create_dir_all(&output_root)
        .with_context(|| format!("failed to create {}", output_root.display()))?;

    let bounds = bounds_for_quality_threshold(config.quality_threshold);
    let half_extent_pc = bounds.max.x;
    let octree = OctreeConfig { depth: config.octree_depth, bounds };
    let octant_shift = (config.octree_depth - 1) as u32 * 3;

    let metadata = load_source_metadata(&data_root)?;
    let sources_seen = metadata.len();

    // Phase 0: single pass — root reservoir + octant map + stats.
    let (reservoir, octant_has_stars, rows_in_bounds, quality_passed_out_of_bounds) =
        scan_for_root(
            &metadata,
            &data_root,
            octree,
            config.quality_threshold,
            config.sample_budget,
            octant_shift,
        )?;
    // Phase 1: stream output points to a temp file, build node tree.
    let temp_path = output_root.join("starcloud_points.tmp");
    let (nodes, point_count) = {
        let temp_file = fs::File::create(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?;
        let mut out = BufWriter::new(temp_file);
        let mut cursor = 0u32;

        // Root subsample points come first in the stream (point_first = 0).
        let root_point_count = reservoir.emit(rows_in_bounds, &mut out)?;
        cursor += root_point_count;

        // Allocate root node + one slot per non-empty octant (contiguous children).
        let mut nodes: Vec<StarcloudNode> =
            vec![StarcloudNode { child_mask: 0, first_child: 0, point_first: 0, point_count: 0 }];
        let nonempty_octants: Vec<u32> =
            (0u32..8).filter(|&c| octant_has_stars[c as usize]).collect();
        let root_first_child = nodes.len() as u32;
        for _ in &nonempty_octants {
            nodes.push(StarcloudNode::default());
        }

        // One canonical scan per non-empty octant.
        let mut child_mask = 0u8;
        for (idx, &octant) in nonempty_octants.iter().enumerate() {
            child_mask |= 1 << octant;
            let child_node_idx = root_first_child as usize + idx;
            let oct_points = collect_octant(
                &metadata,
                &data_root,
                octree,
                config.quality_threshold,
                octant,
                octant_shift,
            )?;
            build_recursive_stream(
                child_node_idx,
                &oct_points,
                1,
                config.octree_depth,
                octant as u64,
                config.sample_budget,
                &mut nodes,
                &mut out,
                &mut cursor,
            )?;
            // oct_points dropped here, freeing ~N/8 × 32 B.
        }

        nodes[0] = StarcloudNode {
            child_mask,
            first_child: if child_mask != 0 { root_first_child } else { 0 },
            point_first: 0,
            point_count: root_point_count,
        };

        out.flush()?;
        (nodes, cursor)
    }; // temp file closed and flushed when `out` drops here

    // Assemble final file: header + nodes + point data from temp file.
    let output_path = output_root.join(STARCLOUD_FILENAME);
    {
        let out_file = fs::File::create(&output_path)
            .with_context(|| format!("failed to create {}", output_path.display()))?;
        let mut writer = BufWriter::new(out_file);
        write_starcloud_header_and_nodes(
            &mut writer,
            config.octree_depth,
            half_extent_pc,
            &nodes,
            point_count as u64,
        )?;
        let mut temp_reader = fs::File::open(&temp_path)
            .with_context(|| format!("failed to open {}", temp_path.display()))?;
        std::io::copy(&mut temp_reader, &mut writer)
            .context("failed to copy point data to output")?;
        writer.flush()?;
    }
    fs::remove_file(&temp_path).ok();

    Ok(BuildStarcloudResult {
        sources_seen,
        rows_in_bounds,
        quality_passed_out_of_bounds,
        node_count: nodes.len(),
        point_count: point_count as usize,
        output_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::starcloud::{StarcloudIndex, decode_starcloud, decode_starcloud_point_bytes, encode_starcloud};

    fn bounds_cube(e: f32) -> Bounds3 {
        Bounds3 {
            min: Vec3 { x: -e, y: -e, z: -e },
            max: Vec3 { x: e, y: e, z: e },
        }
    }

    fn make_point(source_id: u64, morton: u32, lum: f32) -> Point {
        Point {
            source_id,
            position: Vec3 { x: 0.0, y: 0.0, z: 0.0 },
            luminosity: lum,
            bp_rp: 0.5,
            morton,
        }
    }

    // In-memory build helper for unit tests (uses streaming API internally).
    fn build_starcloud_index(
        points: Vec<Point>,
        depth: u8,
        half_extent_pc: f32,
        sample_budget: usize,
    ) -> StarcloudIndex {
        let mut nodes =
            vec![StarcloudNode { child_mask: 0, first_child: 0, point_first: 0, point_count: 0 }];
        let mut point_bytes: Vec<u8> = Vec::new();
        let mut cursor = 0u32;
        build_recursive_stream(
            0,
            &points,
            0,
            depth,
            0,
            sample_budget,
            &mut nodes,
            &mut point_bytes,
            &mut cursor,
        )
        .unwrap();
        let out_points = decode_starcloud_point_bytes(&point_bytes);
        StarcloudIndex { depth, half_extent_pc, nodes, points: out_points }
    }

    #[test]
    fn subsample_boost_conserves_flux_in_expectation_on_average() {
        let points: Vec<Point> = (0..1_000_u64).map(|i| make_point(i, 0, 1.0)).collect();
        let mut out: Vec<u8> = Vec::new();
        let mut cursor = 0u32;
        stream_subsample(&points, 50, 0, 0, &mut out, &mut cursor).unwrap();
        let selected = decode_starcloud_point_bytes(&out);
        let total: f32 = selected.iter().map(|p| p.luminosity).sum();
        let true_total: f32 = points.iter().map(|p| p.luminosity).sum();
        assert_eq!(total, true_total);
    }

    #[test]
    fn subsample_emits_nothing_when_fewer_than_budget() {
        let points: Vec<Point> = (0..10_u64).map(|i| make_point(i, 0, 2.0)).collect();
        let mut out: Vec<u8> = Vec::new();
        let mut cursor = 0u32;
        let sampled = stream_subsample(&points, 256, 0, 0, &mut out, &mut cursor).unwrap();
        assert!(!sampled);
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn build_emits_leaves_and_internal_samples() {
        let bounds = bounds_cube(10.0);
        let octree = OctreeConfig { depth: 1, bounds };
        let mut points: Vec<Point> = Vec::new();
        let positions = [
            Vec3 { x: -5.0, y: -5.0, z: -5.0 },
            Vec3 { x: 5.0, y: -5.0, z: -5.0 },
            Vec3 { x: -5.0, y: 5.0, z: -5.0 },
            Vec3 { x: 5.0, y: 5.0, z: 5.0 },
        ];
        for (i, p) in positions.iter().enumerate() {
            points.push(Point {
                source_id: i as u64,
                position: *p,
                luminosity: 1.0,
                bp_rp: 0.5,
                morton: octree.morton_for_point(*p).unwrap(),
            });
        }
        points.sort_by_key(|p| (p.morton, p.source_id));

        let index = build_starcloud_index(points, 1, 10.0, 2);
        assert_eq!(index.nodes.len(), 5);
        assert_eq!(index.nodes[0].child_mask.count_ones(), 4);
        assert_eq!(index.nodes[0].point_count, 2);
        for child in 1..5 {
            assert_eq!(index.nodes[child].child_mask, 0);
            assert_eq!(index.nodes[child].point_count, 1);
        }
        assert_eq!(index.points.len(), 6);

        let bytes = encode_starcloud(&index);
        let decoded = decode_starcloud(&bytes).unwrap();
        assert_eq!(decoded.nodes, index.nodes);
        assert_eq!(decoded.points.len(), index.points.len());
    }

    #[test]
    fn build_is_deterministic() {
        let mut points: Vec<Point> = (0..100_u64)
            .map(|i| {
                let x = ((i as f32) - 50.0) * 0.1;
                Point {
                    source_id: i,
                    position: Vec3 { x, y: 0.0, z: 0.0 },
                    luminosity: 1.0 + (i as f32) * 0.01,
                    bp_rp: 0.5,
                    morton: i as u32,
                }
            })
            .collect();
        points.sort_by_key(|p| (p.morton, p.source_id));

        let a = build_starcloud_index(points.clone(), 3, 100.0, 8);
        let b = build_starcloud_index(points, 3, 100.0, 8);
        assert_eq!(encode_starcloud(&a), encode_starcloud(&b));
    }
}
