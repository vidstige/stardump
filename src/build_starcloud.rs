// Builds the starcloud.bin artifact from canonical rows.
//
// Every node (leaf and internal) owns a range in the shared point table.
// Leaves emit all their stars at true luminosity. Internal nodes emit a
// uniform random subsample of K descendants with luminosities boosted by
// |D|/K so the subsample's total flux equals the descendants' total flux in
// expectation.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::build_index::{load_source_metadata, read_canonical_part_rows};
use crate::formats::{CANONICAL_ROOT, compute_luminosity};
use crate::octree::{Bounds3, OctreeConfig};
use crate::quality::{
    DEFAULT_PARALLAX_QUALITY_THRESHOLD, maximum_distance_pc_for_quality, passes_parallax_quality,
};
use crate::starcloud::{
    STARCLOUD_FILENAME, StarcloudIndex, StarcloudNode, StarcloudPoint, encode_starcloud,
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
    let bound = maximum_distance_pc_for_quality(minimum_quality);
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

fn load_points(
    data_root: &Path,
    octree: OctreeConfig,
    quality_threshold: f32,
) -> Result<(Vec<Point>, usize, u64)> {
    let metadata = load_source_metadata(data_root)?;
    let mut points = Vec::new();
    let mut rows_in_bounds = 0_u64;
    let sources_seen = metadata.len();
    for source in &metadata {
        for part in &source.canonical_parts {
            for row in read_canonical_part_rows(data_root, source, part)? {
                if !passes_parallax_quality(
                    row.parallax,
                    row.parallax_error,
                    row.phot_g_mean_mag,
                    quality_threshold,
                ) {
                    continue;
                }
                let position = cartesian_coordinates(row.ra, row.dec, row.parallax);
                let Some(morton) = octree.morton_for_point(position) else {
                    continue;
                };
                let luminosity =
                    compute_luminosity(row.parallax, row.phot_g_mean_mag).unwrap_or(0.0);
                if !(luminosity > 0.0) {
                    continue;
                }
                rows_in_bounds += 1;
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
    Ok((points, sources_seen, rows_in_bounds))
}

fn emit_leaf_points(points: &[Point], out: &mut Vec<StarcloudPoint>) {
    for p in points {
        out.push(StarcloudPoint {
            position: p.position,
            luminosity: p.luminosity,
            bp_rp: p.bp_rp,
        });
    }
}

fn emit_subsample(
    points: &[Point],
    k: usize,
    morton_prefix: u64,
    depth: u8,
    out: &mut Vec<StarcloudPoint>,
) {
    if points.len() <= k {
        emit_leaf_points(points, out);
        return;
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
        out.push(StarcloudPoint {
            position: p.position,
            luminosity: p.luminosity * boost,
            bp_rp: p.bp_rp,
        });
    }
}

fn partition_by_child(points: &[Point], shift: u32) -> [usize; 9] {
    // points are sorted by morton; child bit = (morton >> shift) & 7.
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

fn build_recursive(
    node_index: usize,
    points: &[Point],
    current_depth: u8,
    max_depth: u8,
    morton_prefix: u64,
    sample_budget: usize,
    nodes: &mut Vec<StarcloudNode>,
    out_points: &mut Vec<StarcloudPoint>,
) {
    if current_depth == max_depth || points.is_empty() {
        let point_first = out_points.len() as u32;
        emit_leaf_points(points, out_points);
        nodes[node_index] = StarcloudNode {
            child_mask: 0,
            first_child: 0,
            point_first,
            point_count: points.len() as u32,
        };
        return;
    }

    let point_first = out_points.len() as u32;
    emit_subsample(points, sample_budget, morton_prefix, current_depth, out_points);
    let point_count = (out_points.len() as u32) - point_first;

    let shift = (max_depth - current_depth - 1) as u32 * 3;
    let child_bounds = partition_by_child(points, shift);

    let mut child_mask = 0_u8;
    let mut nonempty: Vec<u32> = Vec::new();
    for child in 0..8_u32 {
        if child_bounds[(child + 1) as usize] > child_bounds[child as usize] {
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
        let s = child_bounds[child as usize];
        let e = child_bounds[(child + 1) as usize];
        let child_prefix = (morton_prefix << 3) | (child as u64);
        build_recursive(
            child_node_index,
            &points[s..e],
            current_depth + 1,
            max_depth,
            child_prefix,
            sample_budget,
            nodes,
            out_points,
        );
    }
}

fn build_starcloud_index(
    points: Vec<Point>,
    depth: u8,
    half_extent_pc: f32,
    sample_budget: usize,
) -> StarcloudIndex {
    let mut nodes: Vec<StarcloudNode> = vec![StarcloudNode {
        child_mask: 0,
        first_child: 0,
        point_first: 0,
        point_count: 0,
    }];
    let mut out_points: Vec<StarcloudPoint> = Vec::new();
    build_recursive(
        0,
        &points,
        0,
        depth,
        0,
        sample_budget,
        &mut nodes,
        &mut out_points,
    );
    StarcloudIndex { depth, half_extent_pc, nodes, points: out_points }
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
    let (points, sources_seen, rows_in_bounds) =
        load_points(&data_root, octree, config.quality_threshold)?;
    let index = build_starcloud_index(
        points,
        config.octree_depth,
        half_extent_pc,
        config.sample_budget,
    );

    let output_path = output_root.join(STARCLOUD_FILENAME);
    let bytes = encode_starcloud(&index);
    fs::write(&output_path, &bytes)
        .with_context(|| format!("failed to write {}", output_path.display()))?;

    Ok(BuildStarcloudResult {
        sources_seen,
        rows_in_bounds,
        node_count: index.nodes.len(),
        point_count: index.points.len(),
        output_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::starcloud::decode_starcloud;

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

    #[test]
    fn subsample_boost_conserves_flux_in_expectation_on_average() {
        let points: Vec<Point> = (0..1_000_u64).map(|i| make_point(i, 0, 1.0)).collect();
        let mut out = Vec::new();
        emit_subsample(&points, 50, 0, 0, &mut out);
        let total: f32 = out.iter().map(|p| p.luminosity).sum();
        let true_total: f32 = points.iter().map(|p| p.luminosity).sum();
        assert_eq!(total, true_total);
    }

    #[test]
    fn subsample_returns_all_when_fewer_than_budget() {
        let points: Vec<Point> = (0..10_u64).map(|i| make_point(i, 0, 2.0)).collect();
        let mut out = Vec::new();
        emit_subsample(&points, 256, 0, 0, &mut out);
        assert_eq!(out.len(), 10);
        for p in &out {
            assert_eq!(p.luminosity, 2.0);
        }
    }

    #[test]
    fn build_emits_leaves_and_internal_samples() {
        // 4 points in distinct leaves of a depth-1 tree.
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
        // Root + one child per populated leaf (4).
        assert_eq!(index.nodes.len(), 5);
        assert_eq!(index.nodes[0].child_mask.count_ones(), 4);
        assert_eq!(index.nodes[0].point_count, 2); // root subsample K=2
        // Each leaf has one point.
        for child in 1..5 {
            assert_eq!(index.nodes[child].child_mask, 0);
            assert_eq!(index.nodes[child].point_count, 1);
        }
        // Total points written = 2 (root sample) + 4 (leaves) = 6.
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
