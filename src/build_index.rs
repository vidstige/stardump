use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::formats::{
    CANONICAL_ROOT, CanonicalRow, METADATA_FILENAME, OCTREE_INDEX_FILENAME, OctreeIndex,
    ServingRow, SourceMetadata, append_serving_rows, decode_canonical_rows, decode_source_metadata,
    indices_directory, leaf_filename, write_octree_index,
};
use crate::octree::{Bounds3, OctreeConfig};
use crate::quality::{DEFAULT_PARALLAX_QUALITY_THRESHOLD, maximum_distance_pc_for_quality};
use crate::storage::{
    list_relative_files_recursive, local_path, validate_canonical_layout, validate_serving_layout,
};

pub const DEFAULT_DEPTH: u8 = 6;
// The default quality threshold is 10, and the bright-source Gaia DR3 floor is
// taken as 0.025 mas. That implies a minimum accepted parallax of 0.25 mas and
// therefore a maximum indexed distance of about 4000 pc.
pub const DEFAULT_BOUND_PC: f32 = 1_000.0 / (DEFAULT_PARALLAX_QUALITY_THRESHOLD * 0.025);
pub const DEFAULT_BOUNDS: Bounds3 = Bounds3 {
    min: [-DEFAULT_BOUND_PC, -DEFAULT_BOUND_PC, -DEFAULT_BOUND_PC],
    max: [DEFAULT_BOUND_PC, DEFAULT_BOUND_PC, DEFAULT_BOUND_PC],
};

pub fn bounds_for_quality_threshold(minimum_quality: f32) -> Bounds3 {
    let bound = maximum_distance_pc_for_quality(minimum_quality);
    Bounds3 {
        min: [-bound, -bound, -bound],
        max: [bound, bound, bound],
    }
}

#[derive(Clone, Debug)]
pub struct BuildIndexConfig {
    pub data_root: String,
    pub output_root: String,
    pub octree_depth: u8,
    pub bounds: Bounds3,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BuildIndexResult {
    pub index: OctreeIndex,
    pub sources_seen: usize,
    pub rows_in_bounds: u64,
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

pub fn load_source_metadata(data_root: &Path) -> Result<Vec<SourceMetadata>> {
    let canonical_root = data_root.join(CANONICAL_ROOT);
    let mut metadata = Vec::new();
    for relative in list_relative_files_recursive(&canonical_root)? {
        if !relative.ends_with(&format!("/{METADATA_FILENAME}")) {
            continue;
        }

        let source_root = canonical_root.join(&relative);
        let source_metadata = decode_source_metadata(
            &fs::read(&source_root)
                .with_context(|| format!("failed to read {}", source_root.display()))?,
        )
        .with_context(|| format!("failed to parse {}", source_root.display()))?;
        validate_canonical_layout(data_root, &source_metadata)?;
        metadata.push(source_metadata);
    }

    metadata.sort_by(|left, right| left.input_name.cmp(&right.input_name));
    Ok(metadata)
}

fn prepare_output_root(output_root: &Path, octree_depth: u8) -> Result<()> {
    fs::create_dir_all(output_root)
        .with_context(|| format!("failed to create {}", output_root.display()))?;
    let indices_root = output_root.join(indices_directory(octree_depth));
    if indices_root.exists() {
        fs::remove_dir_all(&indices_root)
            .with_context(|| format!("failed to clear {}", indices_root.display()))?;
    }
    fs::create_dir_all(&indices_root)
        .with_context(|| format!("failed to create {}", indices_root.display()))?;
    Ok(())
}

fn write_index_output(
    output_root: &Path,
    octree_depth: u8,
    bounds: Bounds3,
    leaves: Vec<u32>,
) -> Result<OctreeIndex> {
    let index = OctreeIndex {
        depth: octree_depth,
        bounds,
        leaves,
    };
    write_octree_index(&output_root.join(OCTREE_INDEX_FILENAME), &index)?;
    Ok(index)
}

fn serving_rows_for_canonical_part(
    octree: OctreeConfig,
    rows: Vec<CanonicalRow>,
) -> (BTreeMap<u32, Vec<ServingRow>>, u64) {
    let mut serving_rows = BTreeMap::<u32, Vec<ServingRow>>::new();
    let mut rows_in_bounds = 0;

    for row in rows {
        let [x, y, z] = cartesian_coordinates(row.ra, row.dec, row.parallax);
        let Some(morton) = octree.morton_for_point([x, y, z]) else {
            continue;
        };
        rows_in_bounds += 1;
        serving_rows.entry(morton).or_default().push(ServingRow {
            source_id: row.source_id,
            x,
            y,
            z,
        });
    }

    (serving_rows, rows_in_bounds)
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
    leaves: BTreeSet<u32>,
    rows_in_bounds: u64,
}

impl IndexBuilder {
    pub fn new(output_root: &Path, octree_depth: u8, bounds: Bounds3) -> Result<Self> {
        prepare_output_root(output_root, octree_depth)?;
        Ok(Self {
            output_root: output_root.to_path_buf(),
            octree: OctreeConfig {
                depth: octree_depth,
                bounds,
            },
            leaves: BTreeSet::new(),
            rows_in_bounds: 0,
        })
    }

    pub fn append_rows(&mut self, rows: Vec<CanonicalRow>) -> Result<()> {
        let indices_root = self.output_root.join(indices_directory(self.octree.depth));
        let (mut part_serving_rows, part_rows_in_bounds) =
            serving_rows_for_canonical_part(self.octree, rows);
        self.rows_in_bounds += part_rows_in_bounds;

        for (morton, rows) in &mut part_serving_rows {
            rows.sort_by_key(|row| row.source_id);
            append_serving_rows(&indices_root.join(leaf_filename(*morton)), rows)?;
            self.leaves.insert(*morton);
        }

        Ok(())
    }

    pub fn finish(self) -> Result<(OctreeIndex, u64)> {
        let index = write_index_output(
            &self.output_root,
            self.octree.depth,
            self.octree.bounds,
            self.leaves.into_iter().collect(),
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
    validate_serving_layout(&output_root, &index)?;
    Ok(BuildIndexResult {
        index,
        sources_seen: metadata.len(),
        rows_in_bounds,
    })
}
