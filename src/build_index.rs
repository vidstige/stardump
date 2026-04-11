use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use tempfile::tempdir;

use crate::formats::{
    CANONICAL_ROOT, CanonicalRow, METADATA_FILENAME, OCTREE_INDEX_FILENAME, OctreeIndex,
    ServingRow, SourceMetadata, append_serving_rows, decode_canonical_rows, decode_source_metadata,
    leaf_filename, serving_directory, write_octree_index,
};
use crate::octree::{Bounds3, OctreeConfig};
use crate::storage::{StorageClient, StorageRoot};

pub const DEFAULT_DEPTH: u8 = 6;
pub const DEFAULT_BOUNDS: Bounds3 = Bounds3 {
    min: [-100_000.0, -100_000.0, -100_000.0],
    max: [100_000.0, 100_000.0, 100_000.0],
};

#[derive(Clone, Debug)]
pub struct BuildIndexConfig {
    pub data_root: String,
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

pub fn load_source_metadata(
    storage: &StorageClient,
    data_root: &StorageRoot,
) -> Result<Vec<SourceMetadata>> {
    let canonical_root = data_root.join(CANONICAL_ROOT);
    let mut metadata = Vec::new();
    for relative in storage.list_relative_files_recursive(&canonical_root)? {
        if !relative.ends_with(&format!("/{METADATA_FILENAME}")) {
            continue;
        }

        let source_root = canonical_root.join(&relative);
        let source_metadata = decode_source_metadata(&storage.read_bytes(&source_root)?)
            .with_context(|| format!("failed to parse {}", source_root.display()))?;
        storage.validate_canonical_layout(data_root, &source_metadata)?;
        metadata.push(source_metadata);
    }

    metadata.sort_by(|left, right| left.input_name.cmp(&right.input_name));
    Ok(metadata)
}

fn prepare_output_root(output_root: &Path, octree_depth: u8) -> Result<()> {
    fs::create_dir_all(output_root)
        .with_context(|| format!("failed to create {}", output_root.display()))?;
    let serving_root = output_root.join(serving_directory(octree_depth));
    if serving_root.exists() {
        fs::remove_dir_all(&serving_root)
            .with_context(|| format!("failed to clear {}", serving_root.display()))?;
    }
    fs::create_dir_all(&serving_root)
        .with_context(|| format!("failed to create {}", serving_root.display()))?;
    Ok(())
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
    storage: &StorageClient,
    data_root: &StorageRoot,
    source: &SourceMetadata,
    part: &str,
) -> Result<Vec<CanonicalRow>> {
    let canonical_root = data_root.join(&source.canonical_directory);
    decode_canonical_rows(&storage.read_bytes(&canonical_root.join(part))?)
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
        let serving_root = self.output_root.join(serving_directory(self.octree.depth));
        let (mut part_serving_rows, part_rows_in_bounds) =
            serving_rows_for_canonical_part(self.octree, rows);
        self.rows_in_bounds += part_rows_in_bounds;

        for (morton, rows) in &mut part_serving_rows {
            rows.sort_by_key(|row| row.source_id);
            append_serving_rows(&serving_root.join(leaf_filename(*morton)), rows)?;
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
    let data_root = StorageRoot::parse(&config.data_root)?;
    let storage = StorageClient::new()?;
    let metadata = load_source_metadata(&storage, &data_root)?;
    if metadata.is_empty() {
        bail!("no canonical source metadata found under {CANONICAL_ROOT}");
    }

    match data_root {
        StorageRoot::Local(ref path) => {
            let mut builder = IndexBuilder::new(path, config.octree_depth, config.bounds)?;
            for source in &metadata {
                for part in &source.canonical_parts {
                    builder.append_rows(read_canonical_part_rows(&storage, &data_root, source, part)?)?;
                }
            }
            let (index, rows_in_bounds) = builder.finish()?;
            storage.validate_serving_layout(&data_root, &index)?;
            return Ok(BuildIndexResult {
                index,
                sources_seen: metadata.len(),
                rows_in_bounds,
            });
        }
        ref root @ StorageRoot::Gcs(_) => {
            let local_output = tempdir().context("failed to create temporary output directory")?;
            let mut builder =
                IndexBuilder::new(local_output.path(), config.octree_depth, config.bounds)?;
            for source in &metadata {
                for part in &source.canonical_parts {
                    builder.append_rows(read_canonical_part_rows(&storage, &data_root, source, part)?)?;
                }
            }
            let (index, rows_in_bounds) = builder.finish()?;
            storage.upload_directory(local_output.path(), &root)?;
            storage.validate_serving_layout(&data_root, &index)?;
            return Ok(BuildIndexResult {
                index,
                sources_seen: metadata.len(),
                rows_in_bounds,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::Path;

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tempfile::tempdir;

    use crate::formats::{OCTREE_INDEX_FILENAME, decode_serving_rows, read_octree_index};
    use crate::ingest::{IngestConfig, run_ingestion};

    use super::*;

    fn write_gzip_file(path: &Path, body: &str) {
        let file = fs::File::create(path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(body.as_bytes()).unwrap();
        encoder.finish().unwrap();
    }

    #[test]
    fn builds_index_from_multiple_canonical_sources() {
        let dir = tempdir().unwrap();
        let input_a = dir.path().join("GaiaSource_000000-000001.csv.gz");
        let input_b = dir.path().join("GaiaSource_000002-000003.csv.gz");
        let output_root = dir.path().join("run");

        write_gzip_file(
            &input_a,
            "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             1,0,0,100,1,12.5,0.3\n\
             2,90,0,100,1,13.5,0.6\n",
        );
        write_gzip_file(
            &input_b,
            "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             3,180,0,100,1,14.5,0.9\n\
             4,0,90,100,1,15.5,1.2\n",
        );

        run_ingestion(IngestConfig {
            inputs: vec![input_a.display().to_string(), input_b.display().to_string()],
            output_root: output_root.display().to_string(),
            parallax_filter_mas: None,
        })
        .unwrap();

        let result = run_build_index(BuildIndexConfig {
            data_root: output_root.display().to_string(),
            octree_depth: DEFAULT_DEPTH,
            bounds: DEFAULT_BOUNDS,
        })
        .unwrap();
        let index = read_octree_index(&output_root.join(OCTREE_INDEX_FILENAME)).unwrap();
        let total_rows: usize = index
            .leaves
            .iter()
            .map(|leaf| {
                decode_serving_rows(
                    &fs::read(
                        output_root
                            .join(serving_directory(DEFAULT_DEPTH))
                            .join(leaf_filename(*leaf)),
                    )
                    .unwrap(),
                )
                .unwrap()
                .len()
            })
            .sum();

        assert_eq!(result.index, index);
        assert_eq!(result.sources_seen, 2);
        assert_eq!(result.rows_in_bounds, 4);
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn appends_rows_from_multiple_sources_into_one_leaf() {
        let dir = tempdir().unwrap();
        let input_a = dir.path().join("GaiaSource_000000-000001.csv.gz");
        let input_b = dir.path().join("GaiaSource_000002-000003.csv.gz");
        let output_root = dir.path().join("run");

        write_gzip_file(
            &input_a,
            "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             1,0,0,100,1,12.5,0.3\n",
        );
        write_gzip_file(
            &input_b,
            "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             2,0,0,100,1,13.5,0.6\n",
        );

        run_ingestion(IngestConfig {
            inputs: vec![input_a.display().to_string(), input_b.display().to_string()],
            output_root: output_root.display().to_string(),
            parallax_filter_mas: None,
        })
        .unwrap();

        let result = run_build_index(BuildIndexConfig {
            data_root: output_root.display().to_string(),
            octree_depth: DEFAULT_DEPTH,
            bounds: DEFAULT_BOUNDS,
        })
        .unwrap();

        assert_eq!(result.index.leaves.len(), 1);
        let rows = decode_serving_rows(
            &fs::read(
                output_root
                    .join(serving_directory(DEFAULT_DEPTH))
                    .join(leaf_filename(result.index.leaves[0])),
            )
            .unwrap(),
        )
        .unwrap();
        let source_ids: Vec<u64> = rows.iter().map(|row| row.source_id).collect();

        assert_eq!(source_ids, vec![1, 2]);
    }
}
