use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use tempfile::tempdir;

use crate::formats::{
    CANONICAL_ROOT, METADATA_FILENAME, OCTREE_INDEX_FILENAME, OctreeIndex, ServingRow,
    SourceMetadata, decode_canonical_rows, decode_source_metadata, leaf_filename,
    serving_directory, write_octree_index, write_serving_rows,
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

fn load_source_metadata(
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

fn build_serving_rows(
    storage: &StorageClient,
    data_root: &StorageRoot,
    octree: OctreeConfig,
    metadata: &[SourceMetadata],
) -> Result<(BTreeMap<u32, Vec<ServingRow>>, u64)> {
    let mut serving_rows = BTreeMap::<u32, Vec<ServingRow>>::new();
    let mut rows_in_bounds = 0;

    for source in metadata {
        let canonical_root = data_root.join(&source.canonical_directory);
        for part in &source.canonical_parts {
            let rows = decode_canonical_rows(&storage.read_bytes(&canonical_root.join(part))?)?;
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
        }
    }

    Ok((serving_rows, rows_in_bounds))
}

fn write_index_output(
    output_root: &Path,
    octree_depth: u8,
    serving_rows: &mut BTreeMap<u32, Vec<ServingRow>>,
    bounds: Bounds3,
) -> Result<OctreeIndex> {
    let serving_root = output_root.join(serving_directory(octree_depth));
    if serving_root.exists() {
        fs::remove_dir_all(&serving_root)
            .with_context(|| format!("failed to clear {}", serving_root.display()))?;
    }
    fs::create_dir_all(&serving_root)
        .with_context(|| format!("failed to create {}", serving_root.display()))?;

    let mut leaves = Vec::with_capacity(serving_rows.len());
    for (morton, rows) in serving_rows {
        rows.sort_by_key(|row| row.source_id);
        write_serving_rows(&serving_root.join(leaf_filename(*morton)), rows)?;
        leaves.push(*morton);
    }
    let index = OctreeIndex {
        depth: octree_depth,
        bounds,
        leaves,
    };
    write_octree_index(&output_root.join(OCTREE_INDEX_FILENAME), &index)?;
    Ok(index)
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

    let octree = OctreeConfig {
        depth: config.octree_depth,
        bounds: config.bounds,
    };
    let (mut serving_rows, rows_in_bounds) =
        build_serving_rows(&storage, &data_root, octree, &metadata)?;

    let index = match data_root {
        StorageRoot::Local(ref path) => {
            fs::create_dir_all(&path)
                .with_context(|| format!("failed to create {}", path.display()))?;
            write_index_output(&path, config.octree_depth, &mut serving_rows, config.bounds)?
        }
        ref root @ StorageRoot::Gcs(_) => {
            let local_output = tempdir().context("failed to create temporary output directory")?;
            let index = write_index_output(
                local_output.path(),
                config.octree_depth,
                &mut serving_rows,
                config.bounds,
            )?;
            storage.upload_directory(local_output.path(), &root)?;
            index
        }
    };

    storage.validate_serving_layout(&data_root, &index)?;

    Ok(BuildIndexResult {
        index,
        sources_seen: metadata.len(),
        rows_in_bounds,
    })
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
}
