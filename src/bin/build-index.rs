use clap::Parser;
use tempfile::tempdir;

use star_dump::build_index::{
    BuildIndexConfig, BuildIndexResult, DEFAULT_BOUNDS, DEFAULT_DEPTH, IndexBuilder,
    load_source_metadata, read_canonical_part_rows, run_build_index,
};
use star_dump::quality::passes_parallax_quality;
use star_dump::storage::{StorageClient, StorageRoot};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    data_root: String,

    #[arg(long, default_value_t = DEFAULT_DEPTH)]
    octree_depth: u8,

    #[arg(long, default_value_t = 10.0)]
    quality_threshold: f32,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    run_quality_filtered_build_index(args)?;
    Ok(())
}

fn run_quality_filtered_build_index(args: Args) -> anyhow::Result<BuildIndexResult> {
    let data_root = StorageRoot::parse(&args.data_root)?;
    let storage = StorageClient::new()?;
    let metadata = load_source_metadata(&storage, &data_root)?;

    if metadata.is_empty() {
        return run_build_index(BuildIndexConfig {
            data_root: args.data_root,
            octree_depth: args.octree_depth,
            bounds: DEFAULT_BOUNDS,
        });
    }

    match data_root {
        StorageRoot::Local(ref path) => {
            let mut builder = IndexBuilder::new(path, args.octree_depth, DEFAULT_BOUNDS)?;
            for source in &metadata {
                for part in &source.canonical_parts {
                    let rows = read_canonical_part_rows(&storage, &data_root, source, part)?
                        .into_iter()
                        .filter(|row| {
                            passes_parallax_quality(
                                row.parallax,
                                row.parallax_error,
                                row.phot_g_mean_mag,
                                args.quality_threshold,
                            )
                        })
                        .collect();
                    builder.append_rows(rows)?;
                }
            }
            let (index, rows_in_bounds) = builder.finish()?;
            storage.validate_serving_layout(&data_root, &index)?;
            Ok(BuildIndexResult {
                index,
                sources_seen: metadata.len(),
                rows_in_bounds,
            })
        }
        ref root @ StorageRoot::Gcs(_) => {
            let local_output = tempdir()?;
            let mut builder =
                IndexBuilder::new(local_output.path(), args.octree_depth, DEFAULT_BOUNDS)?;
            for source in &metadata {
                for part in &source.canonical_parts {
                    let rows = read_canonical_part_rows(&storage, &data_root, source, part)?
                        .into_iter()
                        .filter(|row| {
                            passes_parallax_quality(
                                row.parallax,
                                row.parallax_error,
                                row.phot_g_mean_mag,
                                args.quality_threshold,
                            )
                        })
                        .collect();
                    builder.append_rows(rows)?;
                }
            }
            let (index, rows_in_bounds) = builder.finish()?;
            storage.upload_directory(local_output.path(), root)?;
            storage.validate_serving_layout(&data_root, &index)?;
            Ok(BuildIndexResult {
                index,
                sources_seen: metadata.len(),
                rows_in_bounds,
            })
        }
    }
}
