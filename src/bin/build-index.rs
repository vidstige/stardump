use clap::Parser;
use std::time::Instant;

use star_dump::build_index::{
    BuildIndexConfig, BuildIndexResult, DEFAULT_DEPTH, IndexBuilder, bounds_for_quality_threshold,
    load_source_metadata, read_canonical_part_rows, run_build_index,
};
use star_dump::quality::{DEFAULT_PARALLAX_QUALITY_THRESHOLD, passes_parallax_quality};
use star_dump::storage::{local_path, validate_serving_layout};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    data_root: String,

    #[arg(long)]
    output_root: Option<String>,

    #[arg(long, default_value_t = DEFAULT_DEPTH)]
    octree_depth: u8,

    #[arg(long, default_value_t = DEFAULT_PARALLAX_QUALITY_THRESHOLD)]
    quality_threshold: f32,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    run_quality_filtered_build_index(args)?;
    Ok(())
}

fn run_quality_filtered_build_index(args: Args) -> anyhow::Result<BuildIndexResult> {
    let data_root = local_path(&args.data_root)?;
    let output_root = local_path(args.output_root.as_deref().unwrap_or(&args.data_root))?;
    let metadata = load_source_metadata(&data_root)?;
    let started = Instant::now();

    if metadata.is_empty() {
        return run_build_index(BuildIndexConfig {
            data_root: args.data_root,
            output_root: args
                .output_root
                .unwrap_or_else(|| output_root.display().to_string()),
            octree_depth: args.octree_depth,
            bounds: bounds_for_quality_threshold(args.quality_threshold),
        });
    }

    let mut builder = IndexBuilder::new(
        &output_root,
        args.octree_depth,
        bounds_for_quality_threshold(args.quality_threshold),
    )?;
    let source_count = metadata.len();
    let mut rows_kept = 0_u64;
    for (index, source) in metadata.iter().enumerate() {
        for part in &source.canonical_parts {
            let rows = read_canonical_part_rows(&data_root, source, part)?
                .into_iter()
                .filter(|row| {
                    passes_parallax_quality(
                        row.parallax,
                        row.parallax_error,
                        row.phot_g_mean_mag,
                        args.quality_threshold,
                    )
                })
                .collect::<Vec<_>>();
            rows_kept += rows.len() as u64;
            builder.append_rows(rows)?;
        }
        println!(
            "build-index: sources {}/{} rows_kept={} elapsed_s={:.1}",
            index + 1,
            source_count,
            rows_kept,
            started.elapsed().as_secs_f32(),
        );
    }
    let (index, rows_in_bounds) = builder.finish()?;
    validate_serving_layout(&output_root, &index)?;
    println!(
        "build-index: finished sources={} rows_in_bounds={} leaves={} elapsed_s={:.1}",
        source_count,
        rows_in_bounds,
        index.leaves.len(),
        started.elapsed().as_secs_f32(),
    );
    Ok(BuildIndexResult {
        index,
        sources_seen: metadata.len(),
        rows_in_bounds,
    })
}
