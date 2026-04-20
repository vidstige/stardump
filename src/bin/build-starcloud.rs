use clap::Parser;
use std::time::Instant;

use star_dump::build_starcloud::{
    BuildStarcloudConfig, DEFAULT_DEPTH, DEFAULT_SAMPLE_BUDGET, run_build_starcloud,
};
use star_dump::quality::DEFAULT_PARALLAX_QUALITY_THRESHOLD;

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

    #[arg(long, default_value_t = DEFAULT_SAMPLE_BUDGET)]
    sample_budget: usize,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let started = Instant::now();
    let output_root = args.output_root.clone().unwrap_or_else(|| args.data_root.clone());
    let result = run_build_starcloud(BuildStarcloudConfig {
        data_root: args.data_root,
        output_root,
        octree_depth: args.octree_depth,
        quality_threshold: args.quality_threshold,
        sample_budget: args.sample_budget,
    })?;
    println!(
        "build-starcloud: finished sources={} rows_in_bounds={} nodes={} points={} output={} elapsed_s={:.1}",
        result.sources_seen,
        result.rows_in_bounds,
        result.node_count,
        result.point_count,
        result.output_path.display(),
        started.elapsed().as_secs_f32(),
    );
    Ok(())
}
