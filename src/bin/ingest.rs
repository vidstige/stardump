use clap::Parser;
use gaia_viz::ingest::{DEFAULT_BOUNDS, DEFAULT_DEPTH, IngestConfig, run_ingestion};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    input: String,

    #[arg(long)]
    output_root: String,

    #[arg(long)]
    parallax_filter_mas: Option<f32>,

    #[arg(long, default_value_t = DEFAULT_DEPTH)]
    octree_depth: u8,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    run_ingestion(IngestConfig {
        input: args.input,
        output_root: args.output_root,
        parallax_filter_mas: args.parallax_filter_mas,
        octree_depth: args.octree_depth,
        bounds: DEFAULT_BOUNDS,
    })?;
    Ok(())
}
