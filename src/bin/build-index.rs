use clap::Parser;
use star_dump::build_index::{BuildIndexConfig, DEFAULT_BOUNDS, DEFAULT_DEPTH, run_build_index};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    data_root: String,

    #[arg(long, default_value_t = DEFAULT_DEPTH)]
    octree_depth: u8,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    run_build_index(BuildIndexConfig {
        data_root: args.data_root,
        octree_depth: args.octree_depth,
        bounds: DEFAULT_BOUNDS,
    })?;
    Ok(())
}
