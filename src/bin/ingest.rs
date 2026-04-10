use clap::Parser;
use star_dump::ingest::{IngestConfig, run_ingestion};

#[derive(Parser)]
struct Args {
    #[arg(long, required = true)]
    input: Vec<String>,

    #[arg(long)]
    output_root: String,

    #[arg(long)]
    parallax_filter_mas: Option<f32>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    run_ingestion(IngestConfig {
        inputs: args.input,
        output_root: args.output_root,
        parallax_filter_mas: args.parallax_filter_mas,
    })?;
    Ok(())
}
