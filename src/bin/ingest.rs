use clap::Parser;
use star_dump::ingest::{IngestConfig, run_ingestion};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    input: Vec<String>,

    #[arg(long)]
    input_manifest: Option<String>,

    #[arg(long)]
    output_root: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    run_ingestion(
        IngestConfig::new(args.output_root)
            .with_inputs(args.input)
            .with_input_manifest(args.input_manifest),
    )?;
    Ok(())
}
