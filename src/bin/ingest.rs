use clap::Parser;
use star_dump::ingest::{IngestConfig, run_ingestion};

fn parse_cloud_run_usize(name: &str) -> anyhow::Result<Option<usize>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value.parse()?)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_manifest(path: &str) -> anyhow::Result<Vec<String>> {
    Ok(std::fs::read_to_string(path)?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

fn sharded_inputs(inputs: Vec<String>) -> anyhow::Result<Vec<String>> {
    let task_index = parse_cloud_run_usize("CLOUD_RUN_TASK_INDEX")?;
    let task_count = parse_cloud_run_usize("CLOUD_RUN_TASK_COUNT")?;

    match (task_index, task_count) {
        (Some(index), Some(count)) => {
            if count == 0 {
                anyhow::bail!("CLOUD_RUN_TASK_COUNT must be greater than zero");
            }
            if index >= count {
                anyhow::bail!("CLOUD_RUN_TASK_INDEX must be less than CLOUD_RUN_TASK_COUNT");
            }
            Ok(inputs
                .into_iter()
                .enumerate()
                .filter_map(|(offset, input)| (offset % count == index).then_some(input))
                .collect())
        }
        (None, None) => Ok(inputs),
        _ => anyhow::bail!("CLOUD_RUN_TASK_INDEX and CLOUD_RUN_TASK_COUNT must be set together"),
    }
}

fn collect_inputs(
    inputs: Vec<String>,
    input_manifest: Option<String>,
) -> anyhow::Result<Vec<String>> {
    let mut all_inputs = inputs;
    if let Some(input_manifest) = input_manifest {
        all_inputs.extend(read_manifest(&input_manifest)?);
    }
    if all_inputs.is_empty() {
        anyhow::bail!("at least one --input or --input-manifest value is required");
    }
    sharded_inputs(all_inputs)
}

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
    let inputs = collect_inputs(args.input, args.input_manifest)?;
    if inputs.is_empty() {
        return Ok(());
    }
    run_ingestion(IngestConfig {
        inputs,
        output_root: args.output_root,
    })?;
    Ok(())
}
