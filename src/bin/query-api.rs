use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use gaia_viz::query_api::{QueryService, build_app};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    data_root: PathBuf,

    #[arg(long, default_value = "127.0.0.1:3000")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let service = Arc::new(QueryService::load(args.data_root)?);
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, build_app(service)).await?;
    Ok(())
}
