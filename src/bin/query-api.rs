use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use star_dump::query_api::{QueryService, build_app};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    data_root: String,

    #[arg(long, default_value = "127.0.0.1:3000")]
    bind: SocketAddr,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let service = Arc::new(QueryService::load(&args.data_root)?);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(args.bind).await?;
        axum::serve(listener, build_app(service)).await?;
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}
