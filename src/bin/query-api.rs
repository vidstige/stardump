use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use star_dump::query_api::{QueryCatalog, build_app};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "data")]
    data_root: String,

    #[arg(long, default_value = "127.0.0.1:3000")]
    bind: SocketAddr,

    /// Directory holding the built viewer: index.html and dist/.
    #[arg(long, default_value = "viewer")]
    viewer_root: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let catalog = Arc::new(QueryCatalog::load(&args.data_root)?);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(args.bind).await?;
        axum::serve(listener, build_app(catalog, args.viewer_root)).await?;
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}
