// Serves the built viewer: index.html at the root, bundles under /dist.
//
// Hosting it from the query API keeps the page same-origin with the data it
// streams, so byte-range requests need no cross-origin preflight.

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::http::header::{CONTENT_TYPE, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

const INDEX: &str = "index.html";

fn content_type(name: &str) -> &'static str {
    match name.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn valid_asset_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_')
}

async fn serve(root: &PathBuf, relative: &str, name: &str) -> Response {
    match tokio::fs::read(root.join(relative)).await {
        Ok(bytes) => (
            [(CONTENT_TYPE, HeaderValue::from_static(content_type(name)))],
            bytes,
        )
            .into_response(),
        Err(_) => (StatusCode::NOT_FOUND, format!("no viewer asset {name}")).into_response(),
    }
}

async fn index(State(root): State<Arc<PathBuf>>) -> Response {
    serve(&root, INDEX, INDEX).await
}

async fn asset(State(root): State<Arc<PathBuf>>, AxumPath(name): AxumPath<String>) -> Response {
    if !valid_asset_name(&name) {
        return (StatusCode::BAD_REQUEST, "bad asset name").into_response();
    }
    serve(&root, &format!("dist/{name}"), &name).await
}

pub fn build_viewer(root: PathBuf) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/dist/{name}", get(asset))
        .with_state(Arc::new(root))
}
