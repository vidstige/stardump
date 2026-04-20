use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{Method, StatusCode};
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tower_http::cors::{Any, CorsLayer};

use crate::starcloud::STARCLOUD_FILENAME;
use crate::storage::local_path;

pub struct QueryCatalog {
    data_root: PathBuf,
}

fn internal_error(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn not_found(message: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, message.into())
}

fn bad_request(message: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, message.into())
}

fn valid_dataset_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
}

fn dataset_root(data_root: &Path, name: &str) -> PathBuf {
    data_root.join(name)
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn list_indices(
    State(catalog): State<Arc<QueryCatalog>>,
) -> Result<Response, (StatusCode, String)> {
    let body = tokio::task::spawn_blocking(move || {
        let names = catalog.list_names()?;
        Ok::<_, anyhow::Error>(if names.is_empty() {
            String::new()
        } else {
            format!("{}\n", names.join("\n"))
        })
    })
    .await
    .map_err(|error| internal_error(error.into()))?
    .map_err(internal_error)?;
    Ok((
        [(CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"))],
        body,
    )
        .into_response())
}

async fn serve_starcloud(
    State(catalog): State<Arc<QueryCatalog>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, (StatusCode, String)> {
    if !valid_dataset_name(&name) {
        return Err(bad_request("dataset name contains invalid characters"));
    }
    let path = dataset_root(&catalog.data_root, &name).join(STARCLOUD_FILENAME);
    let file = tokio::fs::File::open(&path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            not_found(format!("unknown starcloud dataset {name}"))
        } else {
            internal_error(error.into())
        }
    })?;
    let metadata = file
        .metadata()
        .await
        .map_err(|error| internal_error(error.into()))?;
    let stream = tokio_util::io::ReaderStream::new(file);
    Ok((
        [
            (CONTENT_TYPE, HeaderValue::from_static("application/octet-stream")),
            (
                CONTENT_LENGTH,
                HeaderValue::from_str(&metadata.len().to_string()).unwrap(),
            ),
        ],
        Body::from_stream(stream),
    )
        .into_response())
}

impl QueryCatalog {
    pub fn load(root: &str) -> Result<Self> {
        Ok(Self {
            data_root: local_path(root)?,
        })
    }

    pub fn list_names(&self) -> Result<Vec<String>> {
        if !self.data_root.exists() {
            return Ok(Vec::new());
        }

        let mut names = Vec::new();
        for entry in fs::read_dir(&self.data_root)
            .with_context(|| format!("failed to read {}", self.data_root.display()))?
        {
            let entry =
                entry.with_context(|| format!("failed to read {}", self.data_root.display()))?;
            let path = entry.path();
            if !entry
                .file_type()
                .with_context(|| format!("failed to stat {}", path.display()))?
                .is_dir()
            {
                continue;
            }

            let name = entry.file_name().to_string_lossy().into_owned();
            if !valid_dataset_name(&name) || !path.join(STARCLOUD_FILENAME).is_file() {
                continue;
            }
            names.push(name);
        }
        names.sort();
        Ok(names)
    }
}

pub fn build_app(catalog: Arc<QueryCatalog>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/indices", get(list_indices))
        .route("/datasets/{name}/starcloud.bin", get(serve_starcloud))
        .layer(CorsLayer::new().allow_origin(Any).allow_methods([Method::GET]))
        .with_state(catalog)
}
