use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result, bail};
use axum::Router;
use axum::extract::{Path as AxumPath, RawQuery, State};
use axum::http::StatusCode;
use axum::http::header::{CONTENT_TYPE, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use csv::Writer;

use crate::formats::{
    OCTREE_INDEX_FILENAME, OctreeIndex, ServingRow, decode_octree_index, decode_serving_rows,
    indices_directory, leaf_filename,
};
use crate::octree::{OctreeConfig, morton_encode};
use crate::storage::{local_path, validate_serving_layout};

#[derive(Clone)]
pub struct QueryDataset {
    index: OctreeIndex,
    occupied_leaves: HashSet<u32>,
    indices_root: PathBuf,
}

pub struct QueryCatalog {
    data_root: PathBuf,
    datasets: RwLock<HashMap<String, Arc<QueryDataset>>>,
}

#[derive(Debug, Clone, Copy)]
pub struct RadiusQueryRequest {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub radius: f32,
    pub limit: Option<usize>,
}

#[derive(Debug, PartialEq)]
pub struct RadiusQueryMatch {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub source_id: u64,
}

fn distance(center: [f32; 3], row: &ServingRow) -> f32 {
    let dx = row.x as f64 - center[0] as f64;
    let dy = row.y as f64 - center[1] as f64;
    let dz = row.z as f64 - center[2] as f64;
    (dx * dx + dy * dy + dz * dz).sqrt() as f32
}

fn bad_request(message: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, message.into())
}

fn internal_error(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn not_found(message: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, message.into())
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

fn intersecting_leaves(
    dataset: &QueryDataset,
    octree: OctreeConfig,
    center: [f32; 3],
    radius: f32,
) -> Vec<u32> {
    let Some(ranges) = octree.leaf_ranges_for_bounds(
        [center[0] - radius, center[1] - radius, center[2] - radius],
        [center[0] + radius, center[1] + radius, center[2] + radius],
    ) else {
        return Vec::new();
    };

    let mut leaves = Vec::new();
    for x in ranges[0].0..=ranges[0].1 {
        for y in ranges[1].0..=ranges[1].1 {
            for z in ranges[2].0..=ranges[2].1 {
                let morton = morton_encode(x, y, z);
                if !dataset.occupied_leaves.contains(&morton) {
                    continue;
                }
                if octree.leaf_bounds(morton).intersects_sphere(center, radius) {
                    leaves.push(morton);
                }
            }
        }
    }
    leaves
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
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )],
        body,
    )
        .into_response())
}

async fn query_radius(
    State(catalog): State<Arc<QueryCatalog>>,
    AxumPath(name): AxumPath<String>,
    RawQuery(query): RawQuery,
) -> Result<Response, (StatusCode, String)> {
    if !valid_dataset_name(&name) {
        return Err(bad_request("dataset name contains invalid characters"));
    }
    let Some(dataset) = catalog.dataset(&name).map_err(internal_error)? else {
        return Err(not_found(format!("unknown dataset {name}")));
    };
    let request = parse_query_request(query.as_deref())?;
    let csv = tokio::task::spawn_blocking(move || query_radius_csv(&dataset, request))
        .await
        .map_err(|error| internal_error(error.into()))??;
    Ok((
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("text/csv; charset=utf-8"),
        )],
        csv,
    )
        .into_response())
}

impl QueryDataset {
    pub fn load(data_root: &Path) -> Result<Self> {
        let index: OctreeIndex = decode_octree_index(
            &fs::read(data_root.join(OCTREE_INDEX_FILENAME)).with_context(|| {
                format!(
                    "failed to read {}",
                    data_root.join(OCTREE_INDEX_FILENAME).display()
                )
            })?,
        )
        .context("failed to parse octree index")?;
        validate_serving_layout(data_root, &index)?;
        let indices_root = data_root.join(indices_directory(index.depth));
        Ok(Self {
            occupied_leaves: index.leaves.iter().copied().collect(),
            index,
            indices_root,
        })
    }

    pub fn query_radius(&self, request: RadiusQueryRequest) -> Result<Vec<RadiusQueryMatch>> {
        if !request.radius.is_finite() || request.radius <= 0.0 {
            bail!("radius must be a positive finite number");
        }
        let limit = request.limit.unwrap_or(1000);
        if limit == 0 {
            bail!("limit must be greater than zero");
        }

        let center = [request.x, request.y, request.z];
        let octree = OctreeConfig {
            depth: self.index.depth,
            bounds: self.index.bounds,
        };
        let mut matches = Vec::new();
        let intersecting_leaves = intersecting_leaves(self, octree, center, request.radius);

        for morton in &intersecting_leaves {
            let rows = decode_serving_rows(
                &fs::read(self.indices_root.join(leaf_filename(*morton))).with_context(|| {
                    format!(
                        "failed to read {}",
                        self.indices_root.join(leaf_filename(*morton)).display()
                    )
                })?,
            )?;
            for row in rows {
                let distance = distance(center, &row);
                if distance <= request.radius {
                    matches.push(RadiusQueryMatch {
                        x: row.x,
                        y: row.y,
                        z: row.z,
                        source_id: row.source_id,
                    });
                }
            }
        }

        matches.sort_by(|left, right| left.source_id.cmp(&right.source_id));
        matches.truncate(limit);
        Ok(matches)
    }
}

impl QueryCatalog {
    pub fn load(root: &str) -> Result<Self> {
        Ok(Self {
            data_root: local_path(root)?,
            datasets: RwLock::new(HashMap::new()),
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
            if !valid_dataset_name(&name) || !path.join(OCTREE_INDEX_FILENAME).is_file() {
                continue;
            }
            names.push(name);
        }
        names.sort();
        Ok(names)
    }

    pub fn dataset(&self, name: &str) -> Result<Option<Arc<QueryDataset>>> {
        if let Some(dataset) = self.datasets.read().unwrap().get(name) {
            return Ok(Some(dataset.clone()));
        }

        let root = dataset_root(&self.data_root, name);
        if !root.join(OCTREE_INDEX_FILENAME).is_file() {
            return Ok(None);
        }

        let dataset = Arc::new(QueryDataset::load(&root)?);
        let mut datasets = self.datasets.write().unwrap();
        Ok(Some(
            datasets
                .entry(name.to_string())
                .or_insert_with(|| dataset.clone())
                .clone(),
        ))
    }
}

pub fn build_app(catalog: Arc<QueryCatalog>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/indices", get(list_indices))
        .route("/query/{name}/radius", get(query_radius))
        .with_state(catalog)
}

fn parse_query_parameter(query: &str, name: &str) -> Result<Option<String>, (StatusCode, String)> {
    let mut value = None;
    for part in query.split('&') {
        let (raw_key, raw_value) = part.split_once('=').unwrap_or((part, ""));
        let key = urlencoding::decode(raw_key)
            .map_err(|_| bad_request("query string is not valid percent-encoding"))?;
        if key != name {
            continue;
        }
        if value.is_some() {
            return Err(bad_request(format!("duplicate query parameter {name}")));
        }
        let decoded = urlencoding::decode(raw_value)
            .map_err(|_| bad_request("query string is not valid percent-encoding"))?;
        value = Some(decoded.into_owned());
    }
    Ok(value)
}

fn parse_required_f32(query: &str, name: &str) -> Result<f32, (StatusCode, String)> {
    let value = parse_query_parameter(query, name)?
        .ok_or_else(|| bad_request(format!("missing query parameter {name}")))?;
    value
        .parse()
        .map_err(|_| bad_request(format!("query parameter {name} must be a number")))
}

fn parse_optional_usize(query: &str, name: &str) -> Result<Option<usize>, (StatusCode, String)> {
    let Some(value) = parse_query_parameter(query, name)? else {
        return Ok(None);
    };
    value
        .parse()
        .map(Some)
        .map_err(|_| bad_request(format!("query parameter {name} must be an integer")))
}

fn parse_query_request(query: Option<&str>) -> Result<RadiusQueryRequest, (StatusCode, String)> {
    let query = query.unwrap_or_default();
    Ok(RadiusQueryRequest {
        x: parse_required_f32(query, "x")?,
        y: parse_required_f32(query, "y")?,
        z: parse_required_f32(query, "z")?,
        radius: parse_required_f32(query, "r")?,
        limit: parse_optional_usize(query, "limit")?,
    })
}

pub fn validate_request(request: &RadiusQueryRequest) -> Result<(), (StatusCode, String)> {
    if !request.x.is_finite() || !request.y.is_finite() || !request.z.is_finite() {
        return Err(bad_request("query center must contain only finite numbers"));
    }
    if !request.radius.is_finite() || request.radius <= 0.0 {
        return Err(bad_request("radius must be a positive finite number"));
    }
    if request.limit == Some(0) {
        return Err(bad_request("limit must be greater than zero"));
    }
    Ok(())
}

fn encode_matches_csv(matches: &[RadiusQueryMatch]) -> Result<String, (StatusCode, String)> {
    let mut writer = Writer::from_writer(Vec::new());
    writer
        .write_record(["x", "y", "z", "source_id"])
        .map_err(|error| internal_error(error.into()))?;
    for row in matches {
        writer
            .serialize((row.x, row.y, row.z, row.source_id))
            .map_err(|error| internal_error(error.into()))?;
    }
    let bytes = writer
        .into_inner()
        .map_err(|error| internal_error(error.into_error().into()))?;
    String::from_utf8(bytes).map_err(|error| internal_error(error.into()))
}

pub fn query_radius_csv(
    dataset: &QueryDataset,
    request: RadiusQueryRequest,
) -> Result<String, (StatusCode, String)> {
    validate_request(&request)?;
    let matches = dataset.query_radius(request).map_err(internal_error)?;
    encode_matches_csv(&matches)
}
