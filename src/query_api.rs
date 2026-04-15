use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
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
    OCTREE_INDEX_FILENAME, PackedOctreeIndex, PackedOctreeNode, decode_packed_points,
    dequantize_point, read_packed_octree,
};
use crate::octree::Bounds3;
use crate::storage::{local_path, validate_packed_index_layout};

#[derive(Clone)]
pub struct QueryDataset {
    index: PackedOctreeIndex,
    index_path: PathBuf,
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

fn distance(center: [f32; 3], point: [f32; 3]) -> f32 {
    let dx = point[0] as f64 - center[0] as f64;
    let dy = point[1] as f64 - center[1] as f64;
    let dz = point[2] as f64 - center[2] as f64;
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

fn read_leaf_points(
    file: &mut fs::File,
    index: &PackedOctreeIndex,
    node: PackedOctreeNode,
) -> Result<Vec<crate::formats::PackedPoint>> {
    let offset = index.point_data_offset() + node.first as u64 * crate::formats::PACKED_POINT_SIZE;
    let byte_count = node.count as usize * crate::formats::PACKED_POINT_SIZE as usize;
    let mut bytes = vec![0_u8; byte_count];
    file.seek(SeekFrom::Start(offset))
        .context("failed to seek packed point range")?;
    file.read_exact(&mut bytes)
        .context("failed to read packed point range")?;
    decode_packed_points(&bytes)
}

fn collect_matches(
    file: &mut fs::File,
    index: &PackedOctreeIndex,
    center: [f32; 3],
    radius: f32,
    bounds: Bounds3,
    node_index: u32,
    matches: &mut Vec<RadiusQueryMatch>,
) -> Result<()> {
    if !bounds.intersects_sphere(center, radius) {
        return Ok(());
    }

    let node = *index
        .nodes
        .get(node_index as usize)
        .ok_or_else(|| anyhow::anyhow!("node index {node_index} is out of bounds"))?;

    if node.child_mask == 0 {
        for point in read_leaf_points(file, index, node)? {
            let xyz = dequantize_point(bounds, &point);
            if distance(center, xyz) <= radius {
                matches.push(RadiusQueryMatch {
                    x: xyz[0],
                    y: xyz[1],
                    z: xyz[2],
                    source_id: point.source_id,
                });
            }
        }
        return Ok(());
    }

    let mut child_index = node.first;
    for child in 0..8 {
        if node.child_mask & (1 << child) == 0 {
            continue;
        }
        collect_matches(
            file,
            index,
            center,
            radius,
            bounds.child_bounds(child),
            child_index,
            matches,
        )?;
        child_index += 1;
    }
    Ok(())
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
        let index_path = data_root.join(OCTREE_INDEX_FILENAME);
        let index = read_packed_octree(&index_path).context("failed to parse packed octree")?;
        validate_packed_index_layout(data_root, &index)?;
        Ok(Self { index, index_path })
    }

    pub fn query_radius(&self, request: RadiusQueryRequest) -> Result<Vec<RadiusQueryMatch>> {
        if !request.radius.is_finite() || request.radius <= 0.0 {
            bail!("radius must be a positive finite number");
        }
        let limit = request.limit.unwrap_or(1000);
        if limit == 0 {
            bail!("limit must be greater than zero");
        }

        if self.index.nodes.is_empty() {
            return Ok(Vec::new());
        }

        let center = [request.x, request.y, request.z];
        let mut file = fs::File::open(&self.index_path)
            .with_context(|| format!("failed to open {}", self.index_path.display()))?;
        let mut matches = Vec::new();
        collect_matches(
            &mut file,
            &self.index,
            center,
            request.radius,
            self.index.bounds(),
            0,
            &mut matches,
        )?;
        matches.sort_by_key(|row| row.source_id);
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::tempdir;

    use crate::formats::{
        PackedOctreeNode, PackedPoint, encode_packed_octree, encode_packed_points,
    };

    use super::*;

    #[test]
    fn query_radius_reads_leaf_payloads_from_packed_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(OCTREE_INDEX_FILENAME);
        let index = PackedOctreeIndex {
            depth: 1,
            half_extent_pc: 1.0,
            point_count: 2,
            nodes: vec![
                PackedOctreeNode {
                    child_mask: 0b0000_0001,
                    first: 1,
                    count: 0,
                },
                PackedOctreeNode {
                    child_mask: 0,
                    first: 0,
                    count: 2,
                },
            ],
        };
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(&encode_packed_octree(&index)).unwrap();
        file.write_all(&encode_packed_points(&[
            PackedPoint {
                source_id: 1,
                x_local: 0,
                y_local: 0,
                z_local: 0,
            },
            PackedPoint {
                source_id: 2,
                x_local: u16::MAX,
                y_local: u16::MAX,
                z_local: u16::MAX,
            },
        ]))
        .unwrap();

        let dataset = QueryDataset::load(dir.path()).unwrap();
        let matches = dataset
            .query_radius(RadiusQueryRequest {
                x: -0.99,
                y: -0.99,
                z: -0.99,
                radius: 0.1,
                limit: Some(10),
            })
            .unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].source_id, 1);
    }
}
