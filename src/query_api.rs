use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result, bail};
use axum::Router;
use axum::extract::{Path as AxumPath, RawQuery, State};
use axum::http::{Method, StatusCode};
use axum::http::header::{CONTENT_TYPE, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use csv::Writer;
use tower_http::cors::{Any, CorsLayer};

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

#[derive(Debug, Clone, Copy)]
pub struct FrustumQueryRequest {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub qx: f32,
    pub qy: f32,
    pub qz: f32,
    pub qw: f32,
    pub near: f32,
    pub far: f32,
    pub fovy: f32,
    pub aspect: f32,
    pub limit: Option<usize>,
}

#[derive(Debug, PartialEq)]
pub struct QueryMatch {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub source_id: u64,
}

#[derive(Clone, Copy)]
struct Plane {
    normal: [f32; 3],
    constant: f32,
}

#[derive(Clone, Copy)]
struct DerivedFrustum {
    position: [f32; 3],
    forward: [f32; 3],
    right: [f32; 3],
    up: [f32; 3],
    near: f32,
    far: f32,
    tan_half_fovy: f32,
    aspect: f32,
    planes: [Plane; 6],
}

const DEFAULT_LIMIT: usize = 1000;

fn distance(center: [f32; 3], point: [f32; 3]) -> f32 {
    let dx = point[0] as f64 - center[0] as f64;
    let dy = point[1] as f64 - center[1] as f64;
    let dz = point[2] as f64 - center[2] as f64;
    (dx * dx + dy * dy + dz * dz).sqrt() as f32
}

fn dot(a: [f32; 3], b: [f32; 3]) -> f32 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

fn subtract(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}

fn add(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
}

fn scale(v: [f32; 3], amount: f32) -> [f32; 3] {
    [v[0] * amount, v[1] * amount, v[2] * amount]
}

fn cross(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn length(v: [f32; 3]) -> f32 {
    dot(v, v).sqrt()
}

fn normalize(v: [f32; 3]) -> Option<[f32; 3]> {
    let len = length(v);
    if !len.is_finite() || len == 0.0 {
        return None;
    }
    Some(scale(v, 1.0 / len))
}

fn normalize_quaternion(q: [f32; 4]) -> Option<[f32; 4]> {
    let len = (q[0] * q[0] + q[1] * q[1] + q[2] * q[2] + q[3] * q[3]).sqrt();
    if !len.is_finite() || len == 0.0 {
        return None;
    }
    Some([q[0] / len, q[1] / len, q[2] / len, q[3] / len])
}

fn rotate_vector(q: [f32; 4], v: [f32; 3]) -> [f32; 3] {
    let qv = [q[0], q[1], q[2]];
    let uv = cross(qv, v);
    let uuv = cross(qv, uv);
    add(v, add(scale(uv, 2.0 * q[3]), scale(uuv, 2.0)))
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
    bounds: Bounds3,
    node_index: u32,
    matches: &mut Vec<QueryMatch>,
    bounds_match: &dyn Fn(Bounds3) -> bool,
    point_match: &dyn Fn([f32; 3]) -> bool,
) -> Result<()> {
    if !bounds_match(bounds) {
        return Ok(());
    }

    let node = *index
        .nodes
        .get(node_index as usize)
        .ok_or_else(|| anyhow::anyhow!("node index {node_index} is out of bounds"))?;

    if node.child_mask == 0 {
        for point in read_leaf_points(file, index, node)? {
            let xyz = dequantize_point(bounds, &point);
            if point_match(xyz) {
                matches.push(QueryMatch {
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
            bounds.child_bounds(child),
            child_index,
            matches,
            bounds_match,
            point_match,
        )?;
        child_index += 1;
    }
    Ok(())
}

fn plane_from_point_normal(point: [f32; 3], normal: [f32; 3]) -> Plane {
    let unit = normalize(normal).expect("plane normal must be non-zero");
    Plane {
        normal: unit,
        constant: -dot(unit, point),
    }
}

fn plane_from_points(a: [f32; 3], b: [f32; 3], c: [f32; 3], inside: [f32; 3]) -> Plane {
    let mut normal = normalize(cross(subtract(b, a), subtract(c, a)))
        .expect("plane points must not be collinear");
    if dot(normal, subtract(inside, a)) < 0.0 {
        normal = scale(normal, -1.0);
    }
    plane_from_point_normal(a, normal)
}

fn derive_frustum(request: FrustumQueryRequest) -> Result<DerivedFrustum> {
    let position = [request.x, request.y, request.z];
    let orientation = normalize_quaternion([request.qx, request.qy, request.qz, request.qw])
        .ok_or_else(|| anyhow::anyhow!("orientation quaternion must be non-zero"))?;
    let forward = normalize(rotate_vector(orientation, [0.0, 0.0, -1.0]))
        .ok_or_else(|| anyhow::anyhow!("forward vector must be non-zero"))?;
    let right = normalize(rotate_vector(orientation, [1.0, 0.0, 0.0]))
        .ok_or_else(|| anyhow::anyhow!("right vector must be non-zero"))?;
    let up = normalize(rotate_vector(orientation, [0.0, 1.0, 0.0]))
        .ok_or_else(|| anyhow::anyhow!("up vector must be non-zero"))?;
    let near_center = add(position, scale(forward, request.near));
    let far_center = add(position, scale(forward, request.far));
    let near_half_height = request.near * (request.fovy * 0.5).tan();
    let near_half_width = near_half_height * request.aspect;
    let inside = add(
        position,
        scale(forward, request.near + (request.far - request.near) * 0.5),
    );

    let near_top_left = add(
        add(near_center, scale(up, near_half_height)),
        scale(right, -near_half_width),
    );
    let near_top_right = add(
        add(near_center, scale(up, near_half_height)),
        scale(right, near_half_width),
    );
    let near_bottom_left = add(
        add(near_center, scale(up, -near_half_height)),
        scale(right, -near_half_width),
    );
    let near_bottom_right = add(
        add(near_center, scale(up, -near_half_height)),
        scale(right, near_half_width),
    );

    Ok(DerivedFrustum {
        position,
        forward,
        right,
        up,
        near: request.near,
        far: request.far,
        tan_half_fovy: (request.fovy * 0.5).tan(),
        aspect: request.aspect,
        planes: [
            plane_from_point_normal(near_center, forward),
            plane_from_point_normal(far_center, scale(forward, -1.0)),
            plane_from_points(position, near_bottom_left, near_top_left, inside),
            plane_from_points(position, near_top_right, near_bottom_right, inside),
            plane_from_points(position, near_top_left, near_top_right, inside),
            plane_from_points(position, near_bottom_right, near_bottom_left, inside),
        ],
    })
}

fn bounds_corners(bounds: Bounds3) -> [[f32; 3]; 8] {
    let [min_x, min_y, min_z] = bounds.min;
    let [max_x, max_y, max_z] = bounds.max;
    [
        [min_x, min_y, min_z],
        [min_x, min_y, max_z],
        [min_x, max_y, min_z],
        [min_x, max_y, max_z],
        [max_x, min_y, min_z],
        [max_x, min_y, max_z],
        [max_x, max_y, min_z],
        [max_x, max_y, max_z],
    ]
}

fn plane_distance(plane: Plane, point: [f32; 3]) -> f32 {
    dot(plane.normal, point) + plane.constant
}

fn bounds_intersect_frustum(bounds: Bounds3, frustum: &DerivedFrustum) -> bool {
    let corners = bounds_corners(bounds);
    frustum
        .planes
        .iter()
        .all(|plane| corners.iter().any(|corner| plane_distance(*plane, *corner) >= 0.0))
}

fn point_in_frustum(point: [f32; 3], frustum: &DerivedFrustum) -> bool {
    let relative = subtract(point, frustum.position);
    let depth = dot(relative, frustum.forward);
    if depth < frustum.near || depth > frustum.far {
        return false;
    }

    let half_height = depth * frustum.tan_half_fovy;
    let half_width = half_height * frustum.aspect;
    let horizontal = dot(relative, frustum.right);
    let vertical = dot(relative, frustum.up);
    horizontal.abs() <= half_width && vertical.abs() <= half_height
}

fn query_limit(limit: Option<usize>) -> Result<usize> {
    let limit = limit.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 {
        bail!("limit must be greater than zero");
    }
    Ok(limit)
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

async fn query_frustum(
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
    let request = parse_frustum_query_request(query.as_deref())?;
    let csv = tokio::task::spawn_blocking(move || query_frustum_csv(&dataset, request))
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

    pub fn query_radius(&self, request: RadiusQueryRequest) -> Result<Vec<QueryMatch>> {
        if !request.radius.is_finite() || request.radius <= 0.0 {
            bail!("radius must be a positive finite number");
        }
        let limit = query_limit(request.limit)?;

        if self.index.nodes.is_empty() {
            return Ok(Vec::new());
        }

        let center = [request.x, request.y, request.z];
        let mut file = fs::File::open(&self.index_path)
            .with_context(|| format!("failed to open {}", self.index_path.display()))?;
        let mut matches = Vec::new();
        let bounds_match = |bounds: Bounds3| bounds.intersects_sphere(center, request.radius);
        let point_match = |point: [f32; 3]| distance(center, point) <= request.radius;
        collect_matches(
            &mut file,
            &self.index,
            self.index.bounds(),
            0,
            &mut matches,
            &bounds_match,
            &point_match,
        )?;
        matches.sort_by_key(|row| row.source_id);
        matches.truncate(limit);
        Ok(matches)
    }

    pub fn query_frustum(&self, request: FrustumQueryRequest) -> Result<Vec<QueryMatch>> {
        let frustum = derive_frustum(request)?;
        let limit = query_limit(request.limit)?;

        if self.index.nodes.is_empty() {
            return Ok(Vec::new());
        }

        let mut file = fs::File::open(&self.index_path)
            .with_context(|| format!("failed to open {}", self.index_path.display()))?;
        let mut matches = Vec::new();
        let bounds_match = |bounds: Bounds3| bounds_intersect_frustum(bounds, &frustum);
        let point_match = |point: [f32; 3]| point_in_frustum(point, &frustum);
        collect_matches(
            &mut file,
            &self.index,
            self.index.bounds(),
            0,
            &mut matches,
            &bounds_match,
            &point_match,
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
        .route("/query/{name}/frustum", get(query_frustum))
        .layer(CorsLayer::new().allow_origin(Any).allow_methods([Method::GET]))
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

fn parse_frustum_query_request(query: Option<&str>) -> Result<FrustumQueryRequest, (StatusCode, String)> {
    let query = query.unwrap_or_default();
    Ok(FrustumQueryRequest {
        x: parse_required_f32(query, "x")?,
        y: parse_required_f32(query, "y")?,
        z: parse_required_f32(query, "z")?,
        qx: parse_required_f32(query, "qx")?,
        qy: parse_required_f32(query, "qy")?,
        qz: parse_required_f32(query, "qz")?,
        qw: parse_required_f32(query, "qw")?,
        near: parse_required_f32(query, "near")?,
        far: parse_required_f32(query, "far")?,
        fovy: parse_required_f32(query, "fovy")?,
        aspect: parse_required_f32(query, "aspect")?,
        limit: parse_optional_usize(query, "limit")?,
    })
}

pub fn validate_radius_request(request: &RadiusQueryRequest) -> Result<(), (StatusCode, String)> {
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

pub fn validate_frustum_request(request: &FrustumQueryRequest) -> Result<(), (StatusCode, String)> {
    let finite = [
        request.x,
        request.y,
        request.z,
        request.qx,
        request.qy,
        request.qz,
        request.qw,
        request.near,
        request.far,
        request.fovy,
        request.aspect,
    ]
    .into_iter()
    .all(f32::is_finite);
    if !finite {
        return Err(bad_request("frustum parameters must contain only finite numbers"));
    }
    if request.near <= 0.0 {
        return Err(bad_request("near must be a positive finite number"));
    }
    if request.far <= request.near {
        return Err(bad_request("far must be greater than near"));
    }
    if request.fovy <= 0.0 || request.fovy >= std::f32::consts::PI {
        return Err(bad_request("fovy must be between 0 and pi"));
    }
    if request.aspect <= 0.0 {
        return Err(bad_request("aspect must be a positive finite number"));
    }
    if normalize_quaternion([request.qx, request.qy, request.qz, request.qw]).is_none() {
        return Err(bad_request("orientation quaternion must be non-zero"));
    }
    if request.limit == Some(0) {
        return Err(bad_request("limit must be greater than zero"));
    }
    Ok(())
}

fn encode_matches_csv(matches: &[QueryMatch]) -> Result<String, (StatusCode, String)> {
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
    validate_radius_request(&request)?;
    let matches = dataset.query_radius(request).map_err(internal_error)?;
    encode_matches_csv(&matches)
}

pub fn query_frustum_csv(
    dataset: &QueryDataset,
    request: FrustumQueryRequest,
) -> Result<String, (StatusCode, String)> {
    validate_frustum_request(&request)?;
    let matches = dataset.query_frustum(request).map_err(internal_error)?;
    encode_matches_csv(&matches)
}

#[cfg(test)]
mod tests {
    use std::f32::consts::FRAC_PI_6;
    use std::f32::consts::FRAC_1_SQRT_2;
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

    #[test]
    fn query_frustum_reads_leaf_payloads_from_packed_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(OCTREE_INDEX_FILENAME);
        let index = PackedOctreeIndex {
            depth: 0,
            half_extent_pc: 1.0,
            point_count: 3,
            nodes: vec![PackedOctreeNode {
                child_mask: 0,
                first: 0,
                count: 3,
            }],
        };
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(&encode_packed_octree(&index)).unwrap();
        file.write_all(&encode_packed_points(&[
            PackedPoint {
                source_id: 1,
                x_local: 0,
                y_local: 32_768,
                z_local: 32_768,
            },
            PackedPoint {
                source_id: 2,
                x_local: u16::MAX,
                y_local: 32_768,
                z_local: 32_768,
            },
            PackedPoint {
                source_id: 3,
                x_local: 32_768,
                y_local: u16::MAX,
                z_local: 32_768,
            },
        ]))
        .unwrap();

        let dataset = QueryDataset::load(dir.path()).unwrap();
        let matches = dataset
            .query_frustum(FrustumQueryRequest {
                x: -2.0,
                y: 0.0,
                z: 0.0,
                qx: 0.0,
                qy: -FRAC_1_SQRT_2,
                qz: 0.0,
                qw: FRAC_1_SQRT_2,
                near: 0.5,
                far: 4.0,
                fovy: FRAC_PI_6,
                aspect: 1.0,
                limit: Some(10),
            })
            .unwrap();

        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].source_id, 1);
        assert_eq!(matches[1].source_id, 2);
    }
}
