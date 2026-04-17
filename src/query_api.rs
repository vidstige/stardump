use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
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
struct QueryDataset {
    index: PackedOctreeIndex,
    index_path: PathBuf,
}

pub struct QueryCatalog {
    data_root: PathBuf,
    datasets: RwLock<HashMap<String, Arc<QueryDataset>>>,
}

#[derive(Debug, PartialEq)]
struct QueryMatch {
    x: f32,
    y: f32,
    z: f32,
    source_id: u64,
}

#[derive(Clone, Copy, Debug)]
struct Vec3 {
    x: f32,
    y: f32,
    z: f32,
}

impl Vec3 {
    fn dot(self, other: Vec3) -> f32 {
        self.x * other.x + self.y * other.y + self.z * other.z
    }

    fn cross(self, other: Vec3) -> Vec3 {
        Vec3 {
            x: self.y * other.z - self.z * other.y,
            y: self.z * other.x - self.x * other.z,
            z: self.x * other.y - self.y * other.x,
        }
    }

    fn length(self) -> f32 {
        self.dot(self).sqrt()
    }

    fn normalize(self) -> Option<Vec3> {
        let len = self.length();
        if !len.is_finite() || len == 0.0 {
            return None;
        }
        Some(self * (1.0 / len))
    }
}

impl std::ops::Add for Vec3 {
    type Output = Vec3;
    fn add(self, other: Vec3) -> Vec3 {
        Vec3 { x: self.x + other.x, y: self.y + other.y, z: self.z + other.z }
    }
}

impl std::ops::Sub for Vec3 {
    type Output = Vec3;
    fn sub(self, other: Vec3) -> Vec3 {
        Vec3 { x: self.x - other.x, y: self.y - other.y, z: self.z - other.z }
    }
}

impl std::ops::Mul<f32> for Vec3 {
    type Output = Vec3;
    fn mul(self, scalar: f32) -> Vec3 {
        Vec3 { x: self.x * scalar, y: self.y * scalar, z: self.z * scalar }
    }
}

impl From<[f32; 3]> for Vec3 {
    fn from(v: [f32; 3]) -> Vec3 {
        Vec3 { x: v[0], y: v[1], z: v[2] }
    }
}

impl From<Vec3> for [f32; 3] {
    fn from(v: Vec3) -> [f32; 3] {
        [v.x, v.y, v.z]
    }
}

#[derive(Clone, Copy)]
struct Plane {
    normal: Vec3,
    constant: f32,
}

impl Plane {
    fn distance_to(self, point: Vec3) -> f32 {
        self.normal.dot(point) + self.constant
    }
}

#[derive(Clone, Copy)]
struct DerivedFrustum {
    position: Vec3,
    forward: Vec3,
    right: Vec3,
    up: Vec3,
    near: f32,
    far: f32,
    tan_half_fovy: f32,
    aspect: f32,
    planes: [Plane; 6],
}

const DEFAULT_LIMIT: usize = 1000;

fn normalize_quaternion(q: [f32; 4]) -> Option<[f32; 4]> {
    let len = (q[0] * q[0] + q[1] * q[1] + q[2] * q[2] + q[3] * q[3]).sqrt();
    if !len.is_finite() || len == 0.0 {
        return None;
    }
    Some([q[0] / len, q[1] / len, q[2] / len, q[3] / len])
}

fn rotate_vector(q: [f32; 4], v: Vec3) -> Vec3 {
    let qv = Vec3 { x: q[0], y: q[1], z: q[2] };
    let uv = qv.cross(v);
    let uuv = qv.cross(uv);
    v + uv * (2.0 * q[3]) + uuv * 2.0
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
    point_match: &dyn Fn(Vec3) -> bool,
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
            let xyz = Vec3::from(dequantize_point(bounds, &point));
            if point_match(xyz) {
                matches.push(QueryMatch {
                    x: xyz.x,
                    y: xyz.y,
                    z: xyz.z,
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

fn plane_from_point_normal(point: Vec3, normal: Vec3) -> Plane {
    let unit = normal.normalize().expect("plane normal must be non-zero");
    Plane {
        normal: unit,
        constant: -unit.dot(point),
    }
}

fn plane_from_points(a: Vec3, b: Vec3, c: Vec3, inside: Vec3) -> Plane {
    let mut normal = (b - a).cross(c - a).normalize().expect("plane points must not be collinear");
    if normal.dot(inside - a) < 0.0 {
        normal = normal * -1.0;
    }
    plane_from_point_normal(a, normal)
}

fn derive_frustum(
    x: f32, y: f32, z: f32,
    qx: f32, qy: f32, qz: f32, qw: f32,
    near: f32, far: f32, fovy: f32, aspect: f32,
) -> Result<DerivedFrustum> {
    let position = Vec3 { x, y, z };
    let orientation = normalize_quaternion([qx, qy, qz, qw])
        .ok_or_else(|| anyhow::anyhow!("orientation quaternion must be non-zero"))?;
    let forward = rotate_vector(orientation, Vec3 { x: 0.0, y: 0.0, z: -1.0 })
        .normalize()
        .ok_or_else(|| anyhow::anyhow!("forward vector must be non-zero"))?;
    let right = rotate_vector(orientation, Vec3 { x: 1.0, y: 0.0, z: 0.0 })
        .normalize()
        .ok_or_else(|| anyhow::anyhow!("right vector must be non-zero"))?;
    let up = rotate_vector(orientation, Vec3 { x: 0.0, y: 1.0, z: 0.0 })
        .normalize()
        .ok_or_else(|| anyhow::anyhow!("up vector must be non-zero"))?;
    let near_center = position + forward * near;
    let far_center = position + forward * far;
    let near_half_height = near * (fovy * 0.5).tan();
    let near_half_width = near_half_height * aspect;
    let inside = position + forward * (near + (far - near) * 0.5);

    let near_top_left = near_center + up * near_half_height + right * (-near_half_width);
    let near_top_right = near_center + up * near_half_height + right * near_half_width;
    let near_bottom_left = near_center + up * (-near_half_height) + right * (-near_half_width);
    let near_bottom_right = near_center + up * (-near_half_height) + right * near_half_width;

    Ok(DerivedFrustum {
        position,
        forward,
        right,
        up,
        near,
        far,
        tan_half_fovy: (fovy * 0.5).tan(),
        aspect,
        planes: [
            plane_from_point_normal(near_center, forward),
            plane_from_point_normal(far_center, forward * -1.0),
            plane_from_points(position, near_bottom_left, near_top_left, inside),
            plane_from_points(position, near_top_right, near_bottom_right, inside),
            plane_from_points(position, near_top_left, near_top_right, inside),
            plane_from_points(position, near_bottom_right, near_bottom_left, inside),
        ],
    })
}

fn bounds_corners(bounds: Bounds3) -> [Vec3; 8] {
    let [x0, y0, z0] = bounds.min;
    let [x1, y1, z1] = bounds.max;
    [
        Vec3 { x: x0, y: y0, z: z0 },
        Vec3 { x: x0, y: y0, z: z1 },
        Vec3 { x: x0, y: y1, z: z0 },
        Vec3 { x: x0, y: y1, z: z1 },
        Vec3 { x: x1, y: y0, z: z0 },
        Vec3 { x: x1, y: y0, z: z1 },
        Vec3 { x: x1, y: y1, z: z0 },
        Vec3 { x: x1, y: y1, z: z1 },
    ]
}

fn bounds_intersect_frustum(bounds: Bounds3, frustum: &DerivedFrustum) -> bool {
    let corners = bounds_corners(bounds);
    frustum
        .planes
        .iter()
        .all(|plane| corners.iter().any(|&corner| plane.distance_to(corner) >= 0.0))
}

fn point_in_frustum(point: Vec3, frustum: &DerivedFrustum) -> bool {
    let relative = point - frustum.position;
    let depth = relative.dot(frustum.forward);
    if depth < frustum.near || depth > frustum.far {
        return false;
    }

    let half_height = depth * frustum.tan_half_fovy;
    let half_width = half_height * frustum.aspect;
    let horizontal = relative.dot(frustum.right);
    let vertical = relative.dot(frustum.up);
    horizontal.abs() <= half_width && vertical.abs() <= half_height
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
    let q = query.as_deref().unwrap_or_default();
    let x: f32 = parse_required(q, "x")?;
    let y: f32 = parse_required(q, "y")?;
    let z: f32 = parse_required(q, "z")?;
    let radius: f32 = parse_required(q, "r")?;
    let limit: Option<usize> = parse_optional(q, "limit")?;
    if !x.is_finite() || !y.is_finite() || !z.is_finite() {
        return Err(bad_request("query center must contain only finite numbers"));
    }
    if !radius.is_finite() || radius <= 0.0 {
        return Err(bad_request("radius must be a positive finite number"));
    }
    if limit == Some(0) {
        return Err(bad_request("limit must be greater than zero"));
    }
    let csv = tokio::task::spawn_blocking(move || {
        let matches = dataset.query_radius(x, y, z, radius, limit).map_err(internal_error)?;
        encode_matches_csv(&matches)
    })
    .await
    .map_err(|error| internal_error(error.into()))??;
    Ok((
        [(CONTENT_TYPE, HeaderValue::from_static("text/csv; charset=utf-8"))],
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
    let q = query.as_deref().unwrap_or_default();
    let x: f32 = parse_required(q, "x")?;
    let y: f32 = parse_required(q, "y")?;
    let z: f32 = parse_required(q, "z")?;
    let qx: f32 = parse_required(q, "qx")?;
    let qy: f32 = parse_required(q, "qy")?;
    let qz: f32 = parse_required(q, "qz")?;
    let qw: f32 = parse_required(q, "qw")?;
    let near: f32 = parse_required(q, "near")?;
    let far: f32 = parse_required(q, "far")?;
    let fovy: f32 = parse_required(q, "fovy")?;
    let aspect: f32 = parse_required(q, "aspect")?;
    let limit: Option<usize> = parse_optional(q, "limit")?;
    if ![x, y, z, qx, qy, qz, qw, near, far, fovy, aspect].into_iter().all(f32::is_finite) {
        return Err(bad_request("frustum parameters must contain only finite numbers"));
    }
    if near <= 0.0 {
        return Err(bad_request("near must be a positive finite number"));
    }
    if far <= near {
        return Err(bad_request("far must be greater than near"));
    }
    if fovy <= 0.0 || fovy >= std::f32::consts::PI {
        return Err(bad_request("fovy must be between 0 and pi"));
    }
    if aspect <= 0.0 {
        return Err(bad_request("aspect must be a positive finite number"));
    }
    if normalize_quaternion([qx, qy, qz, qw]).is_none() {
        return Err(bad_request("orientation quaternion must be non-zero"));
    }
    if limit == Some(0) {
        return Err(bad_request("limit must be greater than zero"));
    }
    let csv = tokio::task::spawn_blocking(move || {
        let matches = dataset
            .query_frustum(x, y, z, qx, qy, qz, qw, near, far, fovy, aspect, limit)
            .map_err(internal_error)?;
        encode_matches_csv(&matches)
    })
    .await
    .map_err(|error| internal_error(error.into()))??;
    Ok((
        [(CONTENT_TYPE, HeaderValue::from_static("text/csv; charset=utf-8"))],
        csv,
    )
        .into_response())
}

impl QueryDataset {
    fn load(data_root: &Path) -> Result<Self> {
        let index_path = data_root.join(OCTREE_INDEX_FILENAME);
        let index = read_packed_octree(&index_path).context("failed to parse packed octree")?;
        validate_packed_index_layout(data_root, &index)?;
        Ok(Self { index, index_path })
    }

    fn query_radius(&self, x: f32, y: f32, z: f32, radius: f32, limit: Option<usize>) -> Result<Vec<QueryMatch>> {
        let limit = limit.unwrap_or(DEFAULT_LIMIT);
        if self.index.nodes.is_empty() {
            return Ok(Vec::new());
        }
        let center = Vec3 { x, y, z };
        let mut file = fs::File::open(&self.index_path)
            .with_context(|| format!("failed to open {}", self.index_path.display()))?;
        let mut matches = Vec::new();
        let bounds_match = |bounds: Bounds3| bounds.intersects_sphere(center.into(), radius);
        let point_match = |point: Vec3| (point - center).length() <= radius;
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

    fn query_frustum(
        &self,
        x: f32, y: f32, z: f32,
        qx: f32, qy: f32, qz: f32, qw: f32,
        near: f32, far: f32, fovy: f32, aspect: f32,
        limit: Option<usize>,
    ) -> Result<Vec<QueryMatch>> {
        let frustum = derive_frustum(x, y, z, qx, qy, qz, qw, near, far, fovy, aspect)?;
        let limit = limit.unwrap_or(DEFAULT_LIMIT);
        if self.index.nodes.is_empty() {
            return Ok(Vec::new());
        }
        let mut file = fs::File::open(&self.index_path)
            .with_context(|| format!("failed to open {}", self.index_path.display()))?;
        let mut matches = Vec::new();
        let bounds_match = |bounds: Bounds3| bounds_intersect_frustum(bounds, &frustum);
        let point_match = |point: Vec3| point_in_frustum(point, &frustum);
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

    fn dataset(&self, name: &str) -> Result<Option<Arc<QueryDataset>>> {
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

fn parse_required<T: std::str::FromStr>(query: &str, name: &str) -> Result<T, (StatusCode, String)> {
    let value = parse_query_parameter(query, name)?
        .ok_or_else(|| bad_request(format!("missing query parameter {name}")))?;
    value
        .parse()
        .map_err(|_| bad_request(format!("query parameter {name} must be a number")))
}

fn parse_optional<T: std::str::FromStr>(query: &str, name: &str) -> Result<Option<T>, (StatusCode, String)> {
    let Some(value) = parse_query_parameter(query, name)? else {
        return Ok(None);
    };
    value
        .parse()
        .map(Some)
        .map_err(|_| bad_request(format!("query parameter {name} must be a number")))
}
