use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use axum::extract::{RawQuery, State};
use axum::http::header::{CONTENT_TYPE, HeaderValue};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use csv::Writer;

use crate::formats::{
    OCTREE_INDEX_FILENAME, OctreeIndex, ServingRow, decode_octree_index, decode_serving_rows,
    leaf_filename, serving_directory,
};
use crate::octree::{OctreeConfig, morton_encode};
use crate::storage::{StorageClient, StorageRoot};

#[derive(Clone)]
pub struct QueryService {
    index: OctreeIndex,
    occupied_leaves: HashSet<u32>,
    serving_root: StorageRoot,
    storage: StorageClient,
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

fn intersecting_leaves(
    service: &QueryService,
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
                if !service.occupied_leaves.contains(&morton) {
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

async fn query_radius(
    State(service): State<Arc<QueryService>>,
    RawQuery(query): RawQuery,
) -> Result<Response, (StatusCode, String)> {
    let request = parse_query_request(query.as_deref())?;
    let csv = tokio::task::spawn_blocking(move || query_radius_csv(&service, request))
        .await
        .map_err(|error| internal_error(error.into()))??;
    Ok((
        [(CONTENT_TYPE, HeaderValue::from_static("text/csv; charset=utf-8"))],
        csv,
    )
        .into_response())
}

impl QueryService {
    pub fn load(root: &str) -> Result<Self> {
        let data_root = StorageRoot::parse(root)?;
        let storage = StorageClient::new()?;
        let index: OctreeIndex =
            decode_octree_index(&storage.read_bytes(&data_root.join(OCTREE_INDEX_FILENAME))?)
                .context("failed to parse octree index")?;
        storage.validate_serving_layout(&data_root, &index)?;
        let serving_root = data_root.join(&serving_directory(index.depth));
        Ok(Self {
            occupied_leaves: index.leaves.iter().copied().collect(),
            index,
            serving_root,
            storage,
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
                &self
                    .storage
                    .read_bytes(&self.serving_root.join(&leaf_filename(*morton)))?,
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

pub fn build_app(service: Arc<QueryService>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/query/radius", get(query_radius))
        .with_state(service)
}

fn parse_query_parameter(query: &str, name: &str) -> Result<Option<String>, (StatusCode, String)> {
    let mut value = None;
    for part in query.split('&') {
        let (raw_key, raw_value) = part.split_once('=').unwrap_or((part, ""));
        let key = urlencoding::decode(raw_key).map_err(|_| bad_request("query string is not valid percent-encoding"))?;
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
    service: &QueryService,
    request: RadiusQueryRequest,
) -> Result<String, (StatusCode, String)> {
    validate_request(&request)?;
    let matches = service.query_radius(request).map_err(internal_error)?;
    encode_matches_csv(&matches)
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::Path;

    use axum::body::{self, Body};
    use axum::http::Request;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use crate::build_index::{BuildIndexConfig, DEFAULT_BOUNDS, DEFAULT_DEPTH, run_build_index};
    use crate::ingest::{IngestConfig, run_ingestion};

    use super::*;

    fn write_gzip_file(path: &Path, body: &str) {
        let file = std::fs::File::create(path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(body.as_bytes()).unwrap();
        encoder.finish().unwrap();
    }

    #[test]
    fn serves_exact_radius_queries_over_written_shards() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
        let output_path = dir.path().join("run");

        write_gzip_file(
            &input_path,
            "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             1,0,0,100,1,12.5,0.3\n\
             2,90,0,100,1,13.5,0.6\n\
             3,180,0,100,1,14.5,0.9\n",
        );
        run_ingestion(IngestConfig {
            inputs: vec![input_path.display().to_string()],
            output_root: output_path.display().to_string(),
            parallax_filter_mas: None,
        })
        .unwrap();
        run_build_index(BuildIndexConfig {
            data_root: output_path.display().to_string(),
            octree_depth: DEFAULT_DEPTH,
            bounds: DEFAULT_BOUNDS,
        })
        .unwrap();

        let service = Arc::new(QueryService::load(&output_path.display().to_string()).unwrap());
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let request = Request::builder()
                .method("GET")
                .uri("/query/radius?x=0.0&y=0.0&z=0.0&r=11.0&limit=10")
                .body(Body::empty())
                .unwrap();

            let response = {
                let app = build_app(service.clone());
                app.oneshot(request).await.unwrap()
            };
            assert_eq!(response.status(), StatusCode::OK);

            let bytes = body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let mut rows = csv::Reader::from_reader(bytes.as_ref());
            let headers = rows.headers().unwrap().clone();
            let records = rows.records().collect::<Result<Vec<_>, _>>().unwrap();

            assert_eq!(headers, csv::StringRecord::from(vec!["x", "y", "z", "source_id"]));
            assert_eq!(records.len(), 3);
            assert_eq!(records[0].get(3), Some("1"));
            assert_eq!(records[1].get(3), Some("2"));
            assert_eq!(records[2].get(3), Some("3"));
        });
        drop(service);
    }

    #[test]
    fn returns_stable_results_for_repeated_ingestion_runs() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
        let output_a = dir.path().join("run-a");
        let output_b = dir.path().join("run-b");

        write_gzip_file(
            &input_path,
            "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
             7,0,0,100,1,12.0,0.2\n\
             3,90,0,50,1,13.0,0.4\n\
             9,180,0,25,1,14.0,0.6\n\
             2,0,90,40,1,15.0,0.8\n",
        );

        for output_root in [&output_a, &output_b] {
            run_ingestion(IngestConfig {
                inputs: vec![input_path.display().to_string()],
                output_root: output_root.display().to_string(),
                parallax_filter_mas: None,
            })
            .unwrap();
            run_build_index(BuildIndexConfig {
                data_root: output_root.display().to_string(),
                octree_depth: DEFAULT_DEPTH,
                bounds: DEFAULT_BOUNDS,
            })
            .unwrap();
        }

        let service_a = QueryService::load(&output_a.display().to_string()).unwrap();
        let service_b = QueryService::load(&output_b.display().to_string()).unwrap();
        let request = RadiusQueryRequest {
            x: 0.0,
            y: 0.0,
            z: 0.0,
            radius: 50.0,
            limit: Some(10),
        };

        let response_a = service_a.query_radius(request).unwrap();
        let response_b = service_b
            .query_radius(RadiusQueryRequest {
                x: 0.0,
                y: 0.0,
                z: 0.0,
                radius: 50.0,
                limit: Some(10),
            })
            .unwrap();

        assert_eq!(response_a, response_b);
    }
}
