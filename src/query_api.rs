use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Result, bail};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::formats::{
    OctreeIndex, RunMetadata, ServingRow, leaf_filename, read_json, read_serving_rows,
    validate_run_layout,
};
use crate::octree::OctreeConfig;

#[derive(Clone)]
pub struct QueryService {
    metadata: RunMetadata,
    index: OctreeIndex,
    data_root: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct RadiusQueryRequest {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub radius: f32,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RadiusQueryMatch {
    pub source_id: u64,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub distance: f32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RadiusQueryResponse {
    pub matches: Vec<RadiusQueryMatch>,
    pub examined_leaves: usize,
    pub returned_matches: usize,
    pub truncated: bool,
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

async fn healthz() -> StatusCode {
    StatusCode::OK
}

async fn query_radius(
    State(service): State<Arc<QueryService>>,
    Json(request): Json<RadiusQueryRequest>,
) -> Result<Json<RadiusQueryResponse>, (StatusCode, String)> {
    let response = query_radius_checked(&service, request).map(Json)?;
    Ok(response)
}

impl QueryService {
    pub fn load(root: impl AsRef<Path>) -> Result<Self> {
        let data_root = root.as_ref().to_path_buf();
        let metadata = read_json::<RunMetadata>(&data_root.join("metadata.json"))?;
        let index = read_json::<OctreeIndex>(&data_root.join("index.octree"))?;
        validate_run_layout(&data_root, &metadata, &index)?;
        Ok(Self {
            metadata,
            index,
            data_root,
        })
    }

    pub fn metadata(&self) -> &RunMetadata {
        &self.metadata
    }

    pub fn query_radius(&self, request: RadiusQueryRequest) -> Result<RadiusQueryResponse> {
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
        let serving_root = self.data_root.join(&self.metadata.serving_directory);
        let mut matches = Vec::new();
        let mut examined_leaves = 0;

        for morton in &self.index.leaves {
            let leaf_bounds = octree.leaf_bounds(*morton);
            if !leaf_bounds.intersects_sphere(center, request.radius) {
                continue;
            }

            examined_leaves += 1;
            let rows = read_serving_rows(&serving_root.join(leaf_filename(*morton)))?;
            for row in rows {
                let distance = distance(center, &row);
                if distance <= request.radius {
                    matches.push(RadiusQueryMatch {
                        source_id: row.source_id,
                        x: row.x,
                        y: row.y,
                        z: row.z,
                        distance,
                    });
                }
            }
        }

        matches.sort_by(|left, right| {
            left.source_id
                .cmp(&right.source_id)
                .then_with(|| left.distance.total_cmp(&right.distance))
        });
        let truncated = matches.len() > limit;
        matches.truncate(limit);

        Ok(RadiusQueryResponse {
            returned_matches: matches.len(),
            matches,
            examined_leaves,
            truncated,
        })
    }
}

pub fn build_app(service: Arc<QueryService>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/query/radius", post(query_radius))
        .with_state(service)
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

pub fn query_radius_checked(
    service: &QueryService,
    request: RadiusQueryRequest,
) -> Result<RadiusQueryResponse, (StatusCode, String)> {
    validate_request(&request)?;
    service.query_radius(request).map_err(internal_error)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use axum::body::{self, Body};
    use axum::http::Request;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use crate::ingest::{DEFAULT_BOUNDS, DEFAULT_DEPTH, IngestConfig, run_ingestion};

    use super::*;

    fn write_gzip_file(path: &Path, body: &str) {
        let file = std::fs::File::create(path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(body.as_bytes()).unwrap();
        encoder.finish().unwrap();
    }

    #[tokio::test]
    async fn serves_exact_radius_queries_over_written_shards() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
        let output_path = dir.path().join("run");

        write_gzip_file(
            &input_path,
            "source_id,ra,dec,parallax\n\
             1,0,0,100\n\
             2,90,0,100\n\
             3,180,0,100\n",
        );
        run_ingestion(IngestConfig {
            input: input_path.display().to_string(),
            output_root: output_path.clone(),
            parallax_filter_mas: None,
            octree_depth: DEFAULT_DEPTH,
            bounds: DEFAULT_BOUNDS,
        })
        .unwrap();

        let service = Arc::new(QueryService::load(&output_path).unwrap());
        let app = build_app(service);
        let request = Request::builder()
            .method("POST")
            .uri("/query/radius")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"x":0.0,"y":0.0,"z":0.0,"radius":11.0,"limit":10}"#,
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: RadiusQueryResponse = serde_json::from_slice(&bytes).unwrap();
        let source_ids: Vec<u64> = payload.matches.iter().map(|item| item.source_id).collect();

        assert_eq!(source_ids, vec![1, 2, 3]);
        assert_eq!(payload.returned_matches, 3);
        assert!(!payload.truncated);
    }

    #[test]
    fn returns_stable_results_for_repeated_ingestion_runs() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
        let output_a = dir.path().join("run-a");
        let output_b = dir.path().join("run-b");

        write_gzip_file(
            &input_path,
            "source_id,ra,dec,parallax\n\
             7,0,0,100\n\
             3,90,0,50\n\
             9,180,0,25\n\
             2,0,90,40\n",
        );

        for output_root in [&output_a, &output_b] {
            run_ingestion(IngestConfig {
                input: input_path.display().to_string(),
                output_root: output_root.to_path_buf(),
                parallax_filter_mas: None,
                octree_depth: DEFAULT_DEPTH,
                bounds: DEFAULT_BOUNDS,
            })
            .unwrap();
        }

        let service_a = QueryService::load(&output_a).unwrap();
        let service_b = QueryService::load(&output_b).unwrap();
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

        assert_eq!(response_a.returned_matches, response_b.returned_matches);
        assert_eq!(response_a.examined_leaves, response_b.examined_leaves);
        assert_eq!(response_a.truncated, response_b.truncated);
        assert_eq!(
            response_a
                .matches
                .iter()
                .map(|item| item.source_id)
                .collect::<Vec<_>>(),
            response_b
                .matches
                .iter()
                .map(|item| item.source_id)
                .collect::<Vec<_>>()
        );
    }
}
