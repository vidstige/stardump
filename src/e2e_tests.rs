use std::f32::consts::{FRAC_1_SQRT_2, FRAC_PI_2};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use axum::body::{self, Body};
use axum::http::{Request, StatusCode};
use flate2::Compression;
use flate2::write::GzEncoder;
use tempfile::tempdir;
use tower::ServiceExt;

use crate::build_index::{BuildIndexConfig, DEFAULT_BOUNDS, DEFAULT_DEPTH, run_build_index};
use crate::formats::{OCTREE_INDEX_FILENAME, read_packed_octree};
use crate::ingest::{IngestConfig, run_ingestion};
use crate::query_api::{
    FrustumQueryRequest, QueryCatalog, QueryDataset, RadiusQueryRequest, build_app,
};

fn write_gzip_file(path: &Path, body: &str) {
    let file = std::fs::File::create(path).unwrap();
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(body.as_bytes()).unwrap();
    encoder.finish().unwrap();
}

fn ingest_config(output_root: &Path, inputs: Vec<String>) -> IngestConfig {
    IngestConfig::new(output_root.display().to_string()).with_inputs(inputs)
}

#[test]
fn builds_packed_index_from_multiple_canonical_sources() {
    let dir = tempdir().unwrap();
    let input_a = dir.path().join("GaiaSource_000000-000001.csv.gz");
    let input_b = dir.path().join("GaiaSource_000002-000003.csv.gz");
    let output_root = dir.path().join("run");

    write_gzip_file(
        &input_a,
        "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
         1,0,0,100,1,12.5,0.3\n\
         2,90,0,100,1,13.5,0.6\n",
    );
    write_gzip_file(
        &input_b,
        "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
         3,180,0,100,1,14.5,0.9\n\
         4,0,90,100,1,15.5,1.2\n",
    );

    run_ingestion(ingest_config(
        &output_root,
        vec![input_a.display().to_string(), input_b.display().to_string()],
    ))
    .unwrap();

    let result = run_build_index(BuildIndexConfig {
        data_root: output_root.display().to_string(),
        output_root: output_root.display().to_string(),
        octree_depth: DEFAULT_DEPTH,
        bounds: DEFAULT_BOUNDS,
    })
    .unwrap();
    let index = read_packed_octree(&output_root.join(OCTREE_INDEX_FILENAME)).unwrap();

    assert_eq!(result.index, index);
    assert_eq!(result.sources_seen, 2);
    assert_eq!(result.rows_in_bounds, 4);
    assert_eq!(index.point_count, 4);
    assert!(!index.nodes.is_empty());
}

#[test]
fn serves_exact_radius_queries_over_packed_index() {
    let dir = tempdir().unwrap();
    let data_root = dir.path().join("datasets");
    let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
    let output_path = data_root.join("run");

    write_gzip_file(
        &input_path,
        "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
         1,0,0,100,1,12.5,0.3\n\
         2,90,0,100,1,13.5,0.6\n\
         3,180,0,100,1,14.5,0.9\n",
    );
    run_ingestion(ingest_config(
        &output_path,
        vec![input_path.display().to_string()],
    ))
    .unwrap();
    run_build_index(BuildIndexConfig {
        data_root: output_path.display().to_string(),
        output_root: output_path.display().to_string(),
        octree_depth: DEFAULT_DEPTH,
        bounds: DEFAULT_BOUNDS,
    })
    .unwrap();

    let catalog = Arc::new(QueryCatalog::load(&data_root.display().to_string()).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let request = Request::builder()
            .method("GET")
            .uri("/query/run/radius?x=0.0&y=0.0&z=0.0&r=11.0&limit=10")
            .body(Body::empty())
            .unwrap();

        let response = {
            let app = build_app(catalog.clone());
            app.oneshot(request).await.unwrap()
        };
        assert_eq!(response.status(), StatusCode::OK);

        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let mut rows = csv::Reader::from_reader(bytes.as_ref());
        let headers = rows.headers().unwrap().clone();
        let records = rows.records().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(
            headers,
            csv::StringRecord::from(vec!["x", "y", "z", "source_id"])
        );
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].get(3), Some("1"));
        assert_eq!(records[1].get(3), Some("2"));
        assert_eq!(records[2].get(3), Some("3"));
    });
}

#[test]
fn serves_frustum_queries_with_a_limit() {
    let dir = tempdir().unwrap();
    let data_root = dir.path().join("datasets");
    let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
    let output_path = data_root.join("run");

    write_gzip_file(
        &input_path,
        "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
         1,0,0,100,1,12.5,0.3\n\
         2,90,0,100,1,13.5,0.6\n\
         3,180,0,100,1,14.5,0.9\n",
    );
    run_ingestion(ingest_config(
        &output_path,
        vec![input_path.display().to_string()],
    ))
    .unwrap();
    run_build_index(BuildIndexConfig {
        data_root: output_path.display().to_string(),
        output_root: output_path.display().to_string(),
        octree_depth: DEFAULT_DEPTH,
        bounds: DEFAULT_BOUNDS,
    })
    .unwrap();

    let catalog = Arc::new(QueryCatalog::load(&data_root.display().to_string()).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let request = Request::builder()
            .method("GET")
            .uri(format!(
                "/query/run/frustum?x={}&y={}&z={}&qx={}&qy={}&qz={}&qw={}&near={}&far={}&fovy={}&aspect={}&limit=2",
                -20.0,
                0.0,
                0.0,
                0.0,
                -FRAC_1_SQRT_2,
                0.0,
                FRAC_1_SQRT_2,
                1.0,
                40.0,
                FRAC_PI_2,
                1.0
            ))
            .body(Body::empty())
            .unwrap();

        let response = {
            let app = build_app(catalog.clone());
            app.oneshot(request).await.unwrap()
        };
        assert_eq!(response.status(), StatusCode::OK);

        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let mut rows = csv::Reader::from_reader(bytes.as_ref());
        let records = rows.records().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].get(3), Some("1"));
        assert_eq!(records[1].get(3), Some("2"));
    });
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
        run_ingestion(ingest_config(
            output_root,
            vec![input_path.display().to_string()],
        ))
        .unwrap();
        run_build_index(BuildIndexConfig {
            data_root: output_root.display().to_string(),
            output_root: output_root.display().to_string(),
            octree_depth: DEFAULT_DEPTH,
            bounds: DEFAULT_BOUNDS,
        })
        .unwrap();
    }

    let dataset_a = QueryDataset::load(&output_a).unwrap();
    let dataset_b = QueryDataset::load(&output_b).unwrap();
    let request = RadiusQueryRequest {
        x: 0.0,
        y: 0.0,
        z: 0.0,
        radius: 50.0,
        limit: Some(10),
    };

    let response_a = dataset_a.query_radius(request).unwrap();
    let response_b = dataset_b
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

#[test]
fn frustum_queries_match_for_repeated_ingestion_runs() {
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
    let output_a = dir.path().join("run-a");
    let output_b = dir.path().join("run-b");

    write_gzip_file(
        &input_path,
        "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
         7,0,0,100,1,12.0,0.2\n\
         3,90,0,100,1,13.0,0.4\n\
         9,180,0,100,1,14.0,0.6\n",
    );

    for output_root in [&output_a, &output_b] {
        run_ingestion(ingest_config(
            output_root,
            vec![input_path.display().to_string()],
        ))
        .unwrap();
        run_build_index(BuildIndexConfig {
            data_root: output_root.display().to_string(),
            output_root: output_root.display().to_string(),
            octree_depth: DEFAULT_DEPTH,
            bounds: DEFAULT_BOUNDS,
        })
        .unwrap();
    }

    let dataset_a = QueryDataset::load(&output_a).unwrap();
    let dataset_b = QueryDataset::load(&output_b).unwrap();
    let request = FrustumQueryRequest {
        x: -20.0,
        y: 0.0,
        z: 0.0,
        qx: 0.0,
        qy: -FRAC_1_SQRT_2,
        qz: 0.0,
        qw: FRAC_1_SQRT_2,
        near: 1.0,
        far: 40.0,
        fovy: FRAC_PI_2,
        aspect: 1.0,
        limit: Some(10),
    };

    let response_a = dataset_a.query_frustum(request).unwrap();
    let response_b = dataset_b
        .query_frustum(FrustumQueryRequest {
            x: -20.0,
            y: 0.0,
            z: 0.0,
            qx: 0.0,
            qy: -FRAC_1_SQRT_2,
            qz: 0.0,
            qw: FRAC_1_SQRT_2,
            near: 1.0,
            far: 40.0,
            fovy: FRAC_PI_2,
            aspect: 1.0,
            limit: Some(10),
        })
        .unwrap();

    assert_eq!(response_a, response_b);
}

#[test]
fn lists_available_indices() {
    let dir = tempdir().unwrap();
    let data_root = dir.path().join("datasets");
    let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");

    write_gzip_file(
        &input_path,
        "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
         1,0,0,100,1,12.5,0.3\n",
    );

    for name in ["alpha", "beta"] {
        let output_root = data_root.join(name);
        run_ingestion(ingest_config(
            &output_root,
            vec![input_path.display().to_string()],
        ))
        .unwrap();
        run_build_index(BuildIndexConfig {
            data_root: output_root.display().to_string(),
            output_root: output_root.display().to_string(),
            octree_depth: DEFAULT_DEPTH,
            bounds: DEFAULT_BOUNDS,
        })
        .unwrap();
    }

    let catalog = Arc::new(QueryCatalog::load(&data_root.display().to_string()).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let response = {
            let app = build_app(catalog);
            app.oneshot(
                Request::builder()
                    .uri("/indices")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
        };
        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "alpha\nbeta\n");
    });
}
