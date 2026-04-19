use std::f32::consts::{FRAC_1_SQRT_2, FRAC_PI_2, PI};
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
use crate::query_api::{QueryCatalog, build_app};

fn write_gzip_file(path: &Path, body: &str) {
    let file = std::fs::File::create(path).unwrap();
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(body.as_bytes()).unwrap();
    encoder.finish().unwrap();
}

fn ingest_config(output_root: &Path, inputs: Vec<String>) -> IngestConfig {
    IngestConfig::new(output_root.display().to_string()).with_inputs(inputs)
}

#[tokio::test]
async fn builds_packed_index_from_multiple_canonical_sources() {
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
    .await
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

#[tokio::test]
async fn serves_exact_radius_queries_over_packed_index() {
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
    .await
    .unwrap();
    run_build_index(BuildIndexConfig {
        data_root: output_path.display().to_string(),
        output_root: output_path.display().to_string(),
        octree_depth: DEFAULT_DEPTH,
        bounds: DEFAULT_BOUNDS,
    })
    .unwrap();

    let catalog = Arc::new(QueryCatalog::load(&data_root.display().to_string()).unwrap());
    let request = Request::builder()
        .method("GET")
        .uri("/query/run/radius?x=0.0&y=0.0&z=0.0&r=11.0&limit=10")
        .body(Body::empty())
        .unwrap();

    let response = build_app(catalog.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let mut rows = csv::Reader::from_reader(bytes.as_ref());
    let headers = rows.headers().unwrap().clone();
    let records = rows.records().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(
        headers,
        csv::StringRecord::from(vec!["x", "y", "z", "source_id", "luminosity", "bp_rp"])
    );
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].get(3), Some("1"));
    assert_eq!(records[1].get(3), Some("2"));
    assert_eq!(records[2].get(3), Some("3"));
}

#[tokio::test]
async fn serves_frustum_queries_with_a_limit() {
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
    .await
    .unwrap();
    run_build_index(BuildIndexConfig {
        data_root: output_path.display().to_string(),
        output_root: output_path.display().to_string(),
        octree_depth: DEFAULT_DEPTH,
        bounds: DEFAULT_BOUNDS,
    })
    .unwrap();

    let catalog = Arc::new(QueryCatalog::load(&data_root.display().to_string()).unwrap());
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

    let response = build_app(catalog.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let mut rows = csv::Reader::from_reader(bytes.as_ref());
    let records = rows.records().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].get(3), Some("3"));
    assert_eq!(records[1].get(3), Some("2"));
}

#[tokio::test]
async fn returns_stable_results_for_repeated_ingestion_runs() {
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
    let datasets_a = dir.path().join("datasets-a");
    let datasets_b = dir.path().join("datasets-b");

    write_gzip_file(
        &input_path,
        "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
         7,0,0,100,1,12.0,0.2\n\
         3,90,0,50,1,13.0,0.4\n\
         9,180,0,25,1,14.0,0.6\n\
         2,0,90,40,1,15.0,0.8\n",
    );

    for datasets_root in [&datasets_a, &datasets_b] {
        let output_root = datasets_root.join("run");
        run_ingestion(ingest_config(
            &output_root,
            vec![input_path.display().to_string()],
        ))
        .await
        .unwrap();
        run_build_index(BuildIndexConfig {
            data_root: output_root.display().to_string(),
            output_root: output_root.display().to_string(),
            octree_depth: DEFAULT_DEPTH,
            bounds: DEFAULT_BOUNDS,
        })
        .unwrap();
    }

    let catalog_a = Arc::new(QueryCatalog::load(&datasets_a.display().to_string()).unwrap());
    let catalog_b = Arc::new(QueryCatalog::load(&datasets_b.display().to_string()).unwrap());
    let uri = "/query/run/radius?x=0.0&y=0.0&z=0.0&r=50.0&limit=10";

    let body_a = body::to_bytes(
        build_app(catalog_a)
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let body_b = body::to_bytes(
        build_app(catalog_b)
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();

    assert_eq!(body_a, body_b);
}

#[tokio::test]
async fn frustum_queries_match_for_repeated_ingestion_runs() {
    let dir = tempdir().unwrap();
    let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
    let datasets_a = dir.path().join("datasets-a");
    let datasets_b = dir.path().join("datasets-b");

    write_gzip_file(
        &input_path,
        "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
         7,0,0,100,1,12.0,0.2\n\
         3,90,0,100,1,13.0,0.4\n\
         9,180,0,100,1,14.0,0.6\n",
    );

    for datasets_root in [&datasets_a, &datasets_b] {
        let output_root = datasets_root.join("run");
        run_ingestion(ingest_config(
            &output_root,
            vec![input_path.display().to_string()],
        ))
        .await
        .unwrap();
        run_build_index(BuildIndexConfig {
            data_root: output_root.display().to_string(),
            output_root: output_root.display().to_string(),
            octree_depth: DEFAULT_DEPTH,
            bounds: DEFAULT_BOUNDS,
        })
        .unwrap();
    }

    let catalog_a = Arc::new(QueryCatalog::load(&datasets_a.display().to_string()).unwrap());
    let catalog_b = Arc::new(QueryCatalog::load(&datasets_b.display().to_string()).unwrap());
    let uri = format!(
        "/query/run/frustum?x={}&y={}&z={}&qx={}&qy={}&qz={}&qw={}&near={}&far={}&fovy={}&aspect={}&limit=10",
        -20.0, 0.0, 0.0, 0.0, -FRAC_1_SQRT_2, 0.0, FRAC_1_SQRT_2, 1.0, 40.0, FRAC_PI_2, 1.0
    );

    let body_a = body::to_bytes(
        build_app(catalog_a)
            .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let body_b = body::to_bytes(
        build_app(catalog_b)
            .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();

    assert_eq!(body_a, body_b);
}

#[tokio::test]
async fn lists_available_indices() {
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
        .await
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
    let response = build_app(catalog)
        .oneshot(
            Request::builder()
                .uri("/indices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(std::str::from_utf8(&body).unwrap(), "alpha\nbeta\n");
}

#[tokio::test]
async fn lod_frustum_returns_binary_units() {
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
    run_ingestion(ingest_config(&output_path, vec![input_path.display().to_string()]))
        .await
        .unwrap();
    run_build_index(BuildIndexConfig {
        data_root: output_path.display().to_string(),
        output_root: output_path.display().to_string(),
        octree_depth: DEFAULT_DEPTH,
        bounds: DEFAULT_BOUNDS,
    })
    .unwrap();

    // Camera at (-20,0,0) looking +x (90° rotation around Y)
    let catalog = Arc::new(QueryCatalog::load(&data_root.display().to_string()).unwrap());
    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/query/run/lod-frustum?x={}&y=0&z=0&qx=0&qy={}&qz=0&qw={}&near=1&far=100&fovy={}&aspect=1&width=1920&height=1080&limit=100",
            -20.0, -FRAC_1_SQRT_2, FRAC_1_SQRT_2, FRAC_PI_2
        ))
        .body(Body::empty())
        .unwrap();

    let response = build_app(catalog).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(bytes.len() >= 4);
    let unit_count = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    assert!(unit_count > 0);
    assert_eq!(bytes.len() as u32, 4 + unit_count * 20);

    // All units must have positive luminosity
    for i in 0..unit_count as usize {
        let lum = f32::from_le_bytes(bytes[4 + i * 20 + 12..4 + i * 20 + 16].try_into().unwrap());
        assert!(lum > 0.0, "unit {i} has non-positive luminosity: {lum}");
    }
}

#[tokio::test]
async fn lod_frustum_limit_is_respected() {
    let dir = tempdir().unwrap();
    let data_root = dir.path().join("datasets");
    let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
    let output_path = data_root.join("run");

    write_gzip_file(
        &input_path,
        "source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n\
         1,0,0,100,1,12.5,0.3\n\
         2,90,0,100,1,13.5,0.6\n\
         3,180,0,100,1,14.5,0.9\n\
         4,0,90,100,1,15.5,1.2\n",
    );
    run_ingestion(ingest_config(&output_path, vec![input_path.display().to_string()]))
        .await
        .unwrap();
    run_build_index(BuildIndexConfig {
        data_root: output_path.display().to_string(),
        output_root: output_path.display().to_string(),
        octree_depth: DEFAULT_DEPTH,
        bounds: DEFAULT_BOUNDS,
    })
    .unwrap();

    let catalog = Arc::new(QueryCatalog::load(&data_root.display().to_string()).unwrap());
    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/query/run/lod-frustum?x=0&y=0&z=0&qx=0&qy=0&qz=0&qw=1&near=0.01&far=100&fovy={}&aspect=1&width=1920&height=1080&limit=1",
            PI / 3.0
        ))
        .body(Body::empty())
        .unwrap();

    let response = build_app(catalog).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let unit_count = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    assert!(unit_count <= 1);
}

#[tokio::test]
async fn lod_frustum_covers_wide_frustum_without_clustering() {
    // The old recursion-with-limit algorithm saturated its budget on the first
    // octants it visited, leaving the rest of the frustum empty. Build a dataset
    // scattered across many azimuths and verify the response covers both halves
    // of screen-space.
    let dir = tempdir().unwrap();
    let data_root = dir.path().join("datasets");
    let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
    let output_path = data_root.join("run");

    let mut csv = String::from("source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n");
    // 60 stars around the sky at moderate distance (parallax=10mas → 100pc).
    for i in 0..60_u32 {
        let ra = (i as f32) * 6.0; // 0..354 deg
        let dec = ((i % 5) as f32 - 2.0) * 15.0; // spread ±30 deg
        csv.push_str(&format!("{},{},{},10,1,13.0,0.5\n", i + 1, ra, dec));
    }
    write_gzip_file(&input_path, &csv);
    run_ingestion(ingest_config(&output_path, vec![input_path.display().to_string()]))
        .await
        .unwrap();
    run_build_index(BuildIndexConfig {
        data_root: output_path.display().to_string(),
        output_root: output_path.display().to_string(),
        octree_depth: DEFAULT_DEPTH,
        bounds: DEFAULT_BOUNDS,
    })
    .unwrap();

    // Camera at origin, looking +x, wide fov. Should see roughly half the sky.
    // Quaternion for 90° rotation around +y: (0, sin45, 0, cos45).
    let catalog = Arc::new(QueryCatalog::load(&data_root.display().to_string()).unwrap());
    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/query/run/lod-frustum?x=0&y=0&z=0&qx=0&qy={}&qz=0&qw={}&near=0.01&far=10000&fovy={}&aspect=1.777&width=1920&height=1080&limit=200",
            FRAC_1_SQRT_2, FRAC_1_SQRT_2, PI / 2.0
        ))
        .body(Body::empty())
        .unwrap();

    let response = build_app(catalog).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let unit_count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    assert!(unit_count > 8, "expected many units, got {unit_count}");
    assert!(unit_count <= 200);

    // Camera forward is +x, right is +z (for qy=sin45,qw=cos45 rotation).
    // "Horizontal" screen axis ≈ z. Count how many units fall on each side.
    let mut pos_z = 0;
    let mut neg_z = 0;
    for i in 0..unit_count {
        let off = 4 + i * 20;
        let z = f32::from_le_bytes(bytes[off + 8..off + 12].try_into().unwrap());
        if z > 0.0 {
            pos_z += 1;
        } else if z < 0.0 {
            neg_z += 1;
        }
    }
    assert!(pos_z > 0 && neg_z > 0, "expected units on both sides of screen; got +z={pos_z}, -z={neg_z}");
}

#[tokio::test]
async fn lod_frustum_near_view_reproduces_production_case() {
    // Reproduce the production "far=50 returns 0 units" regression: dense dataset,
    // tight near-frustum. The LOD endpoint must return at least some units here.
    let dir = tempdir().unwrap();
    let data_root = dir.path().join("datasets");
    let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
    let output_path = data_root.join("run");

    let mut csv = String::from("source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n");
    // 500 stars skewed toward the -Z hemisphere so the identity-quaternion frustum
    // (looking -Z) has plenty of in-cone stars. parallax 50 mas → 20pc.
    for i in 0..500_u32 {
        let ra = (i as f32) * 0.71;
        // dec skewed toward -90 so z = sin(dec) < 0.
        let dec = -30.0 - ((i as f32) * 0.31).sin().abs() * 50.0;
        let parallax = 30.0 + ((i as f32) * 0.13).sin().abs() * 200.0;
        csv.push_str(&format!("{},{},{},{},1,14.0,0.5\n", i + 1, ra % 360.0, dec, parallax));
    }
    write_gzip_file(&input_path, &csv);
    run_ingestion(ingest_config(&output_path, vec![input_path.display().to_string()]))
        .await
        .unwrap();
    run_build_index(BuildIndexConfig {
        data_root: output_path.display().to_string(),
        output_root: output_path.display().to_string(),
        octree_depth: DEFAULT_DEPTH,
        bounds: DEFAULT_BOUNDS,
    })
    .unwrap();

    let catalog = Arc::new(QueryCatalog::load(&data_root.display().to_string()).unwrap());
    // Production-matching: identity quaternion (camera looks -Z).
    let lod_uri = "/query/run/lod-frustum?x=0&y=0&z=0&qx=0&qy=0&qz=0&qw=1&near=0.1&far=50&fovy=1.0&aspect=1.5&width=1920&height=1080&limit=10000";
    let request = Request::builder().method("GET").uri(lod_uri).body(Body::empty()).unwrap();
    let response = build_app(catalog.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let unit_count = u32::from_le_bytes(bytes[0..4].try_into().unwrap());

    // Baseline: non-LOD frustum endpoint
    let baseline_uri = "/query/run/frustum?x=0&y=0&z=0&qx=0&qy=0&qz=0&qw=1&near=0.1&far=50&fovy=1.0&aspect=1.5&limit=10000";
    let baseline_request = Request::builder().method("GET").uri(baseline_uri).body(Body::empty()).unwrap();
    let baseline = build_app(catalog).oneshot(baseline_request).await.unwrap();
    let baseline_body = body::to_bytes(baseline.into_body(), usize::MAX).await.unwrap();
    let baseline_text = std::str::from_utf8(&baseline_body).unwrap();
    let baseline_count = baseline_text.lines().count().saturating_sub(1); // minus header

    assert!(
        baseline_count > 0,
        "baseline /frustum returned 0 stars — dataset doesn't reach the near cone, test is invalid"
    );
    assert!(
        unit_count > 0,
        "LOD endpoint returned 0 units while baseline /frustum saw {baseline_count} stars in the same cone"
    );
}

#[tokio::test]
async fn lod_frustum_larger_limit_refines_further() {
    let dir = tempdir().unwrap();
    let data_root = dir.path().join("datasets");
    let input_path = dir.path().join("GaiaSource_786097-786431.csv.gz");
    let output_path = data_root.join("run");

    let mut csv = String::from("source_id,ra,dec,parallax,parallax_error,phot_g_mean_mag,bp_rp\n");
    for i in 0..40_u32 {
        let ra = (i as f32) * 9.0;
        let dec = ((i % 7) as f32 - 3.0) * 10.0;
        csv.push_str(&format!("{},{},{},10,1,13.0,0.5\n", i + 1, ra, dec));
    }
    write_gzip_file(&input_path, &csv);
    run_ingestion(ingest_config(&output_path, vec![input_path.display().to_string()]))
        .await
        .unwrap();
    run_build_index(BuildIndexConfig {
        data_root: output_path.display().to_string(),
        output_root: output_path.display().to_string(),
        octree_depth: DEFAULT_DEPTH,
        bounds: DEFAULT_BOUNDS,
    })
    .unwrap();

    let catalog = Arc::new(QueryCatalog::load(&data_root.display().to_string()).unwrap());
    let make_request = |limit: u32| {
        Request::builder()
            .method("GET")
            .uri(format!(
                "/query/run/lod-frustum?x=0&y=0&z=0&qx=0&qy={}&qz=0&qw={}&near=0.01&far=10000&fovy={}&aspect=1&width=1920&height=1080&limit={}",
                FRAC_1_SQRT_2, FRAC_1_SQRT_2,
                PI / 2.0,
                limit,
            ))
            .body(Body::empty())
            .unwrap()
    };

    let small = build_app(catalog.clone()).oneshot(make_request(5)).await.unwrap();
    let large = build_app(catalog).oneshot(make_request(200)).await.unwrap();
    let small_bytes = body::to_bytes(small.into_body(), usize::MAX).await.unwrap();
    let large_bytes = body::to_bytes(large.into_body(), usize::MAX).await.unwrap();
    let small_count = u32::from_le_bytes(small_bytes[0..4].try_into().unwrap());
    let large_count = u32::from_le_bytes(large_bytes[0..4].try_into().unwrap());
    assert!(small_count <= 5, "small limit exceeded: {small_count}");
    assert!(large_count <= 200, "large limit exceeded: {large_count}");
    assert!(
        large_count > small_count,
        "expected larger limit to return more units; small={small_count}, large={large_count}"
    );
}
