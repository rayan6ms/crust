use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use crust_server::CrustServer;
use crust_server::config::ServerConfig;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

async fn body(response: axum::response::Response<Body>) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), 256 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

fn request(path: &str) -> Request<Body> {
    Request::get(path).body(Body::empty()).unwrap()
}

fn authorized(path: &str) -> Request<Body> {
    Request::get(path)
        .header("authorization", "secret")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn health_probes_are_public_and_readiness_flips_during_shutdown() {
    let mut config = ServerConfig::default().with_password("secret").unwrap();
    config.port = 0;
    config.metrics.prometheus.enabled = true;
    let server = CrustServer::bind(config).await.unwrap();
    let app = server.router();
    assert_eq!(
        app.clone()
            .oneshot(request("/health/live"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone()
            .oneshot(request("/health/ready"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.clone();
    let task = tokio::spawn(async move { server.serve(task_shutdown).await });
    shutdown.cancel();
    task.await.unwrap().unwrap();
    // The app retains the same state as the served server and exposes the
    // terminal readiness state to an in-process probe.
    assert_eq!(
        app.oneshot(request("/health/ready"))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn prometheus_endpoint_is_public_bounded_and_low_cardinality() {
    let mut config = ServerConfig::default().with_password("secret").unwrap();
    config.port = 0;
    config.metrics.prometheus.enabled = true;
    config.metrics.endpoint = "/internal/metrics".to_owned();
    let app: Router = CrustServer::bind(config).await.unwrap().router();
    let response = app
        .clone()
        .oneshot(request("/internal/metrics"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/plain; version=0.0.4")
    );
    let metrics = body(response).await;
    for name in [
        "crust_sessions_active",
        "crust_players_active",
        "crust_loads_in_flight",
        "crust_rest_requests_total",
        "crust_frames_sent_total",
        "crust_dave_failures_total",
        "crust_routeplanner_failures",
        "crust_process_rss_bytes",
        "crust_load_shed_total",
    ] {
        assert!(metrics.contains(name), "missing metric {name}");
    }
    assert!(!metrics.contains("secret"));
    assert!(!metrics.contains("guild"));
    assert_eq!(
        app.oneshot(request("/internal/metrics?token=secret"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn disabled_prometheus_endpoint_is_not_registered() {
    let mut config = ServerConfig::default().with_password("secret").unwrap();
    config.port = 0;
    let app = CrustServer::bind(config).await.unwrap().router();
    assert_ne!(
        app.oneshot(request("/metrics")).await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn info_reports_only_enabled_admitted_sources_and_filters() {
    let mut config = ServerConfig::default().with_password("secret").unwrap();
    config.port = 0;
    config.sources.youtube = false;
    config.filters.timescale = false;
    config.filters.low_pass = false;
    let app = CrustServer::bind(config).await.unwrap().router();
    let response = app.oneshot(authorized("/v4/info")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = serde_json::from_str(&body(response).await).unwrap();
    assert!(
        !value["sourceManagers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "youtube")
    );
    assert!(
        !value["filters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "timescale")
    );
    assert!(
        !value["filters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "lowPass")
    );
}
