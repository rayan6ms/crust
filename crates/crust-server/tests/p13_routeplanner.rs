use std::net::IpAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use crust::media::MantleAdapter;
use crust::routeplanner::{RouteOutcome, RoutePlanner, RoutePlannerConfig, RoutePlannerStrategy};
use crust::voice::VoiceBackend;
use crust_server::CrustServer;
use crust_server::config::ServerConfig;
use crust_testkit::{FakeMantle, FakeVoiceBackend};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn app(planner: RoutePlanner) -> Router {
    let mut config = ServerConfig::default()
        .with_password("test-password")
        .unwrap();
    config.port = 0;
    let mantle: Arc<dyn MantleAdapter> = Arc::new(FakeMantle::default());
    let voice: Arc<dyn VoiceBackend> = Arc::new(FakeVoiceBackend::new(8, 1));
    CrustServer::bind_with_backends_and_route_planner(config, mantle, voice, planner)
        .await
        .unwrap()
        .router()
}

fn request(method: &str, path: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", "test-password")
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

async fn response(app: &Router, request: Request<Body>) -> (StatusCode, Vec<u8>) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap()
        .to_vec();
    (status, body)
}

async fn json_response(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let (status, body) = response(app, request).await;
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn disabled_endpoints_match_lavalink_status_and_error_semantics() {
    let app = app(RoutePlanner::disabled()).await;
    let (status, body) = response(
        &app,
        request("GET", "/v4/routeplanner/status", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());

    let (status, error) = json_response(
        &app,
        request(
            "POST",
            "/v4/routeplanner/free/address",
            Body::from(r#"{"address":"127.0.0.1"}"#),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error["message"], "Can't access disabled route planner");

    let (status, error) = json_response(
        &app,
        request(
            "POST",
            "/v4/routeplanner/free/address",
            Body::from(r#"{"address":"localhost"}"#),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error["message"], "Can't access disabled route planner");

    let (status, error) = json_response(
        &app,
        request("POST", "/v4/routeplanner/free/all", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error["message"], "Can't access disabled route planner");
}

#[tokio::test]
async fn configured_status_and_free_operations_use_the_lavalink_wire_shape() {
    let planner = RoutePlanner::configured(RoutePlannerConfig::new(
        RoutePlannerStrategy::RotateOnBan,
        ["127.0.0.2/31"],
    ))
    .unwrap();
    let selected = planner.select(None).unwrap();
    planner.report(selected, RouteOutcome::SourceRateLimited);
    let app = app(planner.clone()).await;

    let (status, value) = json_response(
        &app,
        request("GET", "/v4/routeplanner/status", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["class"], "RotatingIpRoutePlanner");
    assert_eq!(
        value["details"]["ipBlock"],
        json!({
            "type": "Inet4Address",
            "size": "2",
        })
    );
    assert_eq!(value["details"]["rotateIndex"], "1");
    assert_eq!(value["details"]["ipIndex"], "1");
    assert_eq!(value["details"]["currentAddress"], "/127.0.0.2");
    let failure = &value["details"]["failingAddresses"][0];
    assert_eq!(failure["failingAddress"], "/127.0.0.2");
    assert!(failure["failingTimestamp"].as_u64().is_some());
    assert!(
        failure["failingTime"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );

    let (status, body) = response(
        &app,
        request(
            "POST",
            "/v4/routeplanner/free/address",
            Body::from(r#"{"address":"127.0.0.2"}"#),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());
    assert!(planner.snapshot().unwrap().failing_addresses.is_empty());

    let route = planner.select(None).unwrap();
    planner.report(route, RouteOutcome::SourceRateLimited);
    let (status, body) = response(
        &app,
        request("POST", "/v4/routeplanner/free/all", Body::from("{}")),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());
    assert!(planner.snapshot().unwrap().failing_addresses.is_empty());
}

#[tokio::test]
async fn every_declared_strategy_has_its_exact_discriminator_and_detail_fields() {
    let cases = [
        (
            RoutePlannerStrategy::RotateOnBan,
            "127.0.0.1/32",
            "RotatingIpRoutePlanner",
            StatusCode::INTERNAL_SERVER_ERROR,
            &["rotateIndex", "ipIndex", "currentAddress"][..],
        ),
        (
            RoutePlannerStrategy::LoadBalance,
            "127.0.0.1/32",
            "BalancingIpRoutePlanner",
            StatusCode::OK,
            &[][..],
        ),
        (
            RoutePlannerStrategy::NanoSwitch,
            "2001:db8::/64",
            "NanoIpRoutePlanner",
            StatusCode::OK,
            &["currentAddressIndex"][..],
        ),
        (
            RoutePlannerStrategy::RotatingNanoSwitch,
            "2001:db8::/63",
            "RotatingNanoIpRoutePlanner",
            StatusCode::OK,
            &["blockIndex", "currentAddressIndex"][..],
        ),
    ];
    for (strategy, block, class, expected_status, fields) in cases {
        let planner = RoutePlanner::configured(RoutePlannerConfig::new(strategy, [block])).unwrap();
        let app = app(planner).await;
        let (status, value) = json_response(
            &app,
            request("GET", "/v4/routeplanner/status", Body::empty()),
        )
        .await;
        assert_eq!(status, expected_status);
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            assert_eq!(value["error"], "Internal Server Error");
            assert!(value.get("message").is_none());
            continue;
        }
        assert_eq!(value["class"], class);
        assert!(value["details"]["ipBlock"].is_object());
        assert!(value["details"]["failingAddresses"].is_array());
        for field in fields {
            assert!(
                value["details"].get(field).is_some(),
                "missing {class}.{field}"
            );
        }
    }
}

#[tokio::test]
async fn free_address_rejects_dns_names_malformed_json_and_oversized_bodies() {
    let planner = RoutePlanner::configured(RoutePlannerConfig::new(
        RoutePlannerStrategy::LoadBalance,
        ["127.0.0.1/32"],
    ))
    .unwrap();
    let app = app(planner).await;
    for body in [
        r#"{"address":"localhost"}"#.to_owned(),
        r#"{"address":null}"#.to_owned(),
        "not-json".to_owned(),
    ] {
        let (status, _) = response(
            &app,
            request("POST", "/v4/routeplanner/free/address", Body::from(body)),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    let (status, _) = response(
        &app,
        request(
            "POST",
            "/v4/routeplanner/free/address",
            Body::from(vec![b'x'; 4 * 1024 + 1]),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[test]
fn literal_free_address_input_never_performs_dns_resolution() {
    assert!("127.0.0.1".parse::<IpAddr>().is_ok());
    assert!("localhost".parse::<IpAddr>().is_err());
}
