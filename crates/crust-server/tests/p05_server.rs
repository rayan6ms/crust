use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use crust_server::config::ServerConfig;
use crust_server::{CrustServer, LAVALINK_VERSION};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tower::ServiceExt;

fn config() -> ServerConfig {
    let mut config = ServerConfig::default()
        .with_password("test-password")
        .unwrap();
    config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
    config.port = 0;
    config
}

async fn application() -> axum::Router {
    CrustServer::bind(config()).await.unwrap().router()
}

fn request(path: &str, authorization: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri(path);
    if let Some(authorization) = authorization {
        builder = builder.header("authorization", authorization);
    }
    builder.body(Body::empty()).unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn http_auth_version_info_stats_and_owned_errors_match_v4_shapes() {
    let app = application().await;

    let missing = app
        .clone()
        .oneshot(request("/version", None))
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(missing.headers()["lavalink-api-version"], "4");
    assert!(missing.headers().contains_key("x-request-id"));
    assert_eq!(to_bytes(missing.into_body(), 64).await.unwrap().len(), 0);

    let wrong = app
        .clone()
        .oneshot(request("/version", Some("wrong")))
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::FORBIDDEN);
    assert_eq!(to_bytes(wrong.into_body(), 64).await.unwrap().len(), 0);

    let version = app
        .clone()
        .oneshot(request("/version", Some("test-password")))
        .await
        .unwrap();
    assert_eq!(version.status(), StatusCode::OK);
    assert_eq!(
        version.headers()["content-type"],
        "text/plain;charset=ISO-8859-1"
    );
    assert_eq!(
        to_bytes(version.into_body(), 64).await.unwrap(),
        LAVALINK_VERSION
    );

    let info = app
        .clone()
        .oneshot(request("/v4/info", Some("test-password")))
        .await
        .unwrap();
    let info = json_body(info).await;
    assert_eq!(info["version"]["semver"], "4.2.2");
    assert_eq!(info["plugins"], json!([]));
    assert!(info["jvm"].as_str().unwrap().starts_with("Rust "));
    assert!(info["lavaplayer"].as_str().unwrap().starts_with("Mantle "));

    let first = app
        .clone()
        .oneshot(request("/v4/stats", Some("test-password")))
        .await
        .unwrap();
    let first = json_body(first).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let second = app
        .clone()
        .oneshot(request("/v4/stats", Some("test-password")))
        .await
        .unwrap();
    let second = json_body(second).await;
    assert_eq!(first["frameStats"], Value::Null);
    assert!(second["uptime"].as_u64() >= first["uptime"].as_u64());
    assert!(second["cpu"]["cores"].as_u64().unwrap() > 0);

    let missing_route = app
        .oneshot(request(
            "/v4/not-implemented?trace=true",
            Some("test-password"),
        ))
        .await
        .unwrap();
    assert_eq!(missing_route.status(), StatusCode::NOT_FOUND);
    let error = json_body(missing_route).await;
    assert_eq!(error["status"], 404);
    assert_eq!(error["path"], "/v4/not-implemented");
    assert!(
        error["trace"]
            .as_str()
            .is_some_and(|trace| !trace.is_empty())
    );
}

fn websocket_request(url: &str, user_id: Option<&str>) -> axum::http::Request<()> {
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("authorization", "test-password".parse().unwrap());
    if let Some(user_id) = user_id {
        request
            .headers_mut()
            .insert("user-id", user_id.parse().unwrap());
    }
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_validates_headers_emits_ready_and_reconnects() {
    let server = CrustServer::spawn(config()).await.unwrap();
    let url = format!("ws://{}/v4/websocket", server.local_address());

    let missing_auth = connect_async(&url).await.unwrap_err();
    assert_eq!(http_status(missing_auth), 401);

    let missing_user = connect_async(websocket_request(&url, None))
        .await
        .unwrap_err();
    assert_eq!(http_status(missing_user), 400);

    let zero_user = connect_async(websocket_request(&url, Some("0")))
        .await
        .unwrap_err();
    assert_eq!(http_status(zero_user), 400);

    let (mut first, first_response) =
        connect_async(websocket_request(&url, Some("200000000000000001")))
            .await
            .unwrap();
    assert_eq!(first_response.headers()["session-resumed"], "false");
    let first_ready: Value = serde_json::from_str(
        first
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_eq!(first_ready["op"], "ready");
    assert_eq!(first_ready["resumed"], false);
    let first_stats: Value = serde_json::from_str(
        first
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_eq!(first_stats["op"], "stats");
    assert_eq!(first_stats["frameStats"], Value::Null);
    first.send(Message::Text("ignored".into())).await.unwrap();
    first
        .send(Message::Ping(vec![1, 2, 3].into()))
        .await
        .unwrap();
    assert!(matches!(
        first.next().await.unwrap().unwrap(),
        Message::Pong(_)
    ));
    first.close(None).await.unwrap();

    let (mut second, _) = connect_async(websocket_request(&url, Some("not-numeric")))
        .await
        .unwrap();
    let second_ready: Value = serde_json::from_str(
        second
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_ne!(first_ready["sessionId"], second_ready["sessionId"]);
    second.close(None).await.unwrap();

    server.shutdown(Duration::from_secs(2)).await.unwrap();
}

fn http_status(error: WebSocketError) -> StatusCode {
    match error {
        WebSocketError::Http(response) => response.status(),
        other => panic!("expected HTTP handshake failure, got {other}"),
    }
}
