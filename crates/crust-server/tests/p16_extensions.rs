use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::Request;
use crust::media::MantleAdapter;
use crust::voice::VoiceBackend;
use crust_server::CrustServer;
use crust_server::config::ServerConfig;
use crust_testkit::{FakeMantle, FakeVoiceBackend};
use serde_json::Value;
use tower::ServiceExt;

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), 256 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn request(path: &str) -> Request<Body> {
    Request::get(path)
        .header("authorization", "test-password")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn in_tree_extensions_are_bounded_sorted_and_reported_separately() {
    let mut config = ServerConfig::default()
        .with_password("test-password")
        .unwrap();
    config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
    config.port = 0;
    let core_only = CrustServer::bind(config.clone()).await.unwrap().router();
    let product = json_body(
        core_only
            .clone()
            .oneshot(request("/crust/v1/info"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(product["extensions"], serde_json::json!([]));
    let protocol = json_body(core_only.oneshot(request("/v4/info")).await.unwrap()).await;
    assert_eq!(protocol["plugins"], serde_json::json!([]));

    let mantle: Arc<dyn MantleAdapter> = Arc::new(FakeMantle::default());
    let voice: Arc<dyn VoiceBackend> = Arc::new(FakeVoiceBackend::new(2, 1));
    let app = CrustServer::bind_with_backends(config, mantle, voice)
        .await
        .unwrap()
        .router();

    let product = json_body(
        app.clone()
            .oneshot(request("/crust/v1/info"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        product["extensions"],
        serde_json::json!([
            {
                "id": "crust.mantle",
                "version": "55b718058a36731e1c757b5edf53f432dc01ce3e",
                "kind": "media",
                "capabilities": ["source:youtube", "media:opus", "media:pcm-filters"]
            },
            {
                "id": "crust.oto",
                "version": "1.0.0",
                "kind": "voice",
                "capabilities": ["voice:discord", "voice:dave", "voice:opus-pacing"]
            }
        ])
    );

    let protocol = json_body(app.oneshot(request("/v4/info")).await.unwrap()).await;
    assert_eq!(protocol["plugins"], serde_json::json!([]));
}
