use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use crust_server::CrustServer;
use crust_server::config::ServerConfig;
use crust_server::session::SessionClock;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

#[derive(Default)]
struct ManualClock(AtomicU64);

impl ManualClock {
    fn advance(&self, duration: Duration) {
        self.0.fetch_add(
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            Ordering::AcqRel,
        );
    }
}

impl SessionClock for ManualClock {
    fn now(&self) -> Duration {
        Duration::from_millis(self.0.load(Ordering::Acquire))
    }
}

fn config() -> ServerConfig {
    let mut config = ServerConfig::default()
        .with_password("test-password")
        .unwrap();
    config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
    config.port = 0;
    config.max_sessions = 16;
    config
}

async fn server(
    clock: Arc<ManualClock>,
) -> (
    Router,
    SocketAddr,
    CancellationToken,
    JoinHandle<std::io::Result<()>>,
) {
    let server = CrustServer::bind_with_clock(config(), clock).await.unwrap();
    let address = server.local_address().unwrap();
    let app = server.router();
    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.clone();
    let task = tokio::spawn(async move { server.serve(task_shutdown).await });
    (app, address, shutdown, task)
}

fn websocket_request(address: SocketAddr, user_id: &str, session_id: Option<&str>) -> Request<()> {
    let mut request = format!("ws://{address}/v4/websocket")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "test-password".parse().unwrap());
    request
        .headers_mut()
        .insert("user-id", user_id.parse().unwrap());
    request
        .headers_mut()
        .insert("client-name", "Crust/P06".parse().unwrap());
    if let Some(session_id) = session_id {
        request
            .headers_mut()
            .insert("session-id", session_id.parse().unwrap());
    }
    request
}

async fn ready(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Value {
    serde_json::from_str(
        socket
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap()
}

async fn patch_session(app: &Router, session_id: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::patch(format!("/v4/sessions/{session_id}"))
                .header("authorization", "test-password")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn close(
    mut socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    socket.close(None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_patch_and_resume_match_the_frozen_v4_shapes() {
    let clock = Arc::new(ManualClock::default());
    let (app, address, shutdown, task) = server(clock).await;

    let (mut first, first_response) =
        connect_async(websocket_request(address, "200000000000000201", None))
            .await
            .unwrap();
    assert_eq!(first_response.headers()["session-resumed"], "false");
    let first_ready = ready(&mut first).await;
    let session_id = first_ready["sessionId"].as_str().unwrap().to_owned();
    assert_eq!(first_ready["resumed"], false);

    assert_eq!(
        patch_session(&app, &session_id, json!({})).await,
        (StatusCode::OK, json!({"resuming": false, "timeout": 60}))
    );
    assert_eq!(
        patch_session(&app, &session_id, json!({"resuming": true, "timeout": 15}),).await,
        (StatusCode::OK, json!({"resuming": true, "timeout": 15}))
    );
    assert_eq!(
        patch_session(&app, &session_id, json!({"resuming": null}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        patch_session(&app, "missing", json!({})).await.0,
        StatusCode::NOT_FOUND
    );

    close(first).await;
    let (mut resumed, response) = connect_async(websocket_request(
        address,
        "200000000000000201",
        Some(&session_id),
    ))
    .await
    .unwrap();
    assert_eq!(response.headers()["session-resumed"], "true");
    assert_eq!(
        ready(&mut resumed).await,
        json!({"op": "ready", "resumed": true, "sessionId": session_id})
    );

    patch_session(&app, &session_id, json!({"resuming": false})).await;
    close(resumed).await;
    let (mut fresh, response) = connect_async(websocket_request(
        address,
        "200000000000000201",
        Some(&session_id),
    ))
    .await
    .unwrap();
    assert_eq!(response.headers()["session-resumed"], "false");
    assert_ne!(ready(&mut fresh).await["sessionId"], session_id);
    close(fresh).await;

    shutdown.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_user_and_expired_resume_attempts_create_fresh_sessions() {
    let clock = Arc::new(ManualClock::default());
    let (app, address, shutdown, task) = server(Arc::clone(&clock)).await;
    let owner_id = "200000000000000202";

    let (mut owner, _) = connect_async(websocket_request(address, owner_id, None))
        .await
        .unwrap();
    let session_id = ready(&mut owner).await["sessionId"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        patch_session(&app, &session_id, json!({"resuming": true, "timeout": 1}),)
            .await
            .0,
        StatusCode::OK
    );
    close(owner).await;

    let (mut attacker, response) = connect_async(websocket_request(
        address,
        "200000000000000999",
        Some(&session_id),
    ))
    .await
    .unwrap();
    assert_eq!(response.headers()["session-resumed"], "false");
    assert_ne!(ready(&mut attacker).await["sessionId"], session_id);
    close(attacker).await;

    let (mut owner, response) =
        connect_async(websocket_request(address, owner_id, Some(&session_id)))
            .await
            .unwrap();
    assert_eq!(response.headers()["session-resumed"], "true");
    assert_eq!(ready(&mut owner).await["sessionId"], session_id);
    close(owner).await;

    clock.advance(Duration::from_secs(2));
    let (mut expired, response) =
        connect_async(websocket_request(address, owner_id, Some(&session_id)))
            .await
            .unwrap();
    assert_eq!(response.headers()["session-resumed"], "false");
    assert_ne!(ready(&mut expired).await["sessionId"], session_id);
    close(expired).await;

    shutdown.cancel();
    task.await.unwrap().unwrap();
}
