use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use crust::media::MantleAdapter;
use crust_server::CrustServer;
use crust_server::config::ServerConfig;
use crust_testkit::FakeMantle;
use futures_util::StreamExt;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct Harness {
    app: Router,
    address: SocketAddr,
    shutdown: CancellationToken,
    task: JoinHandle<std::io::Result<()>>,
}

impl Harness {
    async fn new(mut config: ServerConfig) -> Self {
        config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        config.port = 0;
        let server = CrustServer::bind(config).await.unwrap();
        let address = server.local_address().unwrap();
        let app = server.router();
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move { server.serve(task_shutdown).await });
        Self {
            app,
            address,
            shutdown,
            task,
        }
    }

    async fn with_adapter(mut config: ServerConfig, adapter: Arc<dyn MantleAdapter>) -> Self {
        config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        config.port = 0;
        let server = CrustServer::bind_with_adapter(config, adapter)
            .await
            .unwrap();
        let address = server.local_address().unwrap();
        let app = server.router();
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move { server.serve(task_shutdown).await });
        Self {
            app,
            address,
            shutdown,
            task,
        }
    }

    async fn stop(self) {
        self.shutdown.cancel();
        self.task.await.unwrap().unwrap();
    }
}

fn config() -> ServerConfig {
    ServerConfig::default()
        .with_password("test-password")
        .unwrap()
}

async fn open_session(address: SocketAddr, user_id: &str) -> (Socket, String) {
    let mut request = format!("ws://{address}/v4/websocket")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "test-password".parse().unwrap());
    request
        .headers_mut()
        .insert("user-id", user_id.parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();
    let ready = socket.next().await.unwrap().unwrap().into_text().unwrap();
    let ready: Value = serde_json::from_str(&ready).unwrap();
    let session_id = ready["sessionId"].as_str().unwrap().to_owned();
    let stats = socket.next().await.unwrap().unwrap().into_text().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&stats).unwrap()["op"],
        "stats"
    );
    (socket, session_id)
}

async fn response(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

fn authorized(method: &str, path: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", "test-password")
        .header("content-type", "application/json")
        .body(body.into())
        .unwrap()
}

#[tokio::test]
async fn typed_resource_policy_is_the_server_diagnostic_and_bind_gate() {
    let mut configured = config();
    configured.max_request_body_bytes = 1234;
    configured.max_websocket_message_bytes = 2345;
    configured.max_players = 17;
    configured.max_players_per_session = 3;
    configured.max_outbound_connections = 5;
    configured.max_retained_json_bytes = 3456;
    configured.max_json_depth = 7;
    configured.max_json_elements = 89;
    configured.websocket_send_timeout = std::time::Duration::from_millis(321);
    configured.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
    configured.port = 0;
    let app = CrustServer::bind(configured).await.unwrap().router();
    let (status, body) = response(&app, authorized("GET", "/crust/v1/info", Body::empty())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["limits"]["requestBodyBytes"], 1234);
    assert_eq!(body["limits"]["websocketMessageBytes"], 2345);
    assert_eq!(body["limits"]["players"], 17);
    assert_eq!(body["limits"]["playersPerSession"], 3);
    assert_eq!(body["limits"]["outboundConnections"], 5);
    assert_eq!(body["limits"]["retainedJsonBytes"], 3456);
    assert_eq!(body["limits"]["jsonDepth"], 7);
    assert_eq!(body["limits"]["jsonElements"], 89);
    assert_eq!(body["limits"]["websocketSendTimeoutMs"], 321);

    let mut invalid = config();
    invalid.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
    invalid.port = 0;
    invalid.max_players_per_session = 0;
    let error = match CrustServer::bind(invalid).await {
        Ok(_) => panic!("invalid centralized limits unexpectedly bound a server"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_session_and_global_player_overload_recover_without_partial_state() {
    let mut configured = config();
    configured.max_sessions = 4;
    configured.max_players = 2;
    configured.max_players_per_session = 1;
    let harness = Harness::new(configured).await;
    let (mut first_socket, first) = open_session(harness.address, "300000000000001701").await;
    let (mut second_socket, second) = open_session(harness.address, "300000000000001702").await;

    let first_player = format!("/v4/sessions/{first}/players/1701");
    let first_overflow = format!("/v4/sessions/{first}/players/1702");
    let second_player = format!("/v4/sessions/{second}/players/1703");
    assert_eq!(
        response(&harness.app, authorized("PATCH", &first_player, "{}"))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        response(&harness.app, authorized("PATCH", &first_overflow, "{}"),)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        response(&harness.app, authorized("PATCH", &second_player, "{}"))
            .await
            .0,
        StatusCode::OK
    );

    assert_eq!(
        response(
            &harness.app,
            authorized("DELETE", &first_player, Body::empty()),
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        response(&harness.app, authorized("PATCH", &first_overflow, "{}"),)
            .await
            .0,
        StatusCode::OK
    );

    first_socket.close(None).await.unwrap();
    second_socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_ambiguous_and_unbounded_json_is_rejected_before_player_creation() {
    let mut configured = config();
    configured.max_request_body_bytes = 4096;
    configured.max_retained_json_bytes = 48;
    configured.max_json_depth = 6;
    configured.max_json_elements = 10;
    let harness = Harness::new(configured).await;
    let (mut socket, session) = open_session(harness.address, "300000000000001703").await;
    let path = format!("/v4/sessions/{session}/players/1710");

    for body in [
        r#"{"paused":false,"paused":true}"#,
        r#"{"track":{"userData":{"a":{"b":{"c":{"d":1}}}}}}"#,
        r#"{"track":{"userData":{"large":"012345678901234567890123456789012345678901234567890123456789"}}}"#,
        r#"{"filters":{"pluginFilters":{"a":0,"b":1,"c":2,"d":3,"e":4,"f":5,"g":6,"h":7,"i":8}}}"#,
    ] {
        assert_eq!(
            response(&harness.app, authorized("PATCH", &path, body))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }

    let players = format!("/v4/sessions/{session}/players");
    let (status, body) = response(&harness.app, authorized("GET", &players, Body::empty())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!([]));

    let wrong_content_type = Request::patch(&path)
        .header("authorization", "test-password")
        .header("content-type", "text/plain")
        .body(Body::from("{}"))
        .unwrap();
    assert_eq!(
        response(&harness.app, wrong_content_type).await.0,
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let structured_suffix_json = Request::patch(&path)
        .header("authorization", "test-password")
        .header("content-type", "application/vnd.lavalink+json")
        .body(Body::from("{}"))
        .unwrap();
    assert_eq!(
        response(&harness.app, structured_suffix_json).await.0,
        StatusCode::OK
    );

    let unauthorized = Request::get("/v4/stats").body(Body::empty()).unwrap();
    assert_eq!(
        response(&harness.app, unauthorized).await.0,
        StatusCode::UNAUTHORIZED
    );
    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn player_identifier_loads_share_source_and_outbound_admission_and_recover() {
    let mut configured = config();
    configured.max_sessions = 2;
    configured.max_players = 2;
    configured.max_players_per_session = 2;
    configured.max_concurrent_loads = 1;
    configured.max_concurrent_source_requests = 1;
    configured.max_outbound_connections = 1;
    let fake = Arc::new(FakeMantle::default());
    fake.hold_loads();
    let adapter: Arc<dyn MantleAdapter> = fake.clone();
    let harness = Harness::with_adapter(configured, adapter).await;
    let (mut socket, session) = open_session(harness.address, "300000000000001704").await;
    let player = format!("/v4/sessions/{session}/players/1720");
    let held_app = harness.app.clone();
    let held = tokio::spawn(async move {
        response(
            &held_app,
            authorized("PATCH", &player, r#"{"identifier":"fixture:player-held"}"#),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while fake.active_loads() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert_eq!(
        response(
            &harness.app,
            authorized(
                "GET",
                "/v4/loadtracks?identifier=fixture%3Aoverloaded",
                Body::empty(),
            ),
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );

    fake.release_loads();
    assert_eq!(held.await.unwrap().0, StatusCode::OK);
    assert_eq!(
        response(
            &harness.app,
            authorized(
                "GET",
                "/v4/loadtracks?identifier=fixture%3Aactive-playback-bound",
                Body::empty(),
            ),
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let player = format!("/v4/sessions/{session}/players/1720");
    assert_eq!(
        response(&harness.app, authorized("DELETE", &player, Body::empty()),)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        response(
            &harness.app,
            authorized(
                "GET",
                "/v4/loadtracks?identifier=fixture%3Arecovered",
                Body::empty(),
            ),
        )
        .await
        .0,
        StatusCode::OK
    );
    socket.close(None).await.unwrap();
    harness.stop().await;
}
