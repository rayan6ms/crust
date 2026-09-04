use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use crust::media::MantleAdapter;
use crust::voice::{VoiceBackend, VoiceCounters};
use crust_server::config::ServerConfig;
use crust_server::{CRUST_VERSION, CrustServer, LAVALINK_VERSION, MANTLE_REVISION, OTO_VERSION};
use crust_testkit::{FakeMantle, FakeVoiceBackend};
use futures_util::StreamExt;
use serde_json::{Value, json};
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
    async fn new(voice: Arc<FakeVoiceBackend>) -> Self {
        let mut config = ServerConfig::default()
            .with_password("test-password")
            .unwrap();
        config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        config.port = 0;
        config.max_sessions = 2;
        config.max_players = 4;
        config.player_executor_shards = 1;
        config.player_command_capacity = 8;
        config.player_update_interval = Duration::from_secs(60);
        config.stats_interval = Duration::from_millis(25);
        let mantle: Arc<dyn MantleAdapter> = Arc::new(FakeMantle::default());
        let voice: Arc<dyn VoiceBackend> = voice;
        let server = CrustServer::bind_with_backends(config, mantle, voice)
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

async fn open_session(address: SocketAddr) -> (Socket, String, Value) {
    let mut request = format!("ws://{address}/v4/websocket")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "test-password".parse().unwrap());
    request
        .headers_mut()
        .insert("user-id", "400000000000000012".parse().unwrap());
    request
        .headers_mut()
        .insert("client-name", "Crust/P12".parse().unwrap());
    let (mut socket, response) = connect_async(request).await.unwrap();
    assert_eq!(response.headers()["x-crust-version"], CRUST_VERSION);
    assert!(response.headers().contains_key("x-crust-commit"));
    let ready = next_json(&mut socket).await;
    assert_eq!(ready["op"], "ready");
    let session_id = ready["sessionId"].as_str().unwrap().to_owned();
    let stats = next_json(&mut socket).await;
    (socket, session_id, stats)
}

async fn resume_session(address: SocketAddr, session_id: &str) -> Socket {
    let mut request = format!("ws://{address}/v4/websocket")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "test-password".parse().unwrap());
    request
        .headers_mut()
        .insert("user-id", "400000000000000012".parse().unwrap());
    request
        .headers_mut()
        .insert("client-name", "Crust/P12".parse().unwrap());
    request
        .headers_mut()
        .insert("session-id", session_id.parse().unwrap());
    let (mut socket, response) = connect_async(request).await.unwrap();
    assert_eq!(response.headers()["session-resumed"], "true");
    let ready = next_json(&mut socket).await;
    assert_eq!(ready["op"], "ready");
    assert_eq!(ready["resumed"], true);
    assert_eq!(ready["sessionId"], session_id);
    socket
}

fn request(method: &str, path: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", "test-password")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

async fn json_request(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn next_json(socket: &mut Socket) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(1), socket.next())
        .await
        .expect("expected a bounded WebSocket message delay")
        .expect("WebSocket stream ended")
        .expect("WebSocket read failed")
        .into_text()
        .expect("server sent a non-text WebSocket message");
    serde_json::from_str(&message).unwrap()
}

async fn next_stats(socket: &mut Socket) -> Value {
    loop {
        let message = next_json(socket).await;
        if message["op"] == "stats" {
            return message;
        }
    }
}

async fn next_player_update(socket: &mut Socket) -> Value {
    loop {
        let message = next_json(socket).await;
        if message["op"] == "playerUpdate" {
            return message;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_and_websocket_stats_report_real_player_state_and_frame_windows() {
    let voice = Arc::new(FakeVoiceBackend::new(8, 1));
    let harness = Harness::new(Arc::clone(&voice)).await;
    let (mut socket, session_id, initial) = open_session(harness.address).await;
    assert_eq!(initial["op"], "stats");
    assert_eq!(initial["players"], 0);
    assert_eq!(initial["playingPlayers"], 0);
    assert_eq!(initial["frameStats"], Value::Null);

    let path = format!("/v4/sessions/{session_id}/players/800000000000000012");
    let (status, _) = json_request(
        &harness.app,
        request(
            "PATCH",
            &path,
            json!({
                "track": {"identifier": "fixture:p12-stats"},
                "voice": {
                    "token": "test-token",
                    "endpoint": "voice.example.invalid",
                    "sessionId": "test-session",
                    "channelId": "900000000000000012",
                }
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(next_json(&mut socket).await["type"], "TrackStartEvent");
    next_player_update(&mut socket).await;

    let mut first_playing = next_stats(&mut socket).await;
    while first_playing["players"] != 1 {
        first_playing = next_stats(&mut socket).await;
    }
    assert_eq!(first_playing["players"], 1);
    assert_eq!(first_playing["playingPlayers"], 1);
    assert_eq!(first_playing["frameStats"], Value::Null);

    voice
        .latest_connection()
        .unwrap()
        .set_counters(VoiceCounters {
            sent: 2_975,
            nulled: 20,
            deficit: 5,
        });
    assert_eq!(
        json_request(&harness.app, request("GET", &path, Value::Null))
            .await
            .0,
        StatusCode::OK
    );
    let measured = next_stats(&mut socket).await;
    assert_eq!(
        measured["frameStats"],
        json!({"sent": 2_975, "nulled": 20, "deficit": 5})
    );

    let (status, rest) = json_request(&harness.app, request("GET", "/v4/stats", Value::Null)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rest["frameStats"], Value::Null);
    assert_eq!(rest["players"], 1);
    assert_eq!(rest["playingPlayers"], 1);
    assert!(rest["uptime"].as_u64().is_some());
    assert!(rest["memory"]["used"].as_u64().unwrap() > 0);
    assert_eq!(rest["memory"]["used"], rest["memory"]["allocated"]);
    assert!(
        rest["memory"]["reservable"].as_u64().unwrap() >= rest["memory"]["used"].as_u64().unwrap()
    );
    assert!(rest["cpu"]["cores"].as_u64().unwrap() > 0);
    for key in ["systemLoad", "lavalinkLoad"] {
        assert!((0.0..=1.0).contains(&rest["cpu"][key].as_f64().unwrap()));
    }

    assert_eq!(
        json_request(
            &harness.app,
            request("PATCH", &path, json!({"paused": true}))
        )
        .await
        .0,
        StatusCode::OK
    );
    let paused = next_stats(&mut socket).await;
    assert_eq!(paused["playingPlayers"], 0);
    assert_eq!(paused["frameStats"], Value::Null);

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test]
async fn standard_and_crust_identity_are_compatible_truthful_and_separate() {
    let voice = Arc::new(FakeVoiceBackend::new(2, 1));
    let harness = Harness::new(voice).await;

    let (status, info) = json_request(&harness.app, request("GET", "/v4/info", Value::Null)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["version"]["semver"], LAVALINK_VERSION);
    assert!(info["jvm"].as_str().unwrap().starts_with("Rust "));
    assert_eq!(
        info["lavaplayer"],
        format!("Mantle {}", &MANTLE_REVISION[..12])
    );
    assert!(
        info["sourceManagers"]
            .as_array()
            .unwrap()
            .contains(&json!("youtube"))
    );
    assert!(
        info["filters"]
            .as_array()
            .unwrap()
            .contains(&json!("timescale"))
    );

    let (status, product) =
        json_request(&harness.app, request("GET", "/crust/v1/info", Value::Null)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(product["name"], "Crust");
    assert_eq!(product["version"], CRUST_VERSION);
    assert_eq!(product["protocol"]["version"], LAVALINK_VERSION);
    assert_eq!(product["media"]["revision"], MANTLE_REVISION);
    assert_eq!(product["voice"]["name"], "Oto");
    assert_eq!(product["voice"]["version"], OTO_VERSION);
    assert_eq!(product["voice"]["voiceGatewayVersion"], 8);
    assert_eq!(product["voice"]["daveProtocolVersions"], json!([1]));
    assert!(product["build"]["git"]["commit"].as_str().is_some());
    assert_eq!(product["statistics"]["websocketIntervalMs"], 25);

    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumable_sessions_preserve_their_frame_window_without_duplicate_initial_stats() {
    let voice = Arc::new(FakeVoiceBackend::new(8, 1));
    let harness = Harness::new(Arc::clone(&voice)).await;
    let (mut socket, session_id, _) = open_session(harness.address).await;
    let player_path = format!("/v4/sessions/{session_id}/players/800000000000000013");
    assert_eq!(
        json_request(
            &harness.app,
            request(
                "PATCH",
                &player_path,
                json!({
                    "track": {"identifier": "fixture:p12-resume"},
                    "voice": {
                        "token": "test-token",
                        "endpoint": "voice.example.invalid",
                        "sessionId": "test-session",
                        "channelId": "900000000000000013",
                    }
                }),
            ),
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(next_json(&mut socket).await["type"], "TrackStartEvent");
    next_player_update(&mut socket).await;
    let mut baseline = next_stats(&mut socket).await;
    while baseline["playingPlayers"] != 1 {
        baseline = next_stats(&mut socket).await;
    }
    assert_eq!(baseline["frameStats"], Value::Null);
    assert_eq!(
        json_request(
            &harness.app,
            request(
                "PATCH",
                &format!("/v4/sessions/{session_id}"),
                json!({"resuming": true, "timeout": 10}),
            ),
        )
        .await
        .0,
        StatusCode::OK
    );
    socket.close(None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    voice
        .latest_connection()
        .unwrap()
        .set_counters(VoiceCounters {
            sent: 120,
            nulled: 3,
            deficit: 2,
        });
    assert_eq!(
        json_request(&harness.app, request("GET", &player_path, Value::Null))
            .await
            .0,
        StatusCode::OK
    );

    let mut resumed = resume_session(harness.address, &session_id).await;
    assert_eq!(next_json(&mut resumed).await["op"], "playerUpdate");
    let measured = loop {
        let stats = next_stats(&mut resumed).await;
        if stats["frameStats"]["sent"] == 120 {
            break stats;
        }
    };
    assert_eq!(
        measured["frameStats"],
        json!({"sent": 120, "nulled": 3, "deficit": 2})
    );

    resumed.close(None).await.unwrap();
    harness.stop().await;
}
