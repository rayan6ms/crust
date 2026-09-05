use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use crust::media::MantleAdapter;
use crust::voice::{
    VoiceBackend, VoiceConnection, VoiceConnectionInfo, VoiceError, VoiceErrorKind, VoiceFuture,
};
use crust_server::CrustServer;
use crust_server::config::ServerConfig;
use crust_testkit::{FakeMantle, FakeVoiceBackend, FakeVoiceRecord};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

struct Harness {
    app: Router,
    address: SocketAddr,
    shutdown: CancellationToken,
    task: JoinHandle<std::io::Result<()>>,
}

struct HangingShutdownVoice;

impl VoiceBackend for HangingShutdownVoice {
    fn connect(
        &self,
        _info: VoiceConnectionInfo,
        _cancellation: CancellationToken,
    ) -> VoiceFuture<'_, Result<Arc<dyn VoiceConnection>, VoiceError>> {
        Box::pin(async {
            Err(VoiceError::new(
                VoiceErrorKind::ConnectionFailed,
                "synthetic voice connection failure",
            ))
        })
    }

    fn shutdown(&self) -> VoiceFuture<'_, Result<(), VoiceError>> {
        Box::pin(std::future::pending())
    }
}

impl Harness {
    async fn new(mantle: Arc<FakeMantle>, voice: Arc<FakeVoiceBackend>) -> Self {
        let mut config = ServerConfig::default()
            .with_password("test-password")
            .unwrap();
        config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        config.port = 0;
        config.player_executor_shards = 1;
        config.player_command_capacity = 16;
        let mantle: Arc<dyn MantleAdapter> = mantle;
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

async fn open_session(
    address: SocketAddr,
    user_id: &str,
) -> (
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
) {
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
    let stats: Value = serde_json::from_str(&stats).unwrap();
    assert_eq!(stats["op"], "stats");
    (socket, session_id)
}

fn patch(path: &str, body: Value) -> Request<Body> {
    Request::patch(path)
        .header("authorization", "test-password")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

async fn patch_json(app: &Router, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app.clone().oneshot(patch(path, body)).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn player_volume_reaches_media_and_survives_filter_replacement_without_wire_leaks() {
    let mantle = Arc::new(FakeMantle::default());
    let voice = Arc::new(FakeVoiceBackend::new(64, 4));
    let harness = Harness::new(Arc::clone(&mantle), voice).await;
    let (socket, session_id) = open_session(harness.address, "300000000000000710").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000710");
    for (update, volume, filter_volume) in [
        (json!({"volume":37}), 37, None),
        (json!({"filters":{"volume":0.5}}), 37, Some(0.5)),
        (json!({"volume":0}), 0, Some(0.5)),
        (json!({"filters":{}}), 0, None),
        (json!({"volume":100}), 100, None),
    ] {
        let (status, player) = patch_json(&harness.app, &path, update).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(player["volume"], volume);
        assert!(player["filters"].get("player_volume").is_none());
        assert!(player["filters"].get("playerVolume").is_none());
        let configuration = mantle.last_filters().expect("player volume reached Mantle");
        assert_eq!(configuration.player_volume, Some(volume));
        assert_eq!(configuration.volume, filter_volume);
        assert_eq!(
            configuration.is_effective(),
            volume != 100 || filter_volume.is_some()
        );
    }
    drop(socket);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_shutdown_deadline_includes_voice_backend_teardown() {
    let mut config = ServerConfig::default();
    config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
    config.port = 0;
    config.player_executor_shards = 1;
    config.shutdown_timeout = std::time::Duration::from_millis(25);
    let mantle: Arc<dyn MantleAdapter> = Arc::new(FakeMantle::default());
    let voice: Arc<dyn VoiceBackend> = Arc::new(HangingShutdownVoice);
    let server = CrustServer::bind_with_backends(config, mantle, voice)
        .await
        .unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    let error = tokio::time::timeout(std::time::Duration::from_secs(1), server.serve(shutdown))
        .await
        .expect("the server-owned deadline must cover backend teardown")
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn player_routes_voice_updates_and_media_discontinuities_through_one_backend_pacer() {
    let mantle = Arc::new(FakeMantle::default());
    let voice = Arc::new(FakeVoiceBackend::new(64, 4));
    let harness = Harness::new(mantle, Arc::clone(&voice)).await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000710").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000710");
    let voice_state = json!({
        "token": "test-token",
        "endpoint": "voice.example.invalid",
        "sessionId": "test-session",
        "channelId": "900000000000000710"
    });

    let (status, started) = patch_json(
        &harness.app,
        &path,
        json!({
            "track": {"identifier": "fixture:voice-one"},
            "voice": voice_state
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(started["state"]["connected"], true);
    assert_eq!(started["state"]["ping"], 7);
    assert_eq!(
        voice.records()[..2],
        [
            FakeVoiceRecord::Connect {
                guild_id: 800000000000000710,
                user_id: 300000000000000710,
                channel_id: 900000000000000710,
            },
            FakeVoiceRecord::SetSource { generation: 1 },
        ]
    );
    let connection = voice.latest_connection().unwrap();
    let frame = connection.pull_frame().await.unwrap().unwrap();
    assert_eq!(frame.sequence(), 0);
    assert!(!frame.payload().is_empty());

    assert_eq!(
        patch_json(&harness.app, &path, json!({"paused": true}))
            .await
            .0,
        StatusCode::OK
    );
    assert!(matches!(
        voice.records().last(),
        Some(FakeVoiceRecord::StopAudio)
    ));

    assert_eq!(
        patch_json(&harness.app, &path, json!({"paused": false}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(connection.source_generation(), 2);

    assert_eq!(
        patch_json(&harness.app, &path, json!({"position": 80}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(connection.source_generation(), 3);

    assert_eq!(
        patch_json(&harness.app, &path, json!({"filters": {"volume": 0.5}}),)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(connection.source_generation(), 4);

    let (_, moved) = patch_json(
        &harness.app,
        &path,
        json!({
            "voice": {
                "token": "replacement-token",
                "endpoint": "voice-2.example.invalid",
                "sessionId": "replacement-session",
                "channelId": "900000000000000711"
            }
        }),
    )
    .await;
    assert_eq!(moved["voice"]["channelId"], "900000000000000711");
    assert_eq!(connection.source_generation(), 5);
    assert!(voice.records().contains(&FakeVoiceRecord::Update {
        guild_id: 800000000000000710,
        user_id: 300000000000000710,
        channel_id: 900000000000000711,
    }));

    assert_eq!(
        patch_json(&harness.app, &path, json!({"track": {"encoded": null}}),)
            .await
            .0,
        StatusCode::OK
    );
    assert!(matches!(
        voice.records().last(),
        Some(FakeVoiceRecord::StopAudio)
    ));

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::delete(&path)
                .header("authorization", "test-password")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        voice
            .records()
            .contains(&FakeVoiceRecord::ShutdownConnection)
    );

    socket.close(None).await.unwrap();
    harness.stop().await;
    assert!(voice.records().contains(&FakeVoiceRecord::ShutdownBackend));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_snowflake_session_identity_fails_before_credentials_reach_the_backend() {
    let voice = Arc::new(FakeVoiceBackend::new(8, 1));
    let harness = Harness::new(Arc::new(FakeMantle::default()), Arc::clone(&voice)).await;
    let (mut socket, session_id) = open_session(harness.address, "not-a-snowflake").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000711");
    let (status, _) = patch_json(
        &harness.app,
        &path,
        json!({
            "voice": {
                "token": "test-token",
                "endpoint": "voice.example.invalid",
                "sessionId": "test-session",
                "channelId": "900000000000000711"
            }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(voice.records().is_empty());
    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn natural_source_end_publishes_track_end_without_another_rest_command() {
    let voice = Arc::new(FakeVoiceBackend::new(32, 2));
    let harness = Harness::new(Arc::new(FakeMantle::default()), Arc::clone(&voice)).await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000712").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000712");
    let (status, _) = patch_json(
        &harness.app,
        &path,
        json!({
            "track": {"identifier": "fixture:short"},
            "voice": {
                "token": "test-token",
                "endpoint": "voice.example.invalid",
                "sessionId": "test-session",
                "channelId": "900000000000000712"
            }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let connection = voice.latest_connection().unwrap();
    assert!(connection.pull_frame().await.unwrap().is_some());
    assert!(connection.pull_frame().await.unwrap().is_some());
    assert!(connection.pull_frame().await.unwrap().is_none());

    let track_end = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let message = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let message: Value = serde_json::from_str(&message).unwrap();
            if message["type"] == "TrackEndEvent" {
                break message;
            }
        }
    })
    .await
    .expect("natural end event must not wait for another REST request");
    assert_eq!(track_end["reason"], "finished");

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn asynchronous_frame_failure_publishes_exception_and_load_failed_end() {
    let voice = Arc::new(FakeVoiceBackend::new(32, 2));
    let harness = Harness::new(Arc::new(FakeMantle::default()), Arc::clone(&voice)).await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000713").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000713");
    let (status, _) = patch_json(
        &harness.app,
        &path,
        json!({
            "track": {"identifier": "fixture:frame-error"},
            "voice": {
                "token": "test-token",
                "endpoint": "voice.example.invalid",
                "sessionId": "test-session",
                "channelId": "900000000000000713"
            }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let connection = voice.latest_connection().unwrap();
    let failure = connection.pull_frame().await.unwrap_err();
    assert_eq!(failure.kind, crust::voice::VoiceErrorKind::Protocol);

    let events = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        let mut exception = None;
        let mut end = None;
        while exception.is_none() || end.is_none() {
            let message = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let message: Value = serde_json::from_str(&message).unwrap();
            match message["type"].as_str() {
                Some("TrackExceptionEvent") => exception = Some(message),
                Some("TrackEndEvent") => end = Some(message),
                _ => {}
            }
        }
        (exception.unwrap(), end.unwrap())
    })
    .await
    .expect("frame failure events must not wait for another REST request");
    assert_eq!(
        events.0["exception"]["message"],
        "Mantle frame production failed"
    );
    assert_eq!(events.1["reason"], "loadFailed");

    socket.close(None).await.unwrap();
    harness.stop().await;
}
