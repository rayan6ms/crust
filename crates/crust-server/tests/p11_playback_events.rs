use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use crust::media::MantleAdapter;
use crust::voice::{VoiceBackend, VoiceClose, VoiceEvent, VoicePhase};
#[cfg(feature = "oto-voice")]
use crust_oto_adapter::OtoVoiceBackend;
use crust_server::CrustServer;
use crust_server::config::ServerConfig;
use crust_testkit::{FakeMantle, FakeVoiceBackend, FakeVoiceRecord};
use futures_util::StreamExt;
#[cfg(feature = "oto-voice")]
use oto::Oto;
#[cfg(feature = "oto-voice")]
use oto_testkit::{
    FakeUdpServer, FakeUdpServerConfig, FakeVoiceGateway, FakeVoiceGatewayConfig, ManualClock,
};
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
    async fn new(
        mantle: Arc<FakeMantle>,
        voice: Arc<FakeVoiceBackend>,
        player_update_interval: Duration,
    ) -> Self {
        Self::with_stuck_threshold(
            mantle,
            voice,
            player_update_interval,
            Duration::from_secs(10),
        )
        .await
    }

    async fn with_stuck_threshold(
        mantle: Arc<FakeMantle>,
        voice: Arc<FakeVoiceBackend>,
        player_update_interval: Duration,
        threshold: Duration,
    ) -> Self {
        let mut config = ServerConfig::default()
            .with_password("test-password")
            .unwrap();
        config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        config.port = 0;
        config.max_sessions = 4;
        config.max_players = 8;
        config.player_executor_shards = 1;
        config.player_command_capacity = 16;
        config.player_update_interval = player_update_interval;
        config.media.track_stuck_threshold = threshold;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_stuck_detection_preserves_frame_and_rejects_stale_sources() {
    let voice = Arc::new(FakeVoiceBackend::new(32, 2));
    let mantle = Arc::new(FakeMantle::default());
    let threshold = Duration::from_millis(40);
    let harness = Harness::with_stuck_threshold(
        Arc::clone(&mantle),
        Arc::clone(&voice),
        Duration::from_secs(30),
        threshold,
    )
    .await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000899").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000899");
    assert_eq!(
        json_request(
            &harness.app,
            patch(
                &path,
                json!({
                    "track": {"identifier": "fixture:slow-frame", "userData": {"marker":"stalled"}},
                    "voice": voice_state("900000000000000899")
                })
            )
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(next_json(&mut socket).await["type"], "TrackStartEvent");
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");
    let connection = voice.latest_connection().unwrap();
    for sequence in 0..2 {
        let pending = {
            let connection = Arc::clone(&connection);
            tokio::spawn(async move { connection.pull_frame().await })
        };
        let event = next_json(&mut socket).await;
        assert_eq!(event["type"], "TrackStuckEvent");
        assert_eq!(event["thresholdMs"], 40);
        assert_eq!(event["track"]["userData"]["marker"], "stalled");
        let frame = pending.await.unwrap().unwrap().unwrap();
        assert_eq!(frame.sequence(), sequence);
    }
    assert!(
        tokio::time::timeout(threshold * 2, next_json(&mut socket))
            .await
            .is_err(),
        "one event per blocked demand"
    );
    let stale = connection.pull_frame();
    tokio::pin!(stale);
    assert!(futures_util::poll!(&mut stale).is_pending());
    assert_eq!(
        json_request(&harness.app, patch(&path, json!({"paused":true})))
            .await
            .0,
        StatusCode::OK
    );
    let (old_result, no_event) = tokio::join!(
        &mut stale,
        tokio::time::timeout(Duration::from_millis(250), next_json(&mut socket))
    );
    assert!(old_result.unwrap().is_none());
    assert!(
        no_event.is_err(),
        "stale pending demand must not emit after generation change"
    );
    assert!(connection.pull_frame().await.unwrap().is_none());
    assert!(
        tokio::time::timeout(threshold * 2, next_json(&mut socket))
            .await
            .is_err(),
        "pause has no source demand"
    );
    // Invalid DSP configuration must leave the accepted filter chain intact.
    assert_eq!(
        json_request(
            &harness.app,
            patch(&path, json!({"filters":{"volume":0.5}}))
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");
    let before = mantle.last_filters().unwrap();
    assert_eq!(
        json_request(
            &harness.app,
            patch(
                &path,
                json!({"filters":{"karaoke":{"filterWidth":1_000_000.0}}})
            )
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(mantle.last_filters().unwrap(), before);
    socket.close(None).await.unwrap();
    harness.stop().await;
}

async fn open_session(address: SocketAddr, user_id: &str) -> (Socket, String) {
    open_session_with_id(address, user_id, None).await
}

async fn open_session_with_id(
    address: SocketAddr,
    user_id: &str,
    session_id: Option<&str>,
) -> (Socket, String) {
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
        .insert("client-name", "Crust/P11".parse().unwrap());
    if let Some(session_id) = session_id {
        request
            .headers_mut()
            .insert("session-id", session_id.parse().unwrap());
    }
    let (mut socket, _) = connect_async(request).await.unwrap();
    let ready = next_json(&mut socket).await;
    assert_eq!(ready["op"], "ready");
    let ready_session_id = ready["sessionId"].as_str().unwrap().to_owned();
    if session_id.is_none() {
        let stats = next_json(&mut socket).await;
        assert_eq!(stats["op"], "stats");
        assert_eq!(stats["frameStats"], Value::Null);
    }
    (socket, ready_session_id)
}

fn patch(path: &str, body: Value) -> Request<Body> {
    Request::patch(path)
        .header("authorization", "test-password")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn get(path: &str) -> Request<Body> {
    Request::get(path)
        .header("authorization", "test-password")
        .body(Body::empty())
        .unwrap()
}

fn delete(path: &str) -> Request<Body> {
    Request::delete(path)
        .header("authorization", "test-password")
        .body(Body::empty())
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

fn voice_state(channel_id: &str) -> Value {
    json!({
        "token": "test-token",
        "endpoint": "voice.example.invalid",
        "sessionId": "test-session",
        "channelId": channel_id,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exception_and_stuck_payloads_follow_track_start_in_exact_order() {
    let harness = Harness::new(
        Arc::new(FakeMantle::default()),
        Arc::new(FakeVoiceBackend::new(32, 2)),
        Duration::from_secs(5),
    )
    .await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000801").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000801");

    let (status, _) = json_request(
        &harness.app,
        patch(
            &path,
            json!({
                "track": {
                    "identifier": "fixture:event-error",
                    "userData": {"request": "exception-order"},
                }
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let start = next_json(&mut socket).await;
    let exception = next_json(&mut socket).await;
    assert_eq!(start["type"], "TrackStartEvent");
    assert_eq!(exception["op"], "event");
    assert_eq!(exception["type"], "TrackExceptionEvent");
    assert_eq!(exception["guildId"], "800000000000000801");
    assert_eq!(exception["track"]["userData"]["request"], "exception-order");
    assert_eq!(
        exception["exception"],
        json!({
            "message": "synthetic playback failure",
            "severity": "fault",
            "cause": "synthetic playback failure",
            "causeStackTrace": "synthetic playback failure",
        })
    );
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");

    let (status, _) = json_request(
        &harness.app,
        patch(
            &path,
            json!({
                "track": {
                    "identifier": "fixture:event-stuck",
                    "userData": {"request": "stuck-order"},
                }
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let start = next_json(&mut socket).await;
    let stuck = next_json(&mut socket).await;
    assert_eq!(start["type"], "TrackStartEvent");
    assert_eq!(stuck["op"], "event");
    assert_eq!(stuck["type"], "TrackStuckEvent");
    assert_eq!(stuck["guildId"], "800000000000000801");
    assert_eq!(stuck["thresholdMs"], 5_000);
    assert_eq!(stuck["track"]["userData"]["request"], "stuck-order");
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_voice_close_emits_exact_event_then_disconnected_player_state() {
    let mantle = Arc::new(FakeMantle::default());
    let voice = Arc::new(FakeVoiceBackend::new(64, 4));
    let harness = Harness::new(mantle, Arc::clone(&voice), Duration::from_secs(5)).await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000802").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000802");

    let (status, player) = json_request(
        &harness.app,
        patch(
            &path,
            json!({
                "track": {"identifier": "fixture:voice-close"},
                "voice": voice_state("900000000000000802"),
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(player["state"]["connected"], true);
    assert_eq!(next_json(&mut socket).await["type"], "TrackStartEvent");
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");

    let first_connection = voice.latest_connection().unwrap();
    first_connection
        .try_push_event(VoiceEvent::Closed(VoiceClose {
            code: 4_015,
            reason: Arc::from("Discord voice server closed the connection"),
            by_remote: true,
        }))
        .unwrap();

    let closed = next_json(&mut socket).await;
    assert_eq!(
        closed,
        json!({
            "op": "event",
            "type": "WebSocketClosedEvent",
            "guildId": "800000000000000802",
            "code": 4_015,
            "reason": "Discord voice server closed the connection",
            "byRemote": true,
        })
    );
    let update = next_json(&mut socket).await;
    assert_eq!(update["op"], "playerUpdate");
    assert_eq!(update["state"]["connected"], false);
    assert_eq!(update["state"]["ping"], -1);

    let (_, player) = json_request(&harness.app, get(&path)).await;
    assert_eq!(player["state"]["connected"], false);
    assert_eq!(player["state"]["ping"], -1);
    assert!(voice.records().contains(&FakeVoiceRecord::StopAudio));

    // A terminal close removes only the transport. The player can install
    // fresh Discord voice information and gets a new monitored connection.
    let (status, player) = json_request(
        &harness.app,
        patch(&path, json!({"voice": voice_state("900000000000000803")})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(player["state"]["connected"], true);
    assert_eq!(
        voice
            .records()
            .iter()
            .filter(|record| matches!(record, FakeVoiceRecord::Connect { .. }))
            .count(),
        2
    );

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audio_attachment_is_deferred_until_the_voice_generation_is_ready() {
    let voice = Arc::new(FakeVoiceBackend::new_with_phase(
        32,
        2,
        VoicePhase::PreparingDave,
    ));
    let harness = Harness::new(
        Arc::new(FakeMantle::default()),
        Arc::clone(&voice),
        Duration::from_secs(5),
    )
    .await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000810").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000810");

    let (status, player) = json_request(
        &harness.app,
        patch(
            &path,
            json!({
                "track": {"identifier": "fixture:deferred-dave"},
                "voice": voice_state("900000000000000810"),
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(player["state"]["connected"], false);
    assert_eq!(next_json(&mut socket).await["type"], "TrackStartEvent");
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");
    assert!(
        !voice
            .records()
            .iter()
            .any(|record| matches!(record, FakeVoiceRecord::SetSource { .. }))
    );

    let connection = voice.latest_connection().unwrap();
    connection.try_transition_to(VoicePhase::Connected).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !voice
            .records()
            .iter()
            .any(|record| matches!(record, FakeVoiceRecord::SetSource { .. }))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the ready transition did not attach the deferred Mantle source");
    assert!(connection.pull_frame().await.unwrap().is_some());

    let (_, player) = json_request(&harness.app, get(&path)).await;
    assert_eq!(player["state"]["connected"], true);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), socket.next())
            .await
            .is_err(),
        "a readiness transition leaked a terminal voice event"
    );

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_resumable_client_disconnect_explicitly_shuts_down_player_voice() {
    let voice = Arc::new(FakeVoiceBackend::new(32, 2));
    let harness = Harness::new(
        Arc::new(FakeMantle::default()),
        Arc::clone(&voice),
        Duration::from_secs(5),
    )
    .await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000807").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000807");
    assert_eq!(
        json_request(
            &harness.app,
            patch(&path, json!({"voice": voice_state("900000000000000807")}),),
        )
        .await
        .0,
        StatusCode::OK
    );

    socket.close(None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !voice
            .records()
            .contains(&FakeVoiceRecord::ShutdownConnection)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("client disconnect did not explicitly tear down player voice");

    harness.stop().await;
    assert!(voice.records().contains(&FakeVoiceRecord::ShutdownBackend));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_voice_loss_during_resumable_disconnect_replays_once_in_order() {
    let voice = Arc::new(FakeVoiceBackend::new(32, 2));
    let harness = Harness::new(
        Arc::new(FakeMantle::default()),
        Arc::clone(&voice),
        Duration::from_secs(5),
    )
    .await;
    let user_id = "300000000000000808";
    let (mut socket, session_id) = open_session(harness.address, user_id).await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000808");
    assert_eq!(
        json_request(
            &harness.app,
            patch(&path, json!({"voice": voice_state("900000000000000808")}),),
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        json_request(
            &harness.app,
            patch(
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
    assert!(
        !voice
            .records()
            .contains(&FakeVoiceRecord::ShutdownConnection)
    );
    voice
        .latest_connection()
        .unwrap()
        .try_push_event(VoiceEvent::Closed(VoiceClose {
            code: 4_006,
            reason: Arc::from("resumable-session terminal voice close"),
            by_remote: true,
        }))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !voice.records().contains(&FakeVoiceRecord::StopAudio) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("voice-close event was not processed while the client was away");

    let (mut resumed, resumed_id) =
        open_session_with_id(harness.address, user_id, Some(&session_id)).await;
    assert_eq!(resumed_id, session_id);
    let closed = next_json(&mut resumed).await;
    assert_eq!(closed["type"], "WebSocketClosedEvent");
    assert_eq!(closed["code"], 4_006);
    let update = next_json(&mut resumed).await;
    assert_eq!(update["op"], "playerUpdate");
    assert_eq!(update["state"]["connected"], false);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), resumed.next())
            .await
            .is_err(),
        "resume replayed a duplicate player snapshot"
    );

    resumed.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_stop_finish_and_frame_failure_keep_causal_event_order() {
    let voice = Arc::new(FakeVoiceBackend::new(96, 2));
    let harness = Harness::new(
        Arc::new(FakeMantle::default()),
        Arc::clone(&voice),
        Duration::from_secs(5),
    )
    .await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000805").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000805");

    assert_eq!(
        json_request(
            &harness.app,
            patch(
                &path,
                json!({
                    "track": {
                        "identifier": "fixture:replace-one",
                        "userData": {"slot": "old"},
                    },
                    "voice": voice_state("900000000000000805"),
                }),
            ),
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(next_json(&mut socket).await["type"], "TrackStartEvent");
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");

    assert_eq!(
        json_request(
            &harness.app,
            patch(
                &path,
                json!({
                    "track": {
                        "identifier": "fixture:replace-two",
                        "userData": {"slot": "new"},
                    }
                }),
            ),
        )
        .await
        .0,
        StatusCode::OK
    );
    let replaced = next_json(&mut socket).await;
    let replacement_start = next_json(&mut socket).await;
    assert_eq!(replaced["type"], "TrackEndEvent");
    assert_eq!(replaced["reason"], "replaced");
    assert_eq!(replaced["track"]["userData"]["slot"], "old");
    assert_eq!(replacement_start["type"], "TrackStartEvent");
    assert_eq!(replacement_start["track"]["userData"]["slot"], "new");
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");

    assert_eq!(
        json_request(
            &harness.app,
            patch(&path, json!({"track": {"encoded": null}})),
        )
        .await
        .0,
        StatusCode::OK
    );
    let stopped = next_json(&mut socket).await;
    assert_eq!(stopped["type"], "TrackEndEvent");
    assert_eq!(stopped["reason"], "stopped");
    assert_eq!(stopped["track"]["userData"]["slot"], "new");

    assert_eq!(
        json_request(
            &harness.app,
            patch(&path, json!({"track": {"identifier": "fixture:short"}})),
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(next_json(&mut socket).await["type"], "TrackStartEvent");
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");
    let connection = voice.latest_connection().unwrap();
    assert!(connection.pull_frame().await.unwrap().is_some());
    assert!(connection.pull_frame().await.unwrap().is_some());
    assert!(connection.pull_frame().await.unwrap().is_none());
    let finished = next_json(&mut socket).await;
    assert_eq!(finished["type"], "TrackEndEvent");
    assert_eq!(finished["reason"], "finished");
    let finished_state = next_json(&mut socket).await;
    assert_eq!(finished_state["op"], "playerUpdate");
    assert_eq!(finished_state["state"]["position"], 0);

    assert_eq!(
        json_request(
            &harness.app,
            patch(
                &path,
                json!({"track": {"identifier": "fixture:frame-error"}}),
            ),
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(next_json(&mut socket).await["type"], "TrackStartEvent");
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");
    assert!(connection.pull_frame().await.is_err());
    let exception = next_json(&mut socket).await;
    let load_failed = next_json(&mut socket).await;
    assert_eq!(exception["type"], "TrackExceptionEvent");
    assert_eq!(load_failed["type"], "TrackEndEvent");
    assert_eq!(load_failed["reason"], "loadFailed");

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn player_destroy_delivers_cleanup_before_tearing_down_voice() {
    let voice = Arc::new(FakeVoiceBackend::new(32, 2));
    let harness = Harness::new(
        Arc::new(FakeMantle::default()),
        Arc::clone(&voice),
        Duration::from_secs(5),
    )
    .await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000811").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000811");

    assert_eq!(
        json_request(
            &harness.app,
            patch(
                &path,
                json!({
                    "track": {
                        "identifier": "fixture:cleanup",
                        "userData": {"reason": "player-delete"},
                    },
                    "voice": voice_state("900000000000000811"),
                }),
            ),
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(next_json(&mut socket).await["type"], "TrackStartEvent");
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");

    let response = harness.app.clone().oneshot(delete(&path)).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let cleanup = next_json(&mut socket).await;
    assert_eq!(cleanup["type"], "TrackEndEvent");
    assert_eq!(cleanup["reason"], "cleanup");
    assert_eq!(cleanup["track"]["userData"]["reason"], "player-delete");
    assert!(
        voice
            .records()
            .contains(&FakeVoiceRecord::ShutdownConnection)
    );
    assert_eq!(
        harness
            .app
            .clone()
            .oneshot(get(&path))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_player_updates_refresh_latest_state_without_overlapping_a_player() {
    let mantle = Arc::new(FakeMantle::default());
    let voice = Arc::new(FakeVoiceBackend::new(64, 2));
    let interval = Duration::from_millis(60);
    let harness = Harness::new(Arc::clone(&mantle), Arc::clone(&voice), interval).await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000804").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000804");

    let (status, _) = json_request(
        &harness.app,
        patch(
            &path,
            json!({
                "track": {"identifier": "fixture:periodic"},
                "voice": voice_state("900000000000000804"),
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(next_json(&mut socket).await["type"], "TrackStartEvent");
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");

    let connection = voice.latest_connection().unwrap();
    connection.set_ping(Some(Duration::from_millis(41)));
    mantle.clock().advance_ms(321);
    let update = next_json(&mut socket).await;
    assert_eq!(update["op"], "playerUpdate");
    assert_eq!(update["guildId"], "800000000000000804");
    assert_eq!(update["state"]["position"], 321);
    assert_eq!(update["state"]["connected"], true);
    assert_eq!(update["state"]["ping"], 41);

    mantle.hold_snapshots();
    let calls_before_hold = mantle.snapshot_calls();
    tokio::time::timeout(Duration::from_secs(1), async {
        while mantle.active_snapshots() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("periodic refresh did not start");
    tokio::time::sleep(interval * 3).await;
    assert_eq!(mantle.active_snapshots(), 1);
    assert_eq!(mantle.snapshot_calls(), calls_before_hold + 1);
    assert_eq!(mantle.maximum_active_snapshots(), 1);

    mantle.clock().advance_ms(100);
    connection.set_ping(Some(Duration::from_millis(52)));
    let released_at = Instant::now();
    mantle.release_snapshots();
    let update = next_json(&mut socket).await;
    assert!(released_at.elapsed() < Duration::from_secs(1));
    assert_eq!(update["state"]["position"], 421);
    assert_eq!(update["state"]["ping"], 52);

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_players_do_not_create_periodic_refresh_work() {
    let mantle = Arc::new(FakeMantle::default());
    let interval = Duration::from_millis(20);
    let harness = Harness::new(
        Arc::clone(&mantle),
        Arc::new(FakeVoiceBackend::new(16, 1)),
        interval,
    )
    .await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000809").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000809");
    assert_eq!(
        json_request(&harness.app, patch(&path, json!({}))).await.0,
        StatusCode::OK
    );
    let calls = mantle.snapshot_calls();
    assert!(
        tokio::time::timeout(interval * 3, socket.next())
            .await
            .is_err(),
        "idle player unexpectedly emitted a periodic update"
    );
    assert_eq!(mantle.snapshot_calls(), calls);

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[cfg(feature = "oto-voice")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_oto_backend_carries_mantle_opus_through_rest_controls_to_udp() {
    let udp = FakeUdpServer::start(FakeUdpServerConfig::localhost(ManualClock::new(
        Duration::ZERO,
    )))
    .await
    .unwrap();
    let mut gateway_config = FakeVoiceGatewayConfig::local();
    gateway_config.voice_ip = udp.local_addr().ip().to_string();
    gateway_config.voice_port = udp.local_addr().port();
    let gateway = FakeVoiceGateway::start(gateway_config).await.unwrap();
    let oto = Oto::builder()
        .test_tls_config(gateway.tls().client_config())
        .build()
        .unwrap();
    let voice: Arc<dyn VoiceBackend> = Arc::new(OtoVoiceBackend::new(oto, 2, 1));
    let mantle: Arc<dyn MantleAdapter> = Arc::new(FakeMantle::default());

    let mut config = ServerConfig::default()
        .with_password("test-password")
        .unwrap();
    config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
    config.port = 0;
    config.max_players = 2;
    config.player_executor_shards = 1;
    config.player_command_capacity = 16;
    let server = CrustServer::bind_with_backends(config, mantle, voice)
        .await
        .unwrap();
    let address = server.local_address().unwrap();
    let app = server.router();
    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.clone();
    let task = tokio::spawn(async move { server.serve(task_shutdown).await });
    let harness = Harness {
        app,
        address,
        shutdown,
        task,
    };

    let (mut socket, session_id) = open_session(harness.address, "300000000000000806").await;
    let path = format!("/v4/sessions/{session_id}/players/800000000000000806");
    let live_voice = json!({
        "token": "local-voice-token",
        "endpoint": format!("localhost:{}", gateway.local_addr().port()),
        "sessionId": "local-voice-session",
        "channelId": "900000000000000806",
    });
    let (status, player) = json_request(
        &harness.app,
        patch(
            &path,
            json!({
                "track": {"identifier": "fixture:oto-e2e"},
                "voice": live_voice,
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(player["state"]["connected"], true);
    assert_eq!(next_json(&mut socket).await["type"], "TrackStartEvent");
    assert_eq!(next_json(&mut socket).await["op"], "playerUpdate");
    tokio::time::timeout(Duration::from_secs(2), async {
        while udp.capture().len() < 3 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("Oto did not pace Mantle Opus to the UDP peer");
    assert!(!gateway.speaking().is_empty());

    let packets_before_pause = udp.capture().len();
    assert_eq!(
        json_request(&harness.app, patch(&path, json!({"paused": true})))
            .await
            .0,
        StatusCode::OK
    );
    let packets_after_pause = udp.capture().len();
    assert!(packets_after_pause >= packets_before_pause);
    assert!(packets_after_pause <= packets_before_pause + 6);

    assert_eq!(
        json_request(
            &harness.app,
            patch(&path, json!({"position": 200, "filters": {"volume": 0.75}}),),
        )
        .await
        .0,
        StatusCode::OK
    );
    let filtered_update = next_json(&mut socket).await;
    assert_eq!(filtered_update["op"], "playerUpdate");
    assert_eq!(filtered_update["state"]["position"], 200);

    assert_eq!(
        json_request(&harness.app, patch(&path, json!({"paused": false})))
            .await
            .0,
        StatusCode::OK
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while udp.capture().len() <= packets_after_pause + 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("resumed playback did not restart the Oto-paced source");

    socket.close(None).await.unwrap();
    harness.stop().await;
    gateway.shutdown().await.unwrap();
    udp.shutdown().await.unwrap();
}
