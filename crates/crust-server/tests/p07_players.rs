use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode};
use crust::media::MantleAdapter;
use crust_server::CrustServer;
use crust_server::config::ServerConfig;
use crust_testkit::FakeMantle;
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
    async fn new(fake: Arc<FakeMantle>, shards: usize) -> Self {
        let mut config = ServerConfig::default()
            .with_password("test-password")
            .unwrap();
        config.listen_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        config.port = 0;
        config.max_sessions = 16;
        config.max_players = 32;
        config.player_executor_shards = shards;
        config.player_command_capacity = 16;
        let adapter: Arc<dyn MantleAdapter> = fake;
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
        .insert("client-name", "Crust/P07".parse().unwrap());
    if let Some(session_id) = session_id {
        request
            .headers_mut()
            .insert("session-id", session_id.parse().unwrap());
    }
    let (mut socket, _) = connect_async(request).await.unwrap();
    let ready = socket.next().await.unwrap().unwrap().into_text().unwrap();
    let ready: Value = serde_json::from_str(&ready).unwrap();
    let session_id = ready["sessionId"].as_str().unwrap().to_owned();
    (socket, session_id)
}

async fn request(app: &Router, request: Request<Body>) -> Response<Body> {
    app.clone().oneshot(request).await.unwrap()
}

async fn json_response(response: Response<Body>) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn get(path: &str) -> Request<Body> {
    Request::get(path)
        .header("authorization", "test-password")
        .body(Body::empty())
        .unwrap()
}

fn patch(path: &str, body: Value) -> Request<Body> {
    Request::patch(path)
        .header("authorization", "test-password")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn delete(path: &str) -> Request<Body> {
    Request::delete(path)
        .header("authorization", "test-password")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn player_crud_and_patch_null_semantics_match_the_frozen_corpus() {
    let harness = Harness::new(Arc::new(FakeMantle::default()), 2).await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000701").await;
    let players_path = format!("/v4/sessions/{session_id}/players");

    assert_eq!(
        json_response(request(&harness.app, get(&players_path)).await).await,
        (StatusCode::OK, json!([]))
    );
    assert_eq!(
        request(&harness.app, get(&format!("{players_path}/800")))
            .await
            .status(),
        StatusCode::NOT_FOUND
    );

    let (status, player) = json_response(
        request(
            &harness.app,
            patch(&format!("{players_path}/800"), json!({})),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(player["guildId"], "800");
    assert_eq!(player["track"], Value::Null);
    assert_eq!(player["volume"], 100);
    assert_eq!(player["paused"], false);
    assert_eq!(player["filters"], json!({}));
    assert_eq!(
        player["voice"],
        json!({"token":"", "endpoint":"", "sessionId":"", "channelId":null})
    );

    for body in [
        json!({"encodedTrack": null}),
        json!({"endTime": null}),
        json!({"track": {"encoded": null}}),
    ] {
        assert_eq!(
            request(&harness.app, patch(&format!("{players_path}/800"), body))
                .await
                .status(),
            StatusCode::OK
        );
    }
    for (guild, body) in [
        ("804", json!({"track": null})),
        ("805", json!({"volume": null})),
        (
            "806",
            json!({"track":{"encoded":null}, "encodedTrack":null}),
        ),
        (
            "807",
            json!({"track":{"encoded":"invalid", "identifier":"both"}}),
        ),
    ] {
        assert_eq!(
            request(
                &harness.app,
                patch(&format!("{players_path}/{guild}"), body)
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
    }

    let (_, filtered) = json_response(
        request(
            &harness.app,
            patch(
                &format!("{players_path}/808"),
                json!({
                    "filters": {
                        "karaoke": null,
                        "pluginFilters": {"unknown.p07":{"x":[1,null,true]}}
                    }
                }),
            ),
        )
        .await,
    )
    .await;
    assert!(filtered["filters"].get("karaoke").is_none());
    assert_eq!(
        filtered["filters"]["pluginFilters"],
        json!({"unknown.p07":{"x":[1,null,true]}})
    );

    let (_, players) = json_response(request(&harness.app, get(&players_path)).await).await;
    assert_eq!(players.as_array().unwrap().len(), 2);
    assert_eq!(players[0]["guildId"], "800");
    assert_eq!(players[1]["guildId"], "808");

    assert_eq!(
        request(&harness.app, delete(&format!("{players_path}/999")))
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        request(&harness.app, delete(&format!("{players_path}/800")))
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        request(&harness.app, get(&format!("{players_path}/800")))
            .await
            .status(),
        StatusCode::NOT_FOUND
    );

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn playback_no_replace_controls_and_track_start_order_use_fake_mantle() {
    let fake = Arc::new(FakeMantle::default());
    let harness = Harness::new(Arc::clone(&fake), 1).await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000702").await;
    let path = format!("/v4/sessions/{session_id}/players/701");

    let response = request(
        &harness.app,
        patch(
            &path,
            json!({
                "track": {
                    "identifier": "fixture:one",
                    "userData": {"request":"p07", "nested":[1,null,true]}
                },
                "position": 20,
                "endTime": 9000,
                "paused": true
            }),
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let event: Value = serde_json::from_str(
        socket
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_eq!(event["type"], "TrackStartEvent");
    assert_eq!(event["track"]["info"]["identifier"], "fixture:one");
    assert_eq!(event["track"]["userData"]["request"], "p07");
    let initial_update: Value = serde_json::from_str(
        socket
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_eq!(initial_update["op"], "playerUpdate");
    assert_eq!(initial_update["state"]["position"], 20);

    let (_, player) = json_response(response).await;
    assert_eq!(player["track"]["info"]["identifier"], "fixture:one");
    assert_eq!(player["state"]["position"], 20);
    assert_eq!(player["paused"], true);

    let (_, no_replace) = json_response(
        request(
            &harness.app,
            patch(
                &format!("{path}?noReplace=true"),
                json!({"track":{"identifier":"fixture:two"}, "volume":77}),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(no_replace["track"]["info"]["identifier"], "fixture:one");
    assert_eq!(no_replace["volume"], 77);

    let (_, controlled) = json_response(
        request(
            &harness.app,
            patch(
                &path,
                json!({
                    "paused": false,
                    "position": 42,
                    "endTime": null,
                    "voice": {
                        "token":"token", "endpoint":"voice.example",
                        "sessionId":"voice-session", "channelId":"42"
                    }
                }),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(controlled["paused"], false);
    assert_eq!(controlled["state"]["position"], 42);
    assert_eq!(controlled["voice"]["channelId"], "42");
    let update: Value = serde_json::from_str(
        socket
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_eq!(update["op"], "playerUpdate");
    assert_eq!(update["guildId"], "701");

    let (_, stopped) = json_response(
        request(
            &harness.app,
            patch(&path, json!({"track":{"encoded":null}})),
        )
        .await,
    )
    .await;
    assert_eq!(stopped["track"], Value::Null);
    let stopped_event: Value = serde_json::from_str(
        socket
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_eq!(stopped_event["type"], "TrackEndEvent");
    assert_eq!(stopped_event["reason"], "stopped");

    let (_, encoded) = json_response(
        request(
            &harness.app,
            patch(
                &path,
                json!({"track":{"encoded":"fake-v1:fixture:encoded"}}),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(encoded["track"]["info"]["identifier"], "fixture:encoded");
    let encoded_start: Value = serde_json::from_str(
        socket
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_eq!(encoded_start["type"], "TrackStartEvent");

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_slow_guild_does_not_block_its_shard_and_same_guild_commands_serialize() {
    let fake = Arc::new(FakeMantle::default());
    fake.hold_loads();
    let harness = Harness::new(Arc::clone(&fake), 1).await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000703").await;
    let slow_path = format!("/v4/sessions/{session_id}/players/711");
    let fast_path = format!("/v4/sessions/{session_id}/players/712");

    let slow_app = harness.app.clone();
    let slow_request = patch(&slow_path, json!({"track":{"identifier":"fixture:slow"}}));
    let slow = async move { request(&slow_app, slow_request).await };
    tokio::pin!(slow);
    tokio::select! {
        _ = &mut slow => panic!("held Mantle load completed unexpectedly"),
        () = tokio::time::sleep(Duration::from_millis(20)) => {}
    }

    let fast = tokio::time::timeout(
        Duration::from_millis(250),
        request(&harness.app, patch(&fast_path, json!({"volume":55}))),
    )
    .await
    .expect("unrelated guild was blocked by slow Mantle work");
    assert_eq!(fast.status(), StatusCode::OK);

    let later_app = harness.app.clone();
    let later_request = patch(&slow_path, json!({"volume":66, "position":7}));
    let later_same_guild = async move { request(&later_app, later_request).await };
    tokio::pin!(later_same_guild);
    tokio::select! {
        _ = &mut later_same_guild => panic!("same-guild command overtook held load"),
        () = tokio::time::sleep(Duration::from_millis(20)) => {}
    }
    fake.release_loads();
    assert_eq!(slow.await.status(), StatusCode::OK);
    assert_eq!(later_same_guild.await.status(), StatusCode::OK);

    let (_, player) = json_response(request(&harness.app, get(&slow_path)).await).await;
    assert_eq!(player["volume"], 66);
    assert_eq!(player["state"]["position"], 7);

    // The start event is retained as a critical session event throughout the
    // serialized updates and remains available to the connected client.
    let event: Value = serde_json::from_str(
        socket
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_eq!(event["type"], "TrackStartEvent");

    socket.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn player_survives_resume_and_replays_its_latest_state_snapshot() {
    let harness = Harness::new(Arc::new(FakeMantle::default()), 1).await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000704").await;
    let player_path = format!("/v4/sessions/{session_id}/players/721");
    assert_eq!(
        request(
            &harness.app,
            patch(
                &player_path,
                json!({"track":{"identifier":"fixture:resume"}, "volume":61}),
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let start: Value = serde_json::from_str(
        socket
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_eq!(start["type"], "TrackStartEvent");
    assert_eq!(
        request(
            &harness.app,
            patch(
                &format!("/v4/sessions/{session_id}"),
                json!({"resuming":true, "timeout":15}),
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    socket.close(None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    let (mut resumed, resumed_id) =
        open_session_with_id(harness.address, "300000000000000704", Some(&session_id)).await;
    assert_eq!(resumed_id, session_id);
    let replay: Value = serde_json::from_str(
        resumed
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .as_ref(),
    )
    .unwrap();
    assert_eq!(replay["op"], "playerUpdate");
    assert_eq!(replay["guildId"], "721");

    let (_, player) = json_response(request(&harness.app, get(&player_path)).await).await;
    assert_eq!(player["track"]["info"]["identifier"], "fixture:resume");
    assert_eq!(player["volume"], 61);

    resumed.close(None).await.unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filter_replacements_apply_defaults_null_removal_and_validation() {
    let harness = Harness::new(Arc::new(FakeMantle::default()), 1).await;
    let (mut socket, session_id) = open_session(harness.address, "300000000000000709").await;
    let path = format!("/v4/sessions/{session_id}/players/790");

    let (status, configured) = json_response(
        request(
            &harness.app,
            patch(
                &path,
                json!({
                    "filters": {
                        "volume": 0.8,
                        "equalizer": [{"band": 0, "gain": 0.2}, {"band": 0, "gain": 0.3}],
                        "karaoke": {},
                        "timescale": {"speed": 1.25},
                        "tremolo": {"depth": 0.25},
                        "vibrato": {"frequency": 4.0},
                        "rotation": {"rotationHz": 0.5},
                        "distortion": {"scale": 0.75},
                        "channelMix": {"leftToRight": 0.2},
                        "lowPass": {},
                        "pluginFilters": {"opaque.vendor": {"nested": [1, null, true]}}
                    }
                }),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let filters = &configured["filters"];
    assert_eq!(filters["volume"], json!(0.8));
    assert_eq!(filters["karaoke"]["filterBand"], 220.0);
    assert_eq!(filters["timescale"]["pitch"], 1.0);
    assert_eq!(filters["tremolo"]["frequency"], 2.0);
    assert_eq!(filters["vibrato"]["depth"], 0.5);
    assert_eq!(filters["distortion"]["sinScale"], 1.0);
    assert_eq!(filters["channelMix"]["leftToLeft"], 1.0);
    assert_eq!(filters["lowPass"]["smoothing"], 20.0);
    assert_eq!(
        filters["pluginFilters"]["opaque.vendor"],
        json!({"nested": [1, null, true]})
    );

    let (_, removed) = json_response(
        request(
            &harness.app,
            patch(
                &path,
                json!({"filters": {"karaoke": null, "timescale": null}}),
            ),
        )
        .await,
    )
    .await;
    assert_eq!(removed["filters"], json!({}));

    for invalid in [
        json!({"filters": null}),
        json!({"filters": {"volume": null}}),
        json!({"filters": {"equalizer": null}}),
        json!({"filters": {"equalizer": [{"band": 15, "gain": 0.1}]}}),
        json!({"filters": {"volume": 5.1}}),
        json!({"filters": {"timescale": {"speed": 0.0}}}),
        json!({"filters": {"timescale": {"speed": 0.1}}}),
        json!({"filters": {"timescale": {"speed": 129.0}}}),
        json!({"filters": {"vibrato": {"frequency": 14.1}}}),
    ] {
        assert_eq!(
            request(&harness.app, patch(&path, invalid)).await.status(),
            StatusCode::BAD_REQUEST
        );
    }

    socket.close(None).await.unwrap();
    harness.stop().await;
}
