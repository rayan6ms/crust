use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode};
use crust::media::MantleAdapter;
use crust_server::CrustServer;
use crust_server::config::ServerConfig;
use crust_testkit::FakeMantle;
use serde_json::{Value, json};
use tower::ServiceExt;

async fn application(fake: Arc<FakeMantle>, media_capacity: usize) -> Router {
    application_with_capacities(fake, media_capacity, media_capacity).await
}

async fn application_with_capacities(
    fake: Arc<FakeMantle>,
    load_capacity: usize,
    source_capacity: usize,
) -> Router {
    let mut config = ServerConfig::default()
        .with_password("test-password")
        .unwrap();
    config.port = 0;
    config.max_concurrent_loads = load_capacity;
    config.max_concurrent_source_requests = source_capacity;
    let adapter: Arc<dyn MantleAdapter> = fake;
    CrustServer::bind_with_adapter(config, adapter)
        .await
        .unwrap()
        .router()
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

fn post(path: &str, body: impl Into<Body>) -> Request<Body> {
    Request::post(path)
        .header("authorization", "test-password")
        .header("content-type", "application/json")
        .body(body.into())
        .unwrap()
}

fn assert_track(track: &Value, identifier: &str) {
    assert_eq!(track["encoded"], format!("fake-v1:{identifier}"));
    assert_eq!(
        track["info"],
        json!({
            "identifier": identifier,
            "isSeekable": true,
            "author": "Crust testkit",
            "length": 10_000,
            "isStream": false,
            "position": 0,
            "title": format!("Synthetic {identifier}"),
            "uri": null,
            "sourceName": "fixture",
            "artworkUrl": null,
            "isrc": null,
        })
    );
    assert_eq!(
        track["pluginInfo"],
        json!({"fixture":{"identifier":identifier,"nested":[1,null,true]}})
    );
    assert_eq!(track["userData"], json!({}));
}

#[tokio::test]
async fn load_result_variants_match_lavalink_shapes_and_preserve_opaque_json() {
    let fake = Arc::new(FakeMantle::default());
    let app = application(Arc::clone(&fake), 4).await;

    let (status, missing) = json_response(request(&app, get("/v4/loadtracks")).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        missing["message"],
        "Required parameter 'identifier' is not present."
    );
    assert_eq!(missing["path"], "/v4/loadtracks");
    assert!(missing.get("trace").is_none());

    let (status, traced) =
        json_response(request(&app, get("/v4/loadtracks?trace=true")).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        traced["trace"]
            .as_str()
            .is_some_and(|trace| !trace.is_empty())
    );

    let (status, empty) =
        json_response(request(&app, get("/v4/loadtracks?identifier=fixture%3Anone")).await).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(empty, json!({"loadType":"empty","data":null}));

    let (status, loaded) =
        json_response(request(&app, get("/v4/loadtracks?identifier=fixture%3Atrack")).await).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(loaded["loadType"], "track");
    assert_track(&loaded["data"], "fixture:track");

    let (status, search) =
        json_response(request(&app, get("/v4/loadtracks?identifier=fixture%3Asearch")).await).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(search["loadType"], "search");
    assert_eq!(search["data"].as_array().unwrap().len(), 2);
    assert_track(&search["data"][0], "fixture:search/one");
    assert_track(&search["data"][1], "fixture:search/two");

    let (status, playlist) =
        json_response(request(&app, get("/v4/loadtracks?identifier=fixture%3Aplaylist")).await)
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(playlist["loadType"], "playlist");
    assert_eq!(
        playlist["data"]["info"],
        json!({"name":"Synthetic playlist","selectedTrack":0})
    );
    assert_eq!(
        playlist["data"]["pluginInfo"],
        json!({"type":"fixture-playlist"})
    );
    assert_eq!(playlist["data"]["tracks"].as_array().unwrap().len(), 2);

    let (status, unselected) = json_response(
        request(
            &app,
            get("/v4/loadtracks?identifier=fixture%3Aplaylist-none"),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(unselected["data"]["info"]["selectedTrack"], -1);
    assert_eq!(unselected["data"]["pluginInfo"], json!({}));

    let (status, failure) =
        json_response(request(&app, get("/v4/loadtracks?identifier=fixture%3Aload-error")).await)
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(failure["loadType"], "error");
    assert_eq!(failure["data"]["message"], "synthetic load failure");
    assert_eq!(failure["data"]["severity"], "suspicious");
    assert!(failure["data"]["cause"].is_string());
    assert!(failure["data"]["causeStackTrace"].is_string());

    for (encoded, decoded) in [
        (
            "ytsearch%3Aforward%20unchanged",
            "ytsearch:forward unchanged",
        ),
        (
            "ytmsearch%3Aforward%20unchanged",
            "ytmsearch:forward unchanged",
        ),
        (
            "scsearch%3Aforward%20unchanged",
            "scsearch:forward unchanged",
        ),
    ] {
        let (_, prefixed) = json_response(
            request(&app, get(&format!("/v4/loadtracks?identifier={encoded}"))).await,
        )
        .await;
        assert_eq!(prefixed, json!({"loadType":"empty","data":null}));
        assert_eq!(fake.last_identifier().as_deref(), Some(decoded));
    }
}

#[tokio::test]
async fn single_and_batch_decode_match_alias_precedence_and_error_contracts() {
    let app = application(Arc::new(FakeMantle::default()), 4).await;

    let (status, missing) = json_response(request(&app, get("/v4/decodetrack")).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(missing["message"], "No track to decode provided");

    let (status, legacy) = json_response(
        request(
            &app,
            get("/v4/decodetrack?track=fake-v1%3Afixture%3Alegacy"),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_track(&legacy, "fixture:legacy");

    let (status, preferred) = json_response(
        request(
            &app,
            get("/v4/decodetrack?encodedTrack=fake-v1%3Afixture%3Apreferred&track=invalid"),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_track(&preferred, "fixture:preferred");

    let invalid = request(&app, get("/v4/decodetrack?encodedTrack=invalid")).await;
    assert_eq!(invalid.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let (status, empty) = json_response(request(&app, post("/v4/decodetracks", "[]")).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(empty["message"], "No tracks to decode provided");

    for malformed in ["", "null", "{}", "[1]", "not-json"] {
        let (status, body) =
            json_response(request(&app, post("/v4/decodetracks", malformed.to_owned())).await)
                .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["message"], "Invalid encoded tracks");
    }

    let encoded = serde_json::to_vec(&json!([
        "fake-v1:fixture:batch-one",
        "fake-v1:fixture:batch-two"
    ]))
    .unwrap();
    let (status, decoded) =
        json_response(request(&app, post("/v4/decodetracks", encoded)).await).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(decoded.as_array().unwrap().len(), 2);
    assert_track(&decoded[0], "fixture:batch-one");
    assert_track(&decoded[1], "fixture:batch-two");

    let invalid_batch = serde_json::to_vec(&json!(["fake-v1:fixture:valid", "invalid"])).unwrap();
    assert_eq!(
        request(&app, post("/v4/decodetracks", invalid_batch))
            .await
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
async fn media_admission_is_bounded_and_recovers_after_request_cancellation() {
    let fake = Arc::new(FakeMantle::default());
    fake.hold_loads();
    let app = application(Arc::clone(&fake), 1).await;
    let held_app = app.clone();
    let held = tokio::spawn(async move {
        request(&held_app, get("/v4/loadtracks?identifier=fixture%3Aheld")).await
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        while fake.active_loads() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let (status, overloaded) =
        json_response(request(&app, get("/v4/loadtracks?identifier=fixture%3Aoverloaded")).await)
            .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(overloaded["message"], "Concurrent load capacity reached");

    held.abort();
    assert!(held.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while fake.active_loads() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    fake.release_loads();
    let (status, recovered) =
        json_response(request(&app, get("/v4/loadtracks?identifier=fixture%3Arecovered")).await)
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(recovered["loadType"], "track");
}

#[tokio::test]
async fn source_request_admission_is_an_independent_nonblocking_bound() {
    let fake = Arc::new(FakeMantle::default());
    fake.hold_loads();
    let app = application_with_capacities(Arc::clone(&fake), 2, 1).await;
    let held_app = app.clone();
    let held = tokio::spawn(async move {
        request(
            &held_app,
            get("/v4/loadtracks?identifier=fixture%3Asource-held"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while fake.active_loads() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let (status, overloaded) = json_response(
        request(
            &app,
            get("/v4/loadtracks?identifier=fixture%3Asource-overloaded"),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(overloaded["message"], "Source request capacity reached");

    held.abort();
    let _ = held.await;
    fake.release_loads();
}

#[tokio::test]
async fn decode_batch_obeys_the_configured_body_limit() {
    let mut config = ServerConfig::default()
        .with_password("test-password")
        .unwrap();
    config.port = 0;
    config.max_request_body_bytes = 8;
    let adapter: Arc<dyn MantleAdapter> = Arc::new(FakeMantle::default());
    let app = CrustServer::bind_with_adapter(config, adapter)
        .await
        .unwrap()
        .router();
    let (status, body) = json_response(
        request(
            &app,
            post("/v4/decodetracks", "[\"fake-v1:fixture:too-long\"]"),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["message"], "Request body exceeds the configured limit");
}

#[tokio::test]
async fn decode_batch_rejects_too_many_tracks_before_adapter_work() {
    let mut config = ServerConfig::default()
        .with_password("test-password")
        .unwrap();
    config.port = 0;
    config.max_batch_decode_tracks = 1;
    let adapter: Arc<dyn MantleAdapter> = Arc::new(FakeMantle::default());
    let app = CrustServer::bind_with_adapter(config, adapter)
        .await
        .unwrap()
        .router();
    let (status, body) = json_response(
        request(
            &app,
            post(
                "/v4/decodetracks",
                "[\"fake-v1:fixture:one\",\"fake-v1:fixture:two\"]",
            ),
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["message"], "Too many tracks to decode");
}

#[tokio::test]
async fn media_routes_fail_closed_when_no_adapter_is_installed() {
    let mut config = ServerConfig::default()
        .with_password("test-password")
        .unwrap();
    config.port = 0;
    let app = CrustServer::bind(config).await.unwrap().router();
    for request_without_adapter in [
        get("/v4/loadtracks?identifier=fixture%3Atrack"),
        get("/v4/decodetrack?encodedTrack=fake-v1%3Afixture%3Atrack"),
    ] {
        let (status, body) = json_response(request(&app, request_without_adapter).await).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["message"], "Mantle adapter unavailable");
    }
}
