use std::collections::BTreeSet;

use crust::resources::RESOURCE_LIMIT_KEYS;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Matrix {
    schema_version: u32,
    phase: String,
    resource_limit_keys: Vec<String>,
    points: Vec<Point>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Point {
    id: String,
    implementation_phase: String,
    capacity_key: Option<String>,
    queued: bool,
    message_class: Option<String>,
    full_behavior: String,
    shutdown_behavior: String,
    metric: String,
    structured_log: String,
    retry: String,
}

fn matrix() -> Matrix {
    serde_json::from_str(include_str!("fixtures/overload.json")).unwrap()
}

#[test]
fn resource_keys_and_overload_points_are_complete_and_explicit() {
    let matrix = matrix();
    assert_eq!(matrix.schema_version, 1);
    assert_eq!(matrix.phase, "P03");
    assert_eq!(matrix.resource_limit_keys, RESOURCE_LIMIT_KEYS.to_vec());
    let known = matrix
        .resource_limit_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut ids = BTreeSet::new();
    for point in &matrix.points {
        assert!(ids.insert(point.id.as_str()), "duplicate {}", point.id);
        if let Some(capacity) = &point.capacity_key {
            assert!(known.contains(capacity.as_str()), "unknown {capacity}");
        }
        assert!(!point.full_behavior.is_empty());
        assert!(!point.shutdown_behavior.is_empty());
        assert!(!point.metric.is_empty());
        assert!(!point.structured_log.is_empty());
        assert!(!point.retry.is_empty());
        if point.queued {
            assert!(
                point.message_class.is_some(),
                "{} has unclassified queue",
                point.id
            );
        }
    }
    for required in [
        "session-admission",
        "player-admission",
        "concurrent-load-admission",
        "batch-decode-bound",
        "player-command-mailbox",
        "websocket-critical-events",
        "websocket-latest-state",
        "best-effort-telemetry",
        "session-resume-admission",
        "source-request-admission",
        "voice-connect-admission",
        "fake-media-critical-events",
        "media-frame-pull",
        "owned-long-lived-tasks",
        "plugin-calls",
    ] {
        assert!(ids.contains(required), "missing {required}");
    }
}

#[test]
fn only_current_primitives_claim_p03_implementation() {
    let actual = matrix()
        .points
        .into_iter()
        .filter(|point| point.implementation_phase == "P03")
        .map(|point| point.id)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual,
        [
            "best-effort-telemetry",
            "fake-media-critical-events",
            "media-frame-pull",
            "owned-long-lived-tasks",
            "player-command-mailbox",
            "websocket-latest-state",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );
}
