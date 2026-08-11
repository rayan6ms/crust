use std::fmt;

use crust::media::{
    AdapterErrorKind, EncodedTrack, FrameFormat, LoadOutcome, LoadRequest, MantleAdapter,
    MediaEvent, MediaTrack, PlayerStatus, ProcessingMode, SourceRoute, TrackEndReason,
};
use tokio_util::sync::CancellationToken;

pub const ADAPTER_CONFORMANCE_CHECKS: &[&str] = &[
    "load-track",
    "load-search",
    "load-playlist",
    "load-none",
    "load-error",
    "track-round-trip",
    "invalid-track",
    "play-start-event",
    "pause-seek-position",
    "pull-frame-processing-hook",
    "replace-order",
    "stop-event",
    "finished-event",
    "error-stuck-events",
    "cancellation",
    "shutdown",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterConformanceReport {
    pub checks: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterConformanceFailure {
    pub check: &'static str,
    pub detail: String,
}

impl fmt::Display for AdapterConformanceFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.check, self.detail)
    }
}

impl std::error::Error for AdapterConformanceFailure {}

fn failure(check: &'static str, detail: impl Into<String>) -> AdapterConformanceFailure {
    AdapterConformanceFailure {
        check,
        detail: detail.into(),
    }
}

fn request(identifier: &str) -> LoadRequest {
    LoadRequest {
        identifier: identifier.into(),
        route: SourceRoute::default(),
    }
}

async fn track(
    adapter: &dyn MantleAdapter,
    identifier: &str,
) -> Result<MediaTrack, AdapterConformanceFailure> {
    match adapter
        .load(request(identifier), CancellationToken::new())
        .await
        .map_err(|error| failure("load-track", error.to_string()))?
    {
        LoadOutcome::Track(track) => Ok(track),
        other => Err(failure("load-track", format!("unexpected {other:?}"))),
    }
}

/// Runs the contract that P04 must reuse for the real Mantle adapter.
pub async fn run_adapter_conformance(
    adapter: &dyn MantleAdapter,
) -> Result<AdapterConformanceReport, AdapterConformanceFailure> {
    let mut checks = Vec::new();
    let loaded = track(adapter, "fixture:track").await?;
    if loaded.metadata.source_name != "fixture" || loaded.plugin_info.is_empty() {
        return Err(failure("load-track", "metadata/pluginInfo not preserved"));
    }
    checks.push("load-track");

    let search = adapter
        .load(request("fixture:search"), CancellationToken::new())
        .await
        .map_err(|error| failure("load-search", error.to_string()))?;
    if !matches!(search, LoadOutcome::Search(ref tracks) if tracks.len() == 2) {
        return Err(failure("load-search", format!("unexpected {search:?}")));
    }
    checks.push("load-search");

    let playlist = adapter
        .load(request("fixture:playlist"), CancellationToken::new())
        .await
        .map_err(|error| failure("load-playlist", error.to_string()))?;
    if !matches!(playlist, LoadOutcome::Playlist { ref tracks, .. } if tracks.len() == 2) {
        return Err(failure("load-playlist", format!("unexpected {playlist:?}")));
    }
    checks.push("load-playlist");

    let none = adapter
        .load(request("fixture:none"), CancellationToken::new())
        .await
        .map_err(|error| failure("load-none", error.to_string()))?;
    if none != LoadOutcome::NoMatches {
        return Err(failure("load-none", format!("unexpected {none:?}")));
    }
    checks.push("load-none");

    let load_error = adapter
        .load(request("fixture:load-error"), CancellationToken::new())
        .await
        .expect_err("synthetic error must fail");
    if load_error.kind != AdapterErrorKind::LoadFailed {
        return Err(failure("load-error", format!("unexpected {load_error:?}")));
    }
    checks.push("load-error");

    let encoded = adapter
        .encode(loaded.clone(), CancellationToken::new())
        .await
        .map_err(|error| failure("track-encode", error.to_string()))?;
    let decoded = adapter
        .decode(encoded.clone(), CancellationToken::new())
        .await
        .map_err(|error| failure("track-decode", error.to_string()))?;
    if decoded != loaded || encoded != loaded.encoded {
        return Err(failure(
            "track-round-trip",
            "track changed during round trip",
        ));
    }
    checks.push("track-round-trip");

    let invalid = adapter
        .decode(EncodedTrack::new("invalid"), CancellationToken::new())
        .await
        .expect_err("invalid encoding must fail");
    if invalid.kind != AdapterErrorKind::InvalidTrack {
        return Err(failure("invalid-track", format!("unexpected {invalid:?}")));
    }
    checks.push("invalid-track");

    let player = adapter
        .create_player(CancellationToken::new())
        .await
        .map_err(|error| failure("create-player", error.to_string()))?;
    player
        .play(loaded.clone(), CancellationToken::new())
        .await
        .map_err(|error| failure("play", error.to_string()))?;
    if player.next_event(CancellationToken::new()).await.unwrap()
        != Some(MediaEvent::TrackStart(loaded.clone()))
    {
        return Err(failure("play", "TrackStart event missing"));
    }
    checks.push("play-start-event");

    player.pause(true, CancellationToken::new()).await.unwrap();
    player.seek(2_500, CancellationToken::new()).await.unwrap();
    let snapshot = player.snapshot().await.unwrap();
    if snapshot.status != PlayerStatus::Paused || snapshot.position_ms != 2_500 {
        return Err(failure("pause-seek", format!("unexpected {snapshot:?}")));
    }
    checks.push("pause-seek-position");

    player.pause(false, CancellationToken::new()).await.unwrap();
    player
        .set_processing(ProcessingMode::Pcm, CancellationToken::new())
        .await
        .unwrap();
    let frame = player
        .next_frame(CancellationToken::new())
        .await
        .unwrap()
        .ok_or_else(|| failure("frame", "frame missing"))?;
    if frame.sequence != 0 || frame.duration_ms != 20 || frame.format != FrameFormat::PcmPlaceholder
    {
        return Err(failure("frame", format!("unexpected {frame:?}")));
    }
    checks.push("pull-frame-processing-hook");

    let replacement = track(adapter, "fixture:replacement").await?;
    player
        .play(replacement.clone(), CancellationToken::new())
        .await
        .unwrap();
    let replaced = player.next_event(CancellationToken::new()).await.unwrap();
    let started = player.next_event(CancellationToken::new()).await.unwrap();
    if !matches!(
        replaced,
        Some(MediaEvent::TrackEnd {
            reason: TrackEndReason::Replaced,
            ..
        })
    ) || started != Some(MediaEvent::TrackStart(replacement.clone()))
    {
        return Err(failure("replace-order", "replacement events out of order"));
    }
    checks.push("replace-order");

    player.stop(CancellationToken::new()).await.unwrap();
    if !matches!(
        player.next_event(CancellationToken::new()).await.unwrap(),
        Some(MediaEvent::TrackEnd {
            reason: TrackEndReason::Stopped,
            ..
        })
    ) {
        return Err(failure("stop-event", "stopped event missing"));
    }
    checks.push("stop-event");

    let short = track(adapter, "fixture:short").await?;
    player.play(short, CancellationToken::new()).await.unwrap();
    player.next_event(CancellationToken::new()).await.unwrap();
    player.next_frame(CancellationToken::new()).await.unwrap();
    player.next_frame(CancellationToken::new()).await.unwrap();
    if !matches!(
        player.next_event(CancellationToken::new()).await.unwrap(),
        Some(MediaEvent::TrackEnd {
            reason: TrackEndReason::Finished,
            ..
        })
    ) {
        return Err(failure("finished-event", "finished event missing"));
    }
    checks.push("finished-event");

    for (identifier, expected) in [
        ("fixture:event-error", "error"),
        ("fixture:event-stuck", "stuck"),
    ] {
        player
            .play(track(adapter, identifier).await?, CancellationToken::new())
            .await
            .unwrap();
        player.next_event(CancellationToken::new()).await.unwrap();
        let event = player.next_event(CancellationToken::new()).await.unwrap();
        let matches = matches!(
            (&event, expected),
            (Some(MediaEvent::TrackError { .. }), "error")
                | (Some(MediaEvent::TrackStuck { .. }), "stuck")
        );
        if !matches {
            return Err(failure("failure-events", format!("unexpected {event:?}")));
        }
    }
    checks.push("error-stuck-events");

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancellation_error = adapter
        .load(request("fixture:track"), cancelled)
        .await
        .expect_err("cancelled load must fail");
    if cancellation_error.kind != AdapterErrorKind::Cancelled {
        return Err(failure(
            "cancellation",
            format!("unexpected {cancellation_error:?}"),
        ));
    }
    checks.push("cancellation");

    player.shutdown().await.unwrap();
    adapter.shutdown().await.unwrap();
    let after_shutdown = adapter
        .load(request("fixture:track"), CancellationToken::new())
        .await
        .expect_err("load after shutdown must fail");
    if after_shutdown.kind != AdapterErrorKind::Shutdown {
        return Err(failure(
            "shutdown",
            format!("unexpected {after_shutdown:?}"),
        ));
    }
    checks.push("shutdown");

    if checks != ADAPTER_CONFORMANCE_CHECKS {
        return Err(failure(
            "suite-coverage",
            format!("expected {ADAPTER_CONFORMANCE_CHECKS:?}, got {checks:?}"),
        ));
    }
    Ok(AdapterConformanceReport { checks })
}

#[cfg(test)]
mod tests {
    use crate::FakeMantle;

    use super::*;

    #[tokio::test]
    async fn fake_mantle_passes_the_shared_adapter_suite() {
        let fake = FakeMantle::default();
        let report = run_adapter_conformance(&fake).await.unwrap();
        assert_eq!(report.checks, ADAPTER_CONFORMANCE_CHECKS);
    }
}
