//! Wire models needed to describe the frozen Lavalink 4.2.2 PATCH surface.
//!
//! This crate deliberately contains no HTTP server or player behavior. JSON
//! payload limits belong at the future request boundary, before these models
//! are retained.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// A PATCH member that distinguishes absence, JSON `null`, and a value.
///
/// Put `#[serde(default, skip_serializing_if = "PatchField::is_omitted")]` on
/// every containing field. Serializing `Omitted` directly is rejected so an
/// accidentally unannotated model cannot silently emit a compatibility value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PatchField<T> {
    #[default]
    Omitted,
    Null,
    Value(T),
}

impl<T> PatchField<T> {
    #[must_use]
    pub const fn is_omitted(&self) -> bool {
        matches!(self, Self::Omitted)
    }
}

impl<'de, T> Deserialize<'de> for PatchField<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<T>::deserialize(deserializer).map(|value| value.map_or(Self::Null, Self::Value))
    }
}

impl<T> Serialize for PatchField<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Omitted => Err(serde::ser::Error::custom(
                "PatchField::Omitted must be skipped by its containing model",
            )),
            Self::Null => serializer.serialize_none(),
            Self::Value(value) => value.serialize(serializer),
        }
    }
}

pub type JsonObject = BTreeMap<String, Value>;

/// Nested `track` member accepted by a player PATCH.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PlayerUpdateTrack {
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub encoded: PatchField<String>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub identifier: PatchField<String>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub user_data: PatchField<JsonObject>,
}

/// Discord voice state accepted by a player PATCH.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoiceState {
    pub token: String,
    pub endpoint: String,
    pub session_id: String,
    pub channel_id: Option<String>,
}

/// Filter update values. Built-in filter bodies remain opaque in P02; their
/// executable semantics belong to a later phase. This still preserves exact
/// JSON types for differential fixtures and plugin-owned payloads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Filters {
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub volume: PatchField<f64>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub equalizer: PatchField<Value>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub karaoke: PatchField<Value>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub timescale: PatchField<Value>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub tremolo: PatchField<Value>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub vibrato: PatchField<Value>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub distortion: PatchField<Value>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub rotation: PatchField<Value>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub channel_mix: PatchField<Value>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub low_pass: PatchField<Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub plugin_filters: JsonObject,
}

/// Frozen Lavalink 4.2.2 player PATCH request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PlayerUpdate {
    /// Deprecated in 4.2.2, but still accepted.
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub encoded_track: PatchField<String>,
    /// Deprecated in 4.2.2, but still accepted.
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub identifier: PatchField<String>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub track: PatchField<PlayerUpdateTrack>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub position: PatchField<i64>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub end_time: PatchField<i64>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub volume: PatchField<i32>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub paused: PatchField<bool>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub filters: PatchField<Filters>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub voice: PatchField<VoiceState>,
}

/// Frozen Lavalink 4.2.2 session PATCH request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SessionUpdate {
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub resuming: PatchField<bool>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub timeout: PatchField<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn player_patch_distinguishes_omitted_null_and_value_recursively() {
        let omitted: PlayerUpdate = serde_json::from_value(json!({})).unwrap();
        assert_eq!(omitted.encoded_track, PatchField::Omitted);
        assert_eq!(omitted.track, PatchField::Omitted);

        let nulls: PlayerUpdate = serde_json::from_value(json!({
            "encodedTrack": null,
            "endTime": null,
            "track": {"encoded": null}
        }))
        .unwrap();
        assert_eq!(nulls.encoded_track, PatchField::Null);
        assert_eq!(nulls.end_time, PatchField::Null);
        assert_eq!(
            nulls.track,
            PatchField::Value(PlayerUpdateTrack {
                encoded: PatchField::Null,
                ..PlayerUpdateTrack::default()
            })
        );

        let values: PlayerUpdate = serde_json::from_value(json!({
            "encodedTrack": "deprecated",
            "paused": false,
            "track": {"identifier": "search term"}
        }))
        .unwrap();
        assert_eq!(values.encoded_track, PatchField::Value("deprecated".into()));
        assert_eq!(values.paused, PatchField::Value(false));
    }

    #[test]
    fn arbitrary_plugin_and_user_json_round_trips_semantically() {
        let input = json!({
            "track": {"userData": {
                "integer": 9007199254740993_i64,
                "nested": [null, true, {"text": "unchanged"}]
            }},
            "filters": {"pluginFilters": {
                "vendor.example": {"array": [1, 2.5, false], "object": {"x": null}}
            }}
        });
        let update: PlayerUpdate = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(update).unwrap(), input);
    }

    #[test]
    fn serialization_omits_absent_members_but_emits_null() {
        let update = PlayerUpdate {
            encoded_track: PatchField::Null,
            paused: PatchField::Value(false),
            ..PlayerUpdate::default()
        };
        assert_eq!(
            serde_json::to_value(update).unwrap(),
            json!({"encodedTrack": null, "paused": false})
        );
    }

    #[test]
    fn direct_omitted_serialization_fails_closed() {
        assert!(serde_json::to_value(PatchField::<String>::Omitted).is_err());
    }

    #[test]
    fn session_patch_preserves_reference_null_rejections_for_later_validation() {
        let update: SessionUpdate = serde_json::from_value(json!({
            "resuming": null,
            "timeout": 15
        }))
        .unwrap();
        assert_eq!(update.resuming, PatchField::Null);
        assert_eq!(update.timeout, PatchField::Value(15));
    }
}
