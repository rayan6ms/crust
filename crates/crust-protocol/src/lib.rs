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

const fn default_one_f32() -> f32 {
    1.0
}

const fn default_one_f64() -> f64 {
    1.0
}

const fn default_two_f32() -> f32 {
    2.0
}

const fn default_half_f32() -> f32 {
    0.5
}

const fn default_karaoke_band() -> f32 {
    220.0
}

const fn default_karaoke_width() -> f32 {
    100.0
}

const fn default_low_pass() -> f32 {
    20.0
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EqualizerBand {
    pub band: i32,
    #[serde(default = "default_one_f32")]
    pub gain: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Karaoke {
    #[serde(default = "default_one_f32")]
    pub level: f32,
    #[serde(default = "default_one_f32")]
    pub mono_level: f32,
    #[serde(default = "default_karaoke_band")]
    pub filter_band: f32,
    #[serde(default = "default_karaoke_width")]
    pub filter_width: f32,
}

impl Default for Karaoke {
    fn default() -> Self {
        Self {
            level: 1.0,
            mono_level: 1.0,
            filter_band: 220.0,
            filter_width: 100.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Timescale {
    #[serde(default = "default_one_f64")]
    pub speed: f64,
    #[serde(default = "default_one_f64")]
    pub pitch: f64,
    #[serde(default = "default_one_f64")]
    pub rate: f64,
}

impl Default for Timescale {
    fn default() -> Self {
        Self {
            speed: 1.0,
            pitch: 1.0,
            rate: 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Tremolo {
    #[serde(default = "default_two_f32")]
    pub frequency: f32,
    #[serde(default = "default_half_f32")]
    pub depth: f32,
}

impl Default for Tremolo {
    fn default() -> Self {
        Self {
            frequency: 2.0,
            depth: 0.5,
        }
    }
}

pub type Vibrato = Tremolo;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rotation {
    #[serde(default)]
    pub rotation_hz: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Distortion {
    #[serde(default)]
    pub sin_offset: f32,
    #[serde(default = "default_one_f32")]
    pub sin_scale: f32,
    #[serde(default)]
    pub cos_offset: f32,
    #[serde(default = "default_one_f32")]
    pub cos_scale: f32,
    #[serde(default)]
    pub tan_offset: f32,
    #[serde(default = "default_one_f32")]
    pub tan_scale: f32,
    #[serde(default)]
    pub offset: f32,
    #[serde(default = "default_one_f32")]
    pub scale: f32,
}

impl Default for Distortion {
    fn default() -> Self {
        Self {
            sin_offset: 0.0,
            sin_scale: 1.0,
            cos_offset: 0.0,
            cos_scale: 1.0,
            tan_offset: 0.0,
            tan_scale: 1.0,
            offset: 0.0,
            scale: 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelMix {
    #[serde(default = "default_one_f32")]
    pub left_to_left: f32,
    #[serde(default)]
    pub left_to_right: f32,
    #[serde(default)]
    pub right_to_left: f32,
    #[serde(default = "default_one_f32")]
    pub right_to_right: f32,
}

impl Default for ChannelMix {
    fn default() -> Self {
        Self {
            left_to_left: 1.0,
            left_to_right: 0.0,
            right_to_left: 0.0,
            right_to_right: 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LowPass {
    #[serde(default = "default_low_pass")]
    pub smoothing: f32,
}

impl Default for LowPass {
    fn default() -> Self {
        Self { smoothing: 20.0 }
    }
}

/// Complete Lavalink filter replacement carried by a player PATCH.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Filters {
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub volume: PatchField<f32>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub equalizer: PatchField<Vec<EqualizerBand>>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub karaoke: PatchField<Karaoke>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub timescale: PatchField<Timescale>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub tremolo: PatchField<Tremolo>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub vibrato: PatchField<Vibrato>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub distortion: PatchField<Distortion>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub rotation: PatchField<Rotation>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub channel_mix: PatchField<ChannelMix>,
    #[serde(default, skip_serializing_if = "PatchField::is_omitted")]
    pub low_pass: PatchField<LowPass>,
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

    #[test]
    fn filter_subobjects_apply_frozen_defaults_and_serialize_them_explicitly() {
        let update: PlayerUpdate = serde_json::from_value(json!({
            "filters": {
                "equalizer": [{"band": 4}],
                "karaoke": {},
                "timescale": {},
                "tremolo": {},
                "vibrato": {},
                "rotation": {},
                "distortion": {},
                "channelMix": {},
                "lowPass": {},
                "pluginFilters": {"unknown": [1, null, true]}
            }
        }))
        .unwrap();
        let PatchField::Value(filters) = update.filters else {
            panic!("filters missing");
        };
        assert_eq!(filters.karaoke, PatchField::Value(Karaoke::default()));
        assert_eq!(filters.timescale, PatchField::Value(Timescale::default()));
        assert_eq!(filters.distortion, PatchField::Value(Distortion::default()));
        assert_eq!(
            filters.channel_mix,
            PatchField::Value(ChannelMix::default())
        );
        assert_eq!(filters.low_pass, PatchField::Value(LowPass::default()));
        assert_eq!(
            filters.equalizer,
            PatchField::Value(vec![EqualizerBand { band: 4, gain: 1.0 }])
        );
        let encoded = serde_json::to_value(filters).unwrap();
        assert_eq!(encoded["karaoke"]["filterBand"], 220.0);
        assert_eq!(encoded["timescale"]["pitch"], 1.0);
        assert_eq!(encoded["lowPass"]["smoothing"], 20.0);
        assert_eq!(encoded["pluginFilters"]["unknown"], json!([1, null, true]));
    }

    #[test]
    fn filter_patch_preserves_nullable_and_non_nullable_nulls_for_validation() {
        let update: PlayerUpdate = serde_json::from_value(json!({
            "filters": {
                "volume": null,
                "equalizer": null,
                "karaoke": null,
                "timescale": null
            }
        }))
        .unwrap();
        let PatchField::Value(filters) = update.filters else {
            panic!("filters missing");
        };
        assert_eq!(filters.volume, PatchField::Null);
        assert_eq!(filters.equalizer, PatchField::Null);
        assert_eq!(filters.karaoke, PatchField::Null);
        assert_eq!(filters.timescale, PatchField::Null);
    }
}
