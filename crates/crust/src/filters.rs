//! Backend-independent normalized Lavalink filter configuration.

pub const EQUALIZER_BANDS: usize = 15;
/// Lowest supported combined `speed * rate` value for the bounded timescale stage.
pub const MIN_TIMESCALE_DURATION_RATIO: f64 = 128.0 / 1023.0;
/// Highest supported combined `speed * rate` value for the bounded timescale stage.
pub const MAX_TIMESCALE_DURATION_RATIO: f64 = 128.0;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Karaoke {
    pub level: f32,
    pub mono_level: f32,
    pub filter_band: f32,
    pub filter_width: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Timescale {
    pub speed: f64,
    pub pitch: f64,
    pub rate: f64,
}

impl Timescale {
    #[must_use]
    pub fn duration_ratio(self) -> f64 {
        self.speed * self.rate
    }

    #[must_use]
    pub fn has_supported_duration_ratio(self) -> bool {
        (MIN_TIMESCALE_DURATION_RATIO..=MAX_TIMESCALE_DURATION_RATIO)
            .contains(&self.duration_ratio())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Modulation {
    pub frequency: f32,
    pub depth: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Distortion {
    pub sin_offset: f32,
    pub sin_scale: f32,
    pub cos_offset: f32,
    pub cos_scale: f32,
    pub tan_offset: f32,
    pub tan_scale: f32,
    pub offset: f32,
    pub scale: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChannelMix {
    pub left_to_left: f32,
    pub left_to_right: f32,
    pub right_to_left: f32,
    pub right_to_right: f32,
}

/// Effective core filters. Plugin JSON intentionally has no execution path.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FilterConfiguration {
    pub volume: Option<f32>,
    pub equalizer: Option<[f32; EQUALIZER_BANDS]>,
    pub karaoke: Option<Karaoke>,
    pub timescale: Option<Timescale>,
    pub tremolo: Option<Modulation>,
    pub vibrato: Option<Modulation>,
    pub distortion: Option<Distortion>,
    pub rotation_hz: Option<f64>,
    pub channel_mix: Option<ChannelMix>,
    pub low_pass_smoothing: Option<f32>,
}

impl FilterConfiguration {
    #[must_use]
    pub fn is_effective(&self) -> bool {
        self.volume.is_some_and(|value| value != 1.0)
            || self
                .equalizer
                .is_some_and(|bands| bands.into_iter().any(|gain| gain != 0.0))
            || self.karaoke.is_some()
            || self
                .timescale
                .is_some_and(|value| value.speed != 1.0 || value.pitch != 1.0 || value.rate != 1.0)
            || self.tremolo.is_some_and(|value| value.depth != 0.0)
            || self.vibrato.is_some_and(|value| value.depth != 0.0)
            || self.distortion.is_some_and(|value| {
                value.sin_offset != 0.0
                    || value.sin_scale != 1.0
                    || value.cos_offset != 0.0
                    || value.cos_scale != 1.0
                    || value.tan_offset != 0.0
                    || value.tan_scale != 1.0
                    || value.offset != 0.0
                    || value.scale != 1.0
            })
            || self.rotation_hz.is_some_and(|value| value != 0.0)
            || self.channel_mix.is_some_and(|value| {
                value.left_to_left != 1.0
                    || value.left_to_right != 0.0
                    || value.right_to_left != 0.0
                    || value.right_to_right != 1.0
            })
            || self
                .low_pass_smoothing
                .is_some_and(|smoothing| smoothing > 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_enablement_predicates_ignore_identity_values() {
        let identity = FilterConfiguration {
            volume: Some(1.0),
            equalizer: Some([0.0; EQUALIZER_BANDS]),
            timescale: Some(Timescale {
                speed: 1.0,
                pitch: 1.0,
                rate: 1.0,
            }),
            tremolo: Some(Modulation {
                frequency: 2.0,
                depth: 0.0,
            }),
            rotation_hz: Some(0.0),
            channel_mix: Some(ChannelMix {
                left_to_left: 1.0,
                left_to_right: 0.0,
                right_to_left: 0.0,
                right_to_right: 1.0,
            }),
            low_pass_smoothing: Some(1.0),
            ..FilterConfiguration::default()
        };
        assert!(!identity.is_effective());
        assert!(
            FilterConfiguration {
                karaoke: Some(Karaoke {
                    level: 1.0,
                    mono_level: 1.0,
                    filter_band: 220.0,
                    filter_width: 100.0,
                }),
                ..FilterConfiguration::default()
            }
            .is_effective()
        );
    }
}
