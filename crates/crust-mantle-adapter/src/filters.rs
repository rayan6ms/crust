use std::f32::consts::PI;

use crust::filters::{FilterConfiguration, Karaoke, Modulation};
use mantle_audio::{
    AudioFrameError, EqualizerFactory, FilterChainBuilder, PcmFilter, PcmFilterFactory, PcmFormat,
    PcmFrame, StreamingPcmProcessor, StreamingPcmProgress, VolumeLevel, apply_volume,
};
use microfft::Complex32;
use microfft::complex::cfft_1024;
use microfft::inverse::ifft_1024;
use micromath::F32Ext;

pub(crate) struct CrustFilterFactory(pub(crate) FilterConfiguration);

impl PcmFilterFactory for CrustFilterFactory {
    fn build(
        &self,
        format: PcmFormat,
        builder: &mut FilterChainBuilder,
    ) -> Result<(), AudioFrameError> {
        let config = &self.0;
        if let Some(volume) = config.volume.filter(|value| *value != 1.0) {
            builder.push(Volume(volume))?;
        }
        if let Some(gains) = config
            .equalizer
            .filter(|gains| gains.iter().any(|gain| *gain != 0.0))
        {
            let equalizer = EqualizerFactory::new();
            for (band, gain) in gains.into_iter().enumerate() {
                equalizer.set_gain(band, gain);
            }
            equalizer.build(format, builder)?;
        }
        if let Some(karaoke) = config.karaoke {
            builder.push(KaraokeFilter::new(karaoke, format.sample_rate())?)?;
        }
        if let Some(timescale) = config
            .timescale
            .filter(|value| value.speed != 1.0 || value.pitch != 1.0 || value.rate != 1.0)
        {
            builder.push_streaming(TimescaleFilter::new(timescale)?)?;
        }
        if let Some(tremolo) = config.tremolo.filter(|value| value.depth != 0.0) {
            builder.push(Tremolo::new(tremolo, format))?;
        }
        if let Some(vibrato) = config.vibrato.filter(|value| value.depth != 0.0) {
            builder.push(Vibrato::new(vibrato, format))?;
        }
        if let Some(distortion) = config.distortion.filter(|value| {
            value.sin_offset != 0.0
                || value.sin_scale != 1.0
                || value.cos_offset != 0.0
                || value.cos_scale != 1.0
                || value.tan_offset != 0.0
                || value.tan_scale != 1.0
                || value.offset != 0.0
                || value.scale != 1.0
        }) {
            builder.push(Distortion(distortion))?;
        }
        if let Some(rotation_hz) = config.rotation_hz.filter(|value| *value != 0.0) {
            builder.push(Rotation::new(rotation_hz, format.sample_rate()))?;
        }
        if let Some(channel_mix) = config.channel_mix.filter(|value| {
            value.left_to_left != 1.0
                || value.left_to_right != 0.0
                || value.right_to_left != 0.0
                || value.right_to_right != 1.0
        }) {
            builder.push(ChannelMix(channel_mix))?;
        }
        if let Some(smoothing) = config.low_pass_smoothing.filter(|value| *value > 1.0) {
            builder.push(LowPass::new(smoothing, format.channels()))?;
        }
        if let Some(volume) = config.player_volume.filter(|volume| *volume != 100) {
            builder.push(PlayerVolume(VolumeLevel::new(i32::from(volume))))?;
        }
        Ok(())
    }
}

/// Lavaplayer applies player volume to signed PCM after the user filter chain.
/// The fixed scratch buffer avoids a per-frame allocation and preserves Mantle's
/// integer scaling/clipping rather than approximating it with the float filter.
struct PlayerVolume(VolumeLevel);

impl PcmFilter for PlayerVolume {
    fn process(&mut self, frame: &mut PcmFrame) -> Result<(), AudioFrameError> {
        if self.0 == VolumeLevel::MUTED {
            frame.samples_mut().fill(0.0);
            return Ok(());
        }
        let mut scratch = [0_i16; mantle_audio::COMPATIBLE_PCM_SAMPLES];
        for samples in frame.samples_mut().chunks_mut(scratch.len()) {
            for (output, input) in scratch.iter_mut().zip(samples.iter()) {
                #[allow(clippy::cast_possible_truncation)]
                let converted = (*input * 32_768.0) as i32;
                *output = i16::try_from(converted.clamp(i32::from(i16::MIN), i32::from(i16::MAX)))
                    .unwrap_or(0);
            }
            apply_volume(&mut scratch[..samples.len()], self.0);
            for (output, input) in samples.iter_mut().zip(scratch.iter()) {
                *output = f32::from(*input) / 32_768.0;
            }
        }
        Ok(())
    }
    fn reset(&mut self) {}
}

struct Volume(f32);

impl PcmFilter for Volume {
    fn process(&mut self, frame: &mut PcmFrame) -> Result<(), AudioFrameError> {
        let multiplier = if self.0 <= 1.5 {
            (self.0 * 0.79).tan()
        } else {
            2.4612 * self.0 / 1.5
        };
        for sample in frame.samples_mut() {
            *sample = (*sample * multiplier).clamp(-1.0, 1.0);
        }
        Ok(())
    }

    fn reset(&mut self) {}
}

struct KaraokeFilter {
    config: Karaoke,
    a: f32,
    b: f32,
    c: f32,
    y1: f32,
    y2: f32,
}

impl KaraokeFilter {
    fn new(config: Karaoke, sample_rate: u32) -> Result<Self, AudioFrameError> {
        if !config.level.is_finite()
            || !config.mono_level.is_finite()
            || !config.filter_band.is_finite()
            || !config.filter_width.is_finite()
            || !(0.0..=1.0).contains(&config.level)
            || !(0.0..=1.0).contains(&config.mono_level)
            || !(0.0..=sample_rate as f32 / 2.0).contains(&config.filter_band)
            || !(0.0..=sample_rate as f32 / 2.0).contains(&config.filter_width)
        {
            return Err(AudioFrameError::InvalidFilterConfiguration(
                "invalid karaoke parameters",
            ));
        }
        let rate = sample_rate as f32;
        let c = (-2.0 * PI * config.filter_width / rate).exp();
        let b = -4.0 * c / (1.0 + c) * (2.0 * PI * config.filter_band / rate).cos();
        let a = (1.0 - b * b / (4.0 * c)).max(0.0).sqrt() * (1.0 - c);
        if ![a, b, c].iter().all(|v| v.is_finite()) {
            return Err(AudioFrameError::InvalidFilterConfiguration(
                "nonfinite karaoke coefficients",
            ));
        }
        Ok(Self {
            config,
            a,
            b,
            c,
            y1: 0.0,
            y2: 0.0,
        })
    }
}

impl PcmFilter for KaraokeFilter {
    fn process(&mut self, frame: &mut PcmFrame) -> Result<(), AudioFrameError> {
        if frame.channels() != 2 {
            return Ok(());
        }
        for pair in frame.samples_mut().chunks_exact_mut(2) {
            let left = pair[0];
            let right = pair[1];
            let y = self.a * ((left + right) / 2.0) - self.b * self.y1 - self.c * self.y2;
            self.y2 = self.y1;
            self.y1 = y;
            let mono = y * self.config.mono_level * self.config.level;
            pair[0] = left - right * self.config.level + mono;
            pair[1] = right - left * self.config.level + mono;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.y1 = 0.0;
        self.y2 = 0.0;
    }
}

enum TimescaleFilter {
    Speed(Box<WsolaTimescale>),
    General(Box<PhaseVocoderTimescale>),
}

impl TimescaleFilter {
    fn new(config: crust::filters::Timescale) -> Result<Self, AudioFrameError> {
        if config.pitch == 1.0 && config.rate == 1.0 && (0.25..=4.0).contains(&config.speed) {
            Ok(Self::Speed(Box::new(WsolaTimescale::new(config.speed)?)))
        } else {
            Ok(Self::General(Box::new(PhaseVocoderTimescale::new(config)?)))
        }
    }
}

impl StreamingPcmProcessor for TimescaleFilter {
    fn process(
        &mut self,
        input: &[f32],
        output: &mut [f32],
    ) -> Result<StreamingPcmProgress, AudioFrameError> {
        match self {
            Self::Speed(processor) => processor.process(input, output),
            Self::General(processor) => processor.process(input, output),
        }
    }

    fn finish(&mut self, output: &mut [f32]) -> Result<usize, AudioFrameError> {
        match self {
            Self::Speed(processor) => processor.finish(output),
            Self::General(processor) => processor.finish(output),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::Speed(processor) => processor.reset(),
            Self::General(processor) => processor.reset(),
        }
    }
}

const WSOLA_HOP: usize = 480;
const WSOLA_FRAME: usize = WSOLA_HOP * 2;
const WSOLA_SEARCH: usize = 192;
const WSOLA_INPUT_CAPACITY: usize = 4096;
const WSOLA_OUTPUT_SAMPLES: usize = WSOLA_HOP * 2;
const WSOLA_WINDOW: [f32; WSOLA_FRAME] = wsola_window();

struct WsolaTimescale {
    tempo: f64,
    input: [f32; WSOLA_INPUT_CAPACITY * 2],
    input_frames: usize,
    origin: u64,
    output: [f32; WSOLA_OUTPUT_SAMPLES],
    output_offset: usize,
    output_len: usize,
    overlap: [f32; WSOLA_OUTPUT_SAMPLES],
    primed: bool,
    ideal: f64,
    last_source: u64,
    source_frames: u64,
    produced_frames: u64,
    finishing: bool,
}

impl WsolaTimescale {
    fn new(tempo: f64) -> Result<Self, AudioFrameError> {
        let config = crust::filters::Timescale {
            speed: tempo,
            pitch: 1.0,
            rate: 1.0,
        };
        if !config.has_supported_duration_ratio() {
            return Err(AudioFrameError::InvalidResamplerConfiguration(
                "combined timescale speed and rate are outside the supported range",
            ));
        }
        Ok(Self {
            tempo,
            input: [0.0; WSOLA_INPUT_CAPACITY * 2],
            input_frames: 0,
            origin: 0,
            output: [0.0; WSOLA_OUTPUT_SAMPLES],
            output_offset: 0,
            output_len: 0,
            overlap: [0.0; WSOLA_OUTPUT_SAMPLES],
            primed: false,
            ideal: 0.0,
            last_source: 0,
            source_frames: 0,
            produced_frames: 0,
            finishing: false,
        })
    }

    fn sample(&self, frame: u64, channel: usize) -> f32 {
        self.input[(frame - self.origin) as usize * 2 + channel]
    }

    fn available_end(&self) -> u64 {
        self.origin + self.input_frames as u64
    }

    fn drain(&mut self, output: &mut [f32]) -> usize {
        let count = (self.output_len - self.output_offset).min(output.len()) / 2 * 2;
        output[..count]
            .copy_from_slice(&self.output[self.output_offset..self.output_offset + count]);
        self.output_offset += count;
        if self.output_offset == self.output_len {
            self.output_offset = 0;
            self.output_len = 0;
        }
        count
    }

    fn compact(&mut self) {
        let search_floor = self.ideal.floor().max(0.0) as u64;
        let keep = search_floor
            .saturating_sub(WSOLA_SEARCH as u64)
            .min(self.last_source + WSOLA_HOP as u64)
            .clamp(self.origin, self.available_end());
        let discard = (keep - self.origin) as usize;
        if discard == 0 {
            return;
        }
        let remaining = self.input_frames - discard;
        self.input
            .copy_within(discard * 2..self.input_frames * 2, 0);
        self.input[remaining * 2..self.input_frames * 2].fill(0.0);
        self.input_frames = remaining;
        self.origin = keep;
    }

    fn append(&mut self, input: &[f32], source: bool) -> usize {
        let frames = (input.len() / 2).min(WSOLA_INPUT_CAPACITY - self.input_frames);
        let samples = frames * 2;
        let offset = self.input_frames * 2;
        self.input[offset..offset + samples].copy_from_slice(&input[..samples]);
        self.input_frames += frames;
        if source {
            self.source_frames += frames as u64;
        }
        samples
    }

    fn append_padding(&mut self) -> bool {
        self.compact();
        let free = WSOLA_INPUT_CAPACITY - self.input_frames;
        if free == 0 {
            return false;
        }
        let frames = free.min(WSOLA_FRAME + WSOLA_SEARCH);
        let start = self.input_frames * 2;
        self.input[start..start + frames * 2].fill(0.0);
        self.input_frames += frames;
        true
    }

    fn target_frames(&self) -> u64 {
        (self.source_frames as f64 / self.tempo).round() as u64
    }

    fn step(&mut self) -> bool {
        if self.output_len != 0 {
            return true;
        }
        let available_end = self.available_end();
        let target_frames = self.finishing.then(|| self.target_frames());
        if target_frames.is_some_and(|target| self.produced_frames >= target) {
            return false;
        }

        let source = if !self.primed {
            if self.origin + WSOLA_FRAME as u64 > available_end {
                return false;
            }
            self.origin
        } else {
            let base = self.ideal.round() as i64;
            let minimum = self.origin as i64;
            let maximum = available_end as i64 - WSOLA_FRAME as i64;
            let continuation = self.last_source + WSOLA_HOP as u64;
            let needed = (base + WSOLA_SEARCH as i64 + WSOLA_FRAME as i64)
                .max((continuation + WSOLA_HOP as u64) as i64);
            if (!self.finishing && needed > available_end as i64)
                || maximum < minimum
                || continuation + WSOLA_HOP as u64 > available_end
            {
                return false;
            }
            let low = (base - WSOLA_SEARCH as i64).max(minimum);
            let high = (base + WSOLA_SEARCH as i64).min(maximum);
            if high < low {
                return false;
            }
            self.best_match(low as u64, high as u64, continuation)
        };

        let frames = target_frames
            .map(|target| (target - self.produced_frames).min(WSOLA_HOP as u64) as usize)
            .unwrap_or(WSOLA_HOP);
        for (frame, window) in WSOLA_WINDOW.iter().copied().enumerate().take(frames) {
            for channel in 0..2 {
                let index = frame * 2 + channel;
                let current = self.sample(source + frame as u64, channel) * window;
                self.output[index] = if self.primed {
                    self.overlap[index] + current
                } else {
                    current
                };
            }
        }
        for frame in 0..WSOLA_HOP {
            for channel in 0..2 {
                self.overlap[frame * 2 + channel] = self
                    .sample(source + (WSOLA_HOP + frame) as u64, channel)
                    * WSOLA_WINDOW[WSOLA_HOP + frame];
            }
        }
        self.output_offset = 0;
        self.output_len = frames * 2;
        self.produced_frames += frames as u64;
        self.last_source = source;
        if self.primed {
            self.ideal += WSOLA_HOP as f64 * self.tempo;
        } else {
            self.ideal = source as f64 + WSOLA_HOP as f64 * self.tempo;
            self.primed = true;
        }
        self.compact();
        true
    }

    fn best_match(&self, low: u64, high: u64, continuation: u64) -> u64 {
        const STRIDE: usize = 8;
        let mut target_energy = 0.0_f32;
        for frame in (0..WSOLA_HOP).step_by(STRIDE) {
            let mono = self.sample(continuation + frame as u64, 0)
                + self.sample(continuation + frame as u64, 1);
            target_energy += mono * mono;
        }
        let mut best = low;
        let mut best_score = f32::NEG_INFINITY;
        for candidate in low..=high {
            let mut dot = 0.0_f32;
            let mut energy = 0.0_f32;
            for frame in (0..WSOLA_HOP).step_by(STRIDE) {
                let target = self.sample(continuation + frame as u64, 0)
                    + self.sample(continuation + frame as u64, 1);
                let sample = self.sample(candidate + frame as u64, 0)
                    + self.sample(candidate + frame as u64, 1);
                dot += target * sample;
                energy += sample * sample;
            }
            let score = dot * dot.abs() / (target_energy * energy + 1.0e-12);
            if score > best_score {
                best_score = score;
                best = candidate;
            }
        }
        best
    }
}

impl StreamingPcmProcessor for WsolaTimescale {
    fn process(
        &mut self,
        input: &[f32],
        output: &mut [f32],
    ) -> Result<StreamingPcmProgress, AudioFrameError> {
        let mut produced = self.drain(output);
        if produced != 0 {
            return Ok(StreamingPcmProgress::new(0, produced));
        }
        self.step();
        produced = self.drain(output);
        if produced != 0 {
            return Ok(StreamingPcmProgress::new(0, produced));
        }
        let consumed = self.append(input, true);
        if consumed == 0 && !input.is_empty() {
            return Err(AudioFrameError::StreamingProcessorCapacityExceeded {
                required: self.input_frames + input.len() / 2,
                capacity: WSOLA_INPUT_CAPACITY,
            });
        }
        self.step();
        produced = self.drain(output);
        Ok(StreamingPcmProgress::new(consumed, produced))
    }

    fn finish(&mut self, output: &mut [f32]) -> Result<usize, AudioFrameError> {
        let produced = self.drain(output);
        if produced != 0 {
            return Ok(produced);
        }
        self.finishing = true;
        for _ in 0..8 {
            if self.step() {
                return Ok(self.drain(output));
            }
            if self.produced_frames >= self.target_frames() {
                return Ok(0);
            }
            if !self.append_padding() {
                return Err(AudioFrameError::StreamingProcessorCapacityExceeded {
                    required: self.input_frames + WSOLA_FRAME + WSOLA_SEARCH,
                    capacity: WSOLA_INPUT_CAPACITY,
                });
            }
        }
        Err(AudioFrameError::StreamingProcessorCapacityExceeded {
            required: self.input_frames + WSOLA_FRAME + WSOLA_SEARCH,
            capacity: WSOLA_INPUT_CAPACITY,
        })
    }

    fn reset(&mut self) {
        self.input.fill(0.0);
        self.input_frames = 0;
        self.origin = 0;
        self.output.fill(0.0);
        self.output_offset = 0;
        self.output_len = 0;
        self.overlap.fill(0.0);
        self.primed = false;
        self.ideal = 0.0;
        self.last_source = 0;
        self.source_frames = 0;
        self.produced_frames = 0;
        self.finishing = false;
    }
}

const fn wsola_window() -> [f32; WSOLA_FRAME] {
    let mut output = [0.0; WSOLA_FRAME];
    let mut index = 0;
    while index < WSOLA_FRAME {
        output[index] = 0.5 - 0.5 * cosine_approx(index as f32 * 2.0 / WSOLA_FRAME as f32);
        index += 1;
    }
    output
}

struct PhaseVocoderTimescale {
    shifter: Box<StereoShifter>,
    input: [f32; TIMESCALE_INPUT_SAMPLES],
    input_frames: usize,
    output: [f32; TIMESCALE_MAX_OUTPUT_SAMPLES],
    output_offset: usize,
    output_len: usize,
    output_frames_per_hop: f64,
    output_hop_residual: f64,
    shift_semitones: f32,
    finished: bool,
}

const TIMESCALE_INPUT_FRAMES: usize = 128;
const TIMESCALE_INPUT_SAMPLES: usize = TIMESCALE_INPUT_FRAMES * 2;
const TIMESCALE_MAX_OUTPUT_FRAMES: usize = 1023;
const TIMESCALE_MAX_OUTPUT_SAMPLES: usize = TIMESCALE_MAX_OUTPUT_FRAMES * 2;

const SHIFTER_FFT: usize = 1024;
const SHIFTER_HALF_FFT: usize = SHIFTER_FFT / 2;
const SHIFTER_INPUT_HOP: usize = TIMESCALE_INPUT_FRAMES;
const SHIFTER_INPUT_TAIL: usize = SHIFTER_FFT - SHIFTER_INPUT_HOP;
const SHIFTER_INPUT_WINDOW: [f32; SHIFTER_FFT] = shifter_input_window();
const SHIFTER_OUTPUT_WINDOW: [f32; SHIFTER_FFT] = shifter_output_window();

struct StereoShifter {
    history: [Complex32; SHIFTER_FFT],
    spectrum: [Complex32; SHIFTER_FFT],
    overlap: [Complex32; SHIFTER_FFT],
    input_phase: [[f32; SHIFTER_HALF_FFT]; 2],
    output_phase: [[f32; SHIFTER_HALF_FFT]; 2],
    magnitudes: [[f32; SHIFTER_HALF_FFT]; 2],
    frequencies: [[f32; SHIFTER_HALF_FFT]; 2],
    shifted: [[Complex32; SHIFTER_HALF_FFT]; 2],
}

impl StereoShifter {
    fn new() -> Self {
        Self {
            history: [Complex32::new(0.0, 0.0); SHIFTER_FFT],
            spectrum: [Complex32::new(0.0, 0.0); SHIFTER_FFT],
            overlap: [Complex32::new(0.0, 0.0); SHIFTER_FFT],
            input_phase: [[0.0; SHIFTER_HALF_FFT]; 2],
            output_phase: [[0.0; SHIFTER_HALF_FFT]; 2],
            magnitudes: [[0.0; SHIFTER_HALF_FFT]; 2],
            frequencies: [[0.0; SHIFTER_HALF_FFT]; 2],
            shifted: [[Complex32::new(0.0, 0.0); SHIFTER_HALF_FFT]; 2],
        }
    }

    fn shift(
        &mut self,
        left: &[f32; SHIFTER_INPUT_HOP],
        right: &[f32; SHIFTER_INPUT_HOP],
        semitones: f32,
        output_frames: usize,
        output: &mut [f32],
    ) {
        self.history.copy_within(SHIFTER_INPUT_HOP.., 0);
        for index in 0..SHIFTER_INPUT_HOP {
            self.history[SHIFTER_INPUT_TAIL + index] = Complex32::new(left[index], right[index]);
        }
        for (index, value) in self.spectrum.iter_mut().enumerate() {
            *value = self.history[index] * SHIFTER_INPUT_WINDOW[index];
        }
        let _ = cfft_1024(&mut self.spectrum);

        let input_phase_increment = SHIFTER_INPUT_HOP as f32 * 2.0 * PI / SHIFTER_FFT as f32;
        for bin in 0..SHIFTER_HALF_FFT {
            let negative = if bin == 0 { 0 } else { SHIFTER_FFT - bin };
            let positive = self.spectrum[bin];
            let conjugate_negative = self.spectrum[negative].conj();
            let channels = [
                (positive + conjugate_negative) * 0.5,
                (positive - conjugate_negative) * Complex32::new(0.0, -0.5),
            ];
            for (channel, value) in channels.into_iter().enumerate() {
                let magnitude = F32Ext::hypot(value.re, value.im);
                if magnitude <= f32::EPSILON {
                    self.magnitudes[channel][bin] = 0.0;
                    self.frequencies[channel][bin] = bin as f32;
                    continue;
                }
                let phase = F32Ext::atan2(value.im, value.re);
                let delta = phase - self.input_phase[channel][bin];
                self.input_phase[channel][bin] = phase;
                self.magnitudes[channel][bin] = magnitude;
                self.frequencies[channel][bin] = bin as f32
                    + wrap_phase(delta - bin as f32 * input_phase_increment)
                        / input_phase_increment;
            }
        }

        let shift_factor = F32Ext::powf(2.0, semitones / 12.0);
        let output_phase_increment = output_frames as f32 * 2.0 * PI / SHIFTER_FFT as f32;
        for channel in 0..2 {
            for bin in 0..SHIFTER_HALF_FFT {
                let source = bin as f32 / shift_factor;
                let magnitude = interpolate(&self.magnitudes[channel], source);
                let frequency = interpolate(&self.frequencies[channel], source) * shift_factor;
                let phase = wrap_phase(
                    self.output_phase[channel][bin] + frequency * output_phase_increment,
                );
                self.output_phase[channel][bin] = phase;
                let (sine, cosine) = F32Ext::sin_cos(phase);
                self.shifted[channel][bin] = Complex32::new(cosine * magnitude, sine * magnitude);
            }
            self.shifted[channel][0] = Complex32::new(0.0, 0.0);
            self.shifted[channel][SHIFTER_HALF_FFT - 1] = Complex32::new(0.0, 0.0);
        }

        self.spectrum.fill(Complex32::new(0.0, 0.0));
        for bin in 0..SHIFTER_HALF_FFT {
            let left = self.shifted[0][bin];
            let right = self.shifted[1][bin];
            self.spectrum[bin] = left + Complex32::new(-right.im, right.re);
            if bin != 0 {
                self.spectrum[SHIFTER_FFT - bin] = left.conj() + Complex32::new(right.im, right.re);
            }
        }
        let _ = ifft_1024(&mut self.spectrum);

        self.overlap.copy_within(output_frames.., 0);
        self.overlap[SHIFTER_FFT - output_frames..].fill(Complex32::new(0.0, 0.0));
        for (index, window) in SHIFTER_OUTPUT_WINDOW.iter().copied().enumerate() {
            self.overlap[index] += self.spectrum[index] * (window * output_frames as f32);
        }
        for (index, pair) in output.chunks_exact_mut(2).take(output_frames).enumerate() {
            pair[0] = self.overlap[index].re;
            pair[1] = self.overlap[index].im;
        }
    }

    fn reset(&mut self) {
        self.history.fill(Complex32::new(0.0, 0.0));
        self.spectrum.fill(Complex32::new(0.0, 0.0));
        self.overlap.fill(Complex32::new(0.0, 0.0));
        self.input_phase = [[0.0; SHIFTER_HALF_FFT]; 2];
        self.output_phase = [[0.0; SHIFTER_HALF_FFT]; 2];
        self.magnitudes = [[0.0; SHIFTER_HALF_FFT]; 2];
        self.frequencies = [[0.0; SHIFTER_HALF_FFT]; 2];
        self.shifted = [[Complex32::new(0.0, 0.0); SHIFTER_HALF_FFT]; 2];
    }
}

fn interpolate(values: &[f32], index: f32) -> f32 {
    let low = index.trunc() as usize;
    let fraction = index.fract();
    let a = values.get(low).copied().unwrap_or(0.0);
    let b = values.get(low + 1).copied().unwrap_or(0.0);
    a * (1.0 - fraction) + b * fraction
}

fn wrap_phase(value: f32) -> f32 {
    F32Ext::rem_euclid(value + PI, 2.0 * PI) - PI
}

const fn shifter_input_window() -> [f32; SHIFTER_FFT] {
    let mut output = [0.0; SHIFTER_FFT];
    let mut index = 0;
    while index < SHIFTER_FFT {
        output[index] = 0.5 - 0.5 * cosine_approx(index as f32 * 2.0 / 1023.0);
        index += 1;
    }
    output
}

const fn shifter_output_window() -> [f32; SHIFTER_FFT] {
    let input = shifter_input_window();
    let mut sum = 0.0;
    let mut index = 0;
    while index < SHIFTER_FFT {
        sum += input[index] * input[index];
        index += 1;
    }
    let mut output = [0.0; SHIFTER_FFT];
    index = 0;
    while index < SHIFTER_FFT {
        output[index] = input[index] / sum;
        index += 1;
    }
    output
}

const fn cosine_approx(mut pi_units: f32) -> f32 {
    pi_units *= 0.5;
    pi_units -= 0.25 + floor_const(pi_units + 0.25);
    pi_units *= 16.0 * (pi_units.abs() - 0.5);
    pi_units + 0.225 * pi_units * (pi_units.abs() - 1.0)
}

const fn floor_const(value: f32) -> f32 {
    let mut rounded = value as i32 as f32;
    if value < rounded {
        rounded -= 1.0;
    }
    rounded
}

impl PhaseVocoderTimescale {
    fn new(config: crust::filters::Timescale) -> Result<Self, AudioFrameError> {
        if !config.has_supported_duration_ratio() {
            return Err(AudioFrameError::InvalidResamplerConfiguration(
                "combined timescale speed and rate are outside the supported range",
            ));
        }
        let output_frames_per_hop = TIMESCALE_INPUT_FRAMES as f64 / config.duration_ratio();
        Ok(Self {
            shifter: Box::new(StereoShifter::new()),
            input: [0.0; TIMESCALE_INPUT_SAMPLES],
            input_frames: 0,
            output: [0.0; TIMESCALE_MAX_OUTPUT_SAMPLES],
            output_offset: 0,
            output_len: 0,
            output_frames_per_hop,
            output_hop_residual: 0.0,
            shift_semitones: ((config.pitch * config.rate).log2() * 12.0) as f32,
            finished: false,
        })
    }

    fn generate(&mut self) {
        self.output_hop_residual += self.output_frames_per_hop;
        let output_frames = self.output_hop_residual.floor() as usize;
        self.output_hop_residual -= output_frames as f64;
        debug_assert!((1..=TIMESCALE_MAX_OUTPUT_FRAMES).contains(&output_frames));
        let mut left_input = [0.0; TIMESCALE_INPUT_FRAMES];
        let mut right_input = [0.0; TIMESCALE_INPUT_FRAMES];
        for (index, pair) in self.input.chunks_exact(2).enumerate() {
            left_input[index] = pair[0];
            right_input[index] = pair[1];
        }
        self.shifter.shift(
            &left_input,
            &right_input,
            self.shift_semitones,
            output_frames,
            &mut self.output[..output_frames * 2],
        );
        self.output_offset = 0;
        self.output_len = output_frames * 2;
        self.input.fill(0.0);
        self.input_frames = 0;
    }

    fn drain(&mut self, output: &mut [f32]) -> usize {
        let count = (self.output_len - self.output_offset).min(output.len()) / 2 * 2;
        output[..count]
            .copy_from_slice(&self.output[self.output_offset..self.output_offset + count]);
        self.output_offset += count;
        if self.output_offset == self.output_len {
            self.output_offset = 0;
            self.output_len = 0;
        }
        count
    }
}

impl StreamingPcmProcessor for PhaseVocoderTimescale {
    fn process(
        &mut self,
        input: &[f32],
        output: &mut [f32],
    ) -> Result<StreamingPcmProgress, AudioFrameError> {
        let mut produced = self.drain(output);
        let mut consumed = 0;
        if produced == 0 && !input.is_empty() && self.output_len == 0 {
            let frames = (input.len() / 2).min(TIMESCALE_INPUT_FRAMES - self.input_frames);
            let samples = frames * 2;
            let destination = self.input_frames * 2;
            self.input[destination..destination + samples].copy_from_slice(&input[..samples]);
            self.input_frames += frames;
            consumed = samples;
            if self.input_frames == TIMESCALE_INPUT_FRAMES {
                self.generate();
                produced = self.drain(output);
            }
        }
        Ok(StreamingPcmProgress::new(consumed, produced))
    }

    fn finish(&mut self, output: &mut [f32]) -> Result<usize, AudioFrameError> {
        let produced = self.drain(output);
        if produced != 0 {
            return Ok(produced);
        }
        if !self.finished && self.input_frames != 0 {
            self.generate();
            self.finished = true;
            return Ok(self.drain(output));
        }
        self.finished = true;
        Ok(0)
    }

    fn reset(&mut self) {
        self.shifter.reset();
        self.input.fill(0.0);
        self.input_frames = 0;
        self.output.fill(0.0);
        self.output_offset = 0;
        self.output_len = 0;
        self.output_hop_residual = 0.0;
        self.finished = false;
    }
}

struct Tremolo {
    frequency: f32,
    depth: f32,
    phase: [f32; 2],
    sample_rate: f32,
    channels: usize,
}

impl Tremolo {
    fn new(config: Modulation, format: PcmFormat) -> Self {
        Self {
            frequency: config.frequency,
            depth: config.depth / 2.0,
            phase: [0.0; 2],
            sample_rate: format.sample_rate() as f32,
            channels: usize::from(format.channels()),
        }
    }
}

impl PcmFilter for Tremolo {
    fn process(&mut self, frame: &mut PcmFrame) -> Result<(), AudioFrameError> {
        let increment = 2.0 * PI / self.sample_rate * self.frequency;
        for channel in 0..self.channels {
            for sample in frame.samples_mut()[channel..]
                .iter_mut()
                .step_by(self.channels)
            {
                let signal = 1.0 - self.depth + self.depth * self.phase[channel].sin();
                *sample *= signal;
                self.phase[channel] += increment;
            }
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.phase = [0.0; 2];
    }
}

const VIBRATO_BUFFER: usize = 195;

struct Vibrato {
    frequency: f32,
    depth: f32,
    phase: [f32; 2],
    buffers: [[f32; VIBRATO_BUFFER]; 2],
    write: [usize; 2],
    sample_rate: f32,
    channels: usize,
}

impl Vibrato {
    fn new(config: Modulation, format: PcmFormat) -> Self {
        Self {
            frequency: config.frequency,
            depth: config.depth,
            phase: [0.0; 2],
            buffers: [[0.0; VIBRATO_BUFFER]; 2],
            write: [0; 2],
            sample_rate: format.sample_rate() as f32,
            channels: usize::from(format.channels()),
        }
    }

    fn sample(&mut self, channel: usize, input: f32) -> f32 {
        let max_delay = 0.002 * self.sample_rate;
        let lfo = (self.phase[channel].sin() + 1.0) * 0.5;
        self.phase[channel] =
            (self.phase[channel] + 2.0 * PI * self.frequency / self.sample_rate) % (2.0 * PI);
        let delay = lfo * self.depth * max_delay + 3.0;
        let size = VIBRATO_BUFFER - 3;
        let read = (self.write[channel] as f32 - 1.0 - delay).rem_euclid(size as f32);
        let index = read as usize;
        let fraction = read - index as f32;
        let data = &self.buffers[channel];
        let y0 = data[index];
        let y1 = data[index + 1];
        let y2 = data[index + 2];
        let y3 = data[index + 3];
        let c1 = 0.5 * (y2 - y0);
        let c2 = y0 - 2.5 * y1 + 2.0 * y2 - 0.5 * y3;
        let c3 = 0.5 * (y3 - y0) + 1.5 * (y1 - y2);
        let output = ((c3 * fraction + c2) * fraction + c1) * fraction + y1;
        let write = self.write[channel];
        self.buffers[channel][write] = input;
        if write < 3 {
            self.buffers[channel][size + write] = input;
        }
        self.write[channel] = (write + 1) % size;
        output
    }
}

impl PcmFilter for Vibrato {
    fn process(&mut self, frame: &mut PcmFrame) -> Result<(), AudioFrameError> {
        for pair in frame.samples_mut().chunks_exact_mut(self.channels) {
            for (channel, sample) in pair.iter_mut().enumerate() {
                *sample = self.sample(channel, *sample);
            }
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.phase = [0.0; 2];
        self.buffers = [[0.0; VIBRATO_BUFFER]; 2];
        self.write = [0; 2];
    }
}

struct Distortion(crust::filters::Distortion);

impl PcmFilter for Distortion {
    fn process(&mut self, frame: &mut PcmFrame) -> Result<(), AudioFrameError> {
        let config = self.0;
        for sample in frame.samples_mut() {
            let input = *sample;
            let sin = config.sin_offset + (input * config.sin_scale).sin();
            let cos = config.cos_offset + (input * config.cos_scale).cos();
            let tan = config.tan_offset + (input * config.tan_scale).tan();
            *sample = (config.offset + config.scale * sin * cos * tan).clamp(-1.0, 1.0);
        }
        Ok(())
    }

    fn reset(&mut self) {}
}

struct Rotation {
    phase: f64,
    increment: f64,
}

impl Rotation {
    fn new(rotation_hz: f64, sample_rate: u32) -> Self {
        Self {
            phase: 0.0,
            increment: rotation_hz * 2.0 * std::f64::consts::PI / f64::from(sample_rate),
        }
    }
}

impl PcmFilter for Rotation {
    fn process(&mut self, frame: &mut PcmFrame) -> Result<(), AudioFrameError> {
        if frame.channels() != 2 {
            return Ok(());
        }
        for pair in frame.samples_mut().chunks_exact_mut(2) {
            let sine = self.phase.sin() as f32;
            pair[0] *= (sine + 1.0) / 2.0;
            pair[1] *= (-sine + 1.0) / 2.0;
            self.phase += self.increment;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.phase = 0.0;
    }
}

struct ChannelMix(crust::filters::ChannelMix);

impl PcmFilter for ChannelMix {
    fn process(&mut self, frame: &mut PcmFrame) -> Result<(), AudioFrameError> {
        if frame.channels() != 2 {
            return Ok(());
        }
        for pair in frame.samples_mut().chunks_exact_mut(2) {
            let left = pair[0];
            let right = pair[1];
            pair[0] = (self.0.left_to_left * left + self.0.right_to_left * right).clamp(-1.0, 1.0);
            pair[1] =
                (self.0.left_to_right * left + self.0.right_to_right * right).clamp(-1.0, 1.0);
        }
        Ok(())
    }

    fn reset(&mut self) {}
}

struct LowPass {
    smoothing: f32,
    values: [f32; 2],
    initialized: [bool; 2],
    channels: usize,
}

impl LowPass {
    fn new(smoothing: f32, channels: u16) -> Self {
        Self {
            smoothing,
            values: [0.0; 2],
            initialized: [false; 2],
            channels: usize::from(channels),
        }
    }
}

impl PcmFilter for LowPass {
    fn process(&mut self, frame: &mut PcmFrame) -> Result<(), AudioFrameError> {
        for pair in frame.samples_mut().chunks_exact_mut(self.channels) {
            for (channel, sample) in pair.iter_mut().enumerate() {
                if !self.initialized[channel] {
                    self.values[channel] = *sample;
                    self.initialized[channel] = true;
                }
                self.values[channel] += (*sample - self.values[channel]) / self.smoothing;
                *sample = self.values[channel];
            }
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.values = [0.0; 2];
        self.initialized = [false; 2];
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crust::filters::{
        ChannelMix as MixConfig, Distortion as DistortionConfig, FilterConfiguration, Karaoke,
        Modulation, Timescale,
    };
    use mantle_audio::{
        AudioFrameError, COMPATIBLE_PCM_SAMPLES, COMPATIBLE_SAMPLE_RATE, FilterPipeline, PcmFormat,
        PcmFrame, StreamingPcmPoll,
    };

    use super::{
        ChannelMix, CrustFilterFactory, Distortion, KaraokeFilter, LowPass, PcmFilter, Rotation,
        TimescaleFilter, Tremolo, Vibrato, Volume,
    };

    struct CountingAllocator;

    thread_local! {
        static COUNT_THIS_THREAD: Cell<bool> = const { Cell::new(false) };
    }

    static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    #[allow(unsafe_code)]
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            COUNT_THIS_THREAD.with(|enabled| {
                if enabled.get() {
                    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
                }
            });
            // SAFETY: The allocation request is forwarded unchanged to the system allocator.
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            // SAFETY: The pointer and layout came from the system allocator through this adapter.
            unsafe { System.dealloc(pointer, layout) };
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            COUNT_THIS_THREAD.with(|enabled| {
                if enabled.get() {
                    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
                }
            });
            // SAFETY: The allocation request is forwarded unchanged to the system allocator.
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            COUNT_THIS_THREAD.with(|enabled| {
                if enabled.get() {
                    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
                }
            });
            // SAFETY: The original allocation and new size are forwarded unchanged.
            unsafe { System.realloc(pointer, layout, size) }
        }
    }

    fn count_allocations(operation: impl FnOnce()) -> usize {
        ALLOCATIONS.store(0, Ordering::Relaxed);
        COUNT_THIS_THREAD.with(|enabled| enabled.set(true));
        operation();
        COUNT_THIS_THREAD.with(|enabled| enabled.set(false));
        ALLOCATIONS.load(Ordering::Relaxed)
    }

    fn frame(samples: &[f32]) -> PcmFrame {
        let format = PcmFormat::new(COMPATIBLE_SAMPLE_RATE, 2).unwrap();
        let mut frame = PcmFrame::with_capacity(samples.len());
        frame
            .copy_from_interleaved(samples, format, Some(Duration::ZERO))
            .unwrap();
        frame
    }

    #[test]
    fn karaoke_rejects_invalid_parameters_and_boundary_filters_remain_finite() {
        let valid = Karaoke {
            level: 1.0,
            mono_level: 1.0,
            filter_band: 220.0,
            filter_width: 100.0,
        };
        for invalid in [
            Karaoke {
                filter_width: 1_000_000.0,
                ..valid
            },
            Karaoke {
                filter_width: f32::NAN,
                ..valid
            },
            Karaoke {
                filter_band: f32::INFINITY,
                ..valid
            },
            Karaoke {
                filter_band: -1.0,
                ..valid
            },
            Karaoke {
                level: 1.01,
                ..valid
            },
            Karaoke {
                mono_level: -0.01,
                ..valid
            },
        ] {
            assert!(matches!(
                KaraokeFilter::new(invalid, 48_000),
                Err(AudioFrameError::InvalidFilterConfiguration(_))
            ));
        }
        for band in [0.0, 220.0, 24_000.0] {
            for width in [0.0, 100.0, 24_000.0] {
                let mut filter = KaraokeFilter::new(
                    Karaoke {
                        filter_band: band,
                        filter_width: width,
                        ..valid
                    },
                    48_000,
                )
                .unwrap();
                let mut pcm = frame(&[0.25; mantle_audio::COMPATIBLE_PCM_SAMPLES]);
                for _ in 0..500 {
                    pcm.samples_mut().fill(0.25);
                    filter.process(&mut pcm).unwrap();
                    assert!(
                        pcm.samples().iter().all(|s| s.is_finite()),
                        "band={band} width={width}"
                    );
                }
            }
        }
    }

    #[test]
    fn player_volume_matches_frozen_lavaplayer_signed_pcm_oracle_without_allocations() {
        // Lavaplayer 2.2.6 PcmVolumeProcessor.applyVolume(100, volume, samples),
        // executed independently against the reference JAR on 2026-09-05.
        for (volume, expected) in [
            (0, [0, 0, 0, 0]),
            (37, [4929, -4929, 9859, -9859]),
            (70, [10112, -10112, 20223, -20224]),
            (100, [16384, -16384, 32767, -32768]),
            (150, [32767, -32768, 32767, -32768]),
            (200, [32767, -32768, 32767, -32768]),
        ] {
            let mut input = frame(&[0.5, -0.5, 32767.0 / 32768.0, -1.0]);
            let mut filter = super::PlayerVolume(mantle_audio::VolumeLevel::new(volume));
            assert_eq!(count_allocations(|| filter.process(&mut input).unwrap()), 0);
            for (actual, expected) in input.samples().iter().zip(expected) {
                assert_eq!(
                    *actual,
                    f32::from(i16::try_from(expected).unwrap()) / 32768.0,
                    "volume {volume}"
                );
            }
        }
    }

    #[test]
    fn fixed_filter_golden_vectors_match_lavadsp_formulas() {
        let mut volume_frame = frame(&[0.5, -0.5]);
        Volume(0.5).process(&mut volume_frame).unwrap();
        let multiplier = (0.5_f32 * 0.79).tan();
        assert!((volume_frame.samples()[0] - 0.5 * multiplier).abs() < 1.0e-6);

        let mut mix_frame = frame(&[0.8, -0.4]);
        ChannelMix(MixConfig {
            left_to_left: 0.5,
            left_to_right: 0.25,
            right_to_left: 0.5,
            right_to_right: 0.75,
        })
        .process(&mut mix_frame)
        .unwrap();
        assert!((mix_frame.samples()[0] - 0.2).abs() < 1.0e-6);
        assert!((mix_frame.samples()[1] + 0.1).abs() < 1.0e-6);

        let mut low_pass = LowPass::new(2.0, 2);
        let mut low_pass_frame = frame(&[1.0, -1.0, 0.0, 0.0]);
        low_pass.process(&mut low_pass_frame).unwrap();
        assert_eq!(low_pass_frame.samples(), &[1.0, -1.0, 0.5, -0.5]);
        low_pass.reset();
        let mut after_reset = frame(&[0.25, 0.75]);
        low_pass.process(&mut after_reset).unwrap();
        assert_eq!(after_reset.samples(), &[0.25, 0.75]);
    }

    #[test]
    fn remaining_fixed_filter_golden_vectors_match_lavadsp_formulas() {
        let format = PcmFormat::new(COMPATIBLE_SAMPLE_RATE, 2).unwrap();

        let mut karaoke = KaraokeFilter::new(
            Karaoke {
                level: 1.0,
                mono_level: 1.0,
                filter_band: 220.0,
                filter_width: 100.0,
            },
            COMPATIBLE_SAMPLE_RATE,
        )
        .unwrap();
        let mut karaoke_frame = frame(&[1.0, 1.0]);
        karaoke.process(&mut karaoke_frame).unwrap();
        assert!((karaoke_frame.samples()[0] - 0.000_383_999_43).abs() < 1.0e-7);
        assert_eq!(karaoke_frame.samples()[0], karaoke_frame.samples()[1]);

        let mut tremolo = Tremolo::new(
            Modulation {
                frequency: 12_000.0,
                depth: 0.8,
            },
            format,
        );
        let mut tremolo_frame = frame(&[1.0, -1.0, 1.0, -1.0]);
        tremolo.process(&mut tremolo_frame).unwrap();
        assert!((tremolo_frame.samples()[0] - 0.6).abs() < 1.0e-6);
        assert!((tremolo_frame.samples()[1] + 0.6).abs() < 1.0e-6);
        assert!((tremolo_frame.samples()[2] - 1.0).abs() < 1.0e-6);
        assert!((tremolo_frame.samples()[3] + 1.0).abs() < 1.0e-6);

        let mut rotation = Rotation::new(12_000.0, COMPATIBLE_SAMPLE_RATE);
        let mut rotation_frame = frame(&[1.0, 1.0, 1.0, 1.0]);
        rotation.process(&mut rotation_frame).unwrap();
        assert_eq!(rotation_frame.samples()[0], 0.5);
        assert_eq!(rotation_frame.samples()[1], 0.5);
        assert_eq!(rotation_frame.samples()[2], 1.0);
        assert!(rotation_frame.samples()[3].abs() < 1.0e-7);

        let mut distortion = Distortion(DistortionConfig {
            sin_offset: 2.0,
            sin_scale: 0.0,
            cos_offset: 0.0,
            cos_scale: 0.0,
            tan_offset: 1.0,
            tan_scale: 0.0,
            offset: 0.0,
            scale: 0.25,
        });
        let mut distortion_frame = frame(&[-0.75, 0.25]);
        distortion.process(&mut distortion_frame).unwrap();
        assert_eq!(distortion_frame.samples(), &[0.5, 0.5]);

        let mut vibrato = Vibrato::new(
            Modulation {
                frequency: 2.0,
                depth: 0.5,
            },
            format,
        );
        let impulse = {
            let mut samples = [0.0; 128];
            samples[0] = 1.0;
            samples[1] = -1.0;
            samples
        };
        let mut first = frame(&impulse);
        vibrato.process(&mut first).unwrap();
        assert!(first.samples()[..48].iter().all(|sample| *sample == 0.0));
        assert!(first.samples()[48..].iter().any(|sample| *sample != 0.0));
        vibrato.reset();
        let mut after_reset = frame(&impulse);
        vibrato.process(&mut after_reset).unwrap();
        assert_eq!(first.samples(), after_reset.samples());
    }

    fn direct_timescale_output(config: Timescale, hops: usize) -> (usize, f32, Vec<f32>) {
        use mantle_audio::StreamingPcmProcessor as _;

        let shift_semitones = ((config.pitch * config.rate).log2() * 12.0) as f32;
        let mut processor = TimescaleFilter::new(config).unwrap();
        let mut input = [0.0; 256];
        let mut output = [0.0; 2046];
        let mut produced_frames = 0;
        let mut left_output = Vec::new();
        for hop in 0..hops {
            for frame in 0..128 {
                let value = (2.0 * std::f32::consts::PI * 440.0 * (hop * 128 + frame) as f32
                    / COMPATIBLE_SAMPLE_RATE as f32)
                    .sin();
                input[frame * 2] = value;
                input[frame * 2 + 1] = value;
            }
            let progress = processor.process(&input, &mut output).unwrap();
            assert_eq!(progress.consumed_samples, input.len());
            produced_frames += progress.produced_samples / 2;
            left_output.extend(
                output[..progress.produced_samples]
                    .chunks_exact(2)
                    .map(|pair| pair[0]),
            );
        }
        loop {
            let produced = processor.finish(&mut output).unwrap();
            if produced == 0 {
                break;
            }
            produced_frames += produced / 2;
            left_output.extend(output[..produced].chunks_exact(2).map(|pair| pair[0]));
        }
        (produced_frames, shift_semitones, left_output)
    }

    fn measured_frequency(samples: &[f32]) -> f32 {
        let start = 2_048.min(samples.len());
        let end = samples.len().saturating_sub(1_024).max(start);
        let samples = &samples[start..end];
        let crossings = samples
            .windows(2)
            .filter(|pair| pair[0] <= 0.0 && pair[1] > 0.0)
            .count();
        crossings as f32 * COMPATIBLE_SAMPLE_RATE as f32 / samples.len() as f32
    }

    #[test]
    fn timescale_speed_pitch_and_rate_are_independent_and_fractionally_paced() {
        let (speed_frames, speed_shift, speed_output) = direct_timescale_output(
            Timescale {
                speed: 2.0,
                pitch: 1.0,
                rate: 1.0,
            },
            100,
        );
        let (pitch_frames, pitch_shift, pitch_output) = direct_timescale_output(
            Timescale {
                speed: 1.0,
                pitch: 2.0,
                rate: 1.0,
            },
            100,
        );
        let (rate_frames, rate_shift, rate_output) = direct_timescale_output(
            Timescale {
                speed: 1.0,
                pitch: 1.0,
                rate: 2.0,
            },
            100,
        );
        let (fractional_frames, _, _) = direct_timescale_output(
            Timescale {
                speed: 1.3,
                pitch: 1.0,
                rate: 1.0,
            },
            100,
        );

        assert_eq!(speed_frames, 6_400);
        assert_eq!(pitch_frames, 12_800);
        assert_eq!(rate_frames, 6_400);
        assert_eq!(fractional_frames, (12_800.0_f64 / 1.3).round() as usize);
        assert_eq!(speed_shift, 0.0);
        assert!((pitch_shift - 12.0).abs() < 1.0e-6);
        assert!((rate_shift - 12.0).abs() < 1.0e-6);
        assert!((measured_frequency(&speed_output) - 440.0).abs() < 15.0);
        assert!((measured_frequency(&pitch_output) - 880.0).abs() < 30.0);
        assert!((measured_frequency(&rate_output) - 880.0).abs() < 30.0);
    }

    #[test]
    fn wsola_finish_flushes_exact_duration_at_speed_boundaries() {
        use mantle_audio::StreamingPcmProcessor as _;

        const SOURCE_FRAMES: usize = 37 * 128;
        let input = [0.125; 256];
        let mut output = [0.0; 2046];

        for speed in [0.25, 4.0] {
            let mut processor = TimescaleFilter::new(Timescale {
                speed,
                pitch: 1.0,
                rate: 1.0,
            })
            .unwrap();
            let mut consumed_frames = 0;
            let mut produced_frames = 0;
            while consumed_frames < SOURCE_FRAMES {
                let offered_frames = (SOURCE_FRAMES - consumed_frames).min(input.len() / 2);
                let progress = processor
                    .process(&input[..offered_frames * 2], &mut output)
                    .unwrap();
                assert!(progress.consumed_samples <= offered_frames * 2);
                assert!(progress.produced_samples <= output.len());
                assert!(progress.consumed_samples != 0 || progress.produced_samples != 0);
                consumed_frames += progress.consumed_samples / 2;
                produced_frames += progress.produced_samples / 2;
            }

            let mut finish_calls = 0;
            loop {
                let produced = processor.finish(&mut output).unwrap();
                if produced == 0 {
                    break;
                }
                finish_calls += 1;
                produced_frames += produced / 2;
            }

            assert!(finish_calls > 0, "speed {speed} did not flush at EOF");
            assert_eq!(
                produced_frames,
                (SOURCE_FRAMES as f64 / speed).round() as usize,
                "wrong flushed duration at speed {speed}"
            );
            assert_eq!(processor.finish(&mut output).unwrap(), 0);
        }
    }

    #[test]
    fn timescale_reset_replacement_removal_and_ratio_bounds_are_deterministic() {
        use crust::filters::{MAX_TIMESCALE_DURATION_RATIO, MIN_TIMESCALE_DURATION_RATIO};
        use mantle_audio::StreamingPcmProcessor as _;

        for ratio in [
            MIN_TIMESCALE_DURATION_RATIO,
            1.0,
            MAX_TIMESCALE_DURATION_RATIO,
        ] {
            assert!(
                TimescaleFilter::new(Timescale {
                    speed: ratio,
                    pitch: 1.0,
                    rate: 1.0,
                })
                .is_ok()
            );
        }
        for ratio in [
            MIN_TIMESCALE_DURATION_RATIO / 2.0,
            MAX_TIMESCALE_DURATION_RATIO * 2.0,
        ] {
            assert!(
                TimescaleFilter::new(Timescale {
                    speed: ratio,
                    pitch: 1.0,
                    rate: 1.0,
                })
                .is_err()
            );
        }

        let config = Timescale {
            speed: 1.0,
            pitch: 1.1,
            rate: 1.0,
        };
        let mut processor = TimescaleFilter::new(config).unwrap();
        let partial = [1.0; 128];
        let mut output = [0.0; 2046];
        assert_eq!(
            processor
                .process(&partial, &mut output)
                .unwrap()
                .produced_samples,
            0
        );
        processor.reset();
        let zeros = [0.0; 256];
        let progress = processor.process(&zeros, &mut output).unwrap();
        assert_eq!(progress.produced_samples, 256);
        assert!(
            output[..progress.produced_samples]
                .iter()
                .all(|sample| sample.is_finite() && sample.abs() < 1.0e-6),
            "post-reset output: {:?}",
            &output[..16]
        );

        let format = PcmFormat::new(COMPATIBLE_SAMPLE_RATE, 2).unwrap();
        let factory = CrustFilterFactory(FilterConfiguration {
            timescale: Some(Timescale {
                speed: 2.0,
                ..config
            }),
            ..FilterConfiguration::default()
        });
        let mut pipeline = FilterPipeline::new(format, 32).unwrap();
        pipeline.install_factory(Some(&factory)).unwrap();
        assert!(pipeline.has_streaming_processor());
        pipeline.install_factory(None).unwrap();
        assert!(!pipeline.has_streaming_processor());
    }

    #[test]
    fn crust_timescale_allocates_zero_times_after_construction() {
        use mantle_audio::StreamingPcmProcessor as _;

        let mut processor = TimescaleFilter::new(Timescale {
            speed: 0.75,
            pitch: 1.1,
            rate: 1.0,
        })
        .unwrap();
        let input = [0.125; 256];
        let mut output = [0.0; 2046];
        processor.process(&input, &mut output).unwrap();
        let allocations = count_allocations(|| {
            for _ in 0..1_000 {
                let progress = processor.process(&input, &mut output).unwrap();
                assert_eq!(progress.consumed_samples, input.len());
                assert!(progress.produced_samples <= output.len());
            }
        });
        assert_eq!(allocations, 0);

        let mut speed_only = TimescaleFilter::new(Timescale {
            speed: 1.1,
            pitch: 1.0,
            rate: 1.0,
        })
        .unwrap();
        let allocations = count_allocations(|| {
            for _ in 0..1_000 {
                let progress = speed_only.process(&input, &mut output).unwrap();
                assert!(progress.consumed_samples <= input.len());
                assert!(progress.produced_samples <= output.len());
            }
        });
        assert_eq!(allocations, 0);
    }

    #[test]
    #[ignore = "manual release-profile filter benchmark; run with --ignored --nocapture"]
    fn p09_filter_benchmark_report() {
        use std::hint::black_box;
        use std::time::Instant;

        use mantle_audio::StreamingPcmProcessor as _;

        const SOURCE_SECONDS: usize = 10;
        const SOURCE_FRAMES: usize = SOURCE_SECONDS * 48_000;
        let mut processor = TimescaleFilter::new(Timescale {
            speed: 1.1,
            pitch: 1.0,
            rate: 1.0,
        })
        .unwrap();
        let mut input = [0.0; 256];
        let mut output = [0.0; 2046];
        for (index, sample) in input.iter_mut().enumerate() {
            *sample = (index as f32 * 0.037).sin() * 0.2;
        }
        for _ in 0..32 {
            black_box(
                processor
                    .process(black_box(&input), black_box(&mut output))
                    .unwrap(),
            );
        }

        let mut runs = [0.0_f64; 5];
        let mut final_produced = 0_usize;
        for run in &mut runs {
            processor.reset();
            let started = Instant::now();
            let mut consumed_frames = 0_usize;
            let mut produced = 0_usize;
            while consumed_frames < SOURCE_FRAMES {
                let offered_frames = (SOURCE_FRAMES - consumed_frames).min(128);
                let progress = processor
                    .process(
                        black_box(&input[..offered_frames * 2]),
                        black_box(&mut output),
                    )
                    .unwrap();
                consumed_frames += progress.consumed_samples / 2;
                produced += progress.produced_samples;
            }
            loop {
                let count = processor.finish(black_box(&mut output)).unwrap();
                if count == 0 {
                    break;
                }
                produced += count;
            }
            *run = started.elapsed().as_secs_f64() / SOURCE_SECONDS as f64 * 10.0;
            final_produced = produced;
        }
        runs.sort_by(f64::total_cmp);
        println!(
            "P09_BENCH {{\"sourceSecondsPerRun\":{SOURCE_SECONDS},\"runs\":5,\"producedSamples\":{final_produced},\"tenTrackEquivalentCoresMin\":{:.6},\"tenTrackEquivalentCoresMedian\":{:.6},\"tenTrackEquivalentCoresMax\":{:.6}}}",
            runs[0], runs[2], runs[4]
        );
    }

    fn output_frames(speed: f64) -> usize {
        let format = PcmFormat::new(COMPATIBLE_SAMPLE_RATE, 2).unwrap();
        let config = FilterConfiguration {
            timescale: Some(Timescale {
                speed,
                pitch: 1.0,
                rate: 1.0,
            }),
            ..FilterConfiguration::default()
        };
        let factory = CrustFilterFactory(config);
        let mut pipeline = FilterPipeline::new(format, 32).unwrap();
        pipeline.install_factory(Some(&factory)).unwrap();
        let mut input = PcmFrame::with_capacity(COMPATIBLE_PCM_SAMPLES);
        let mut output = PcmFrame::with_capacity(COMPATIBLE_PCM_SAMPLES);
        let mut frames = 0;
        for index in 0..50_u64 {
            let samples = input
                .prepare(
                    COMPATIBLE_PCM_SAMPLES,
                    format,
                    Some(Duration::from_millis(index * 20)),
                )
                .unwrap();
            for (sample_index, sample) in samples.iter_mut().enumerate() {
                *sample = ((sample_index / 2 + index as usize * 960) as f32 * 0.01).sin() * 0.2;
            }
            pipeline.submit_input(&input).unwrap();
            loop {
                match pipeline.read_output(&mut output).unwrap() {
                    StreamingPcmPoll::Frame => frames += 1,
                    StreamingPcmPoll::NeedInput => break,
                    StreamingPcmPoll::Finished => panic!("finished before EOF"),
                }
            }
        }
        pipeline.finish_input();
        loop {
            match pipeline.read_output(&mut output).unwrap() {
                StreamingPcmPoll::Frame => frames += 1,
                StreamingPcmPoll::Finished => break,
                StreamingPcmPoll::NeedInput => panic!("requested input after EOF"),
            }
        }
        frames
    }

    #[test]
    fn phase_vocoder_timescale_changes_stream_duration() {
        let fast = output_frames(2.0);
        let slow = output_frames(0.5);
        assert!(fast < 35, "fast output frames: {fast}");
        assert!(slow > 75, "slow output frames: {slow}");
    }
}
