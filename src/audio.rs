use anyhow::{Context, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{HostId, SampleFormat, StreamConfig};
use rubato::{
    Resampler as RubatoResampler, SincFixedIn, SincInterpolationParameters, SincInterpolationType,
    WindowFunction,
};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};

pub const TARGET_CHANNELS: u16 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResamplerMode {
    Linear,
    SincFast,
    SincQuality,
}

impl ResamplerMode {
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_lowercase().as_str() {
            "linear" | "basic" => Some(Self::Linear),
            "sinc" | "rubato" | "quality" | "hq" => Some(Self::SincQuality),
            "sinc-fast" | "fast" | "medium" => Some(Self::SincFast),
            _ => None,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Linear => "linear",
            Self::SincFast => "sinc-fast",
            Self::SincQuality => "sinc",
        }
    }
}

pub struct CaptureSession {
    pub receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    pub error_receiver: mpsc::Receiver<String>,
    pub stream: cpal::Stream,
    pub sample_rate: u32,
    pub channels: u16,
    pub format: SampleFormat,
    pub observed_rate: Arc<Mutex<Option<u32>>>,
}

pub fn list_input_device_details() -> Result<Vec<crate::models::CaptureDeviceInfo>> {
    let host = select_host()?;
    let devices = host.input_devices().context("enumerate input devices")?;
    let mut results = Vec::new();
    for device in devices {
        let name = device
            .name()
            .unwrap_or_else(|_| "Unknown Device".to_string());
        let mut channels = 0u16;
        let mut rates = BTreeSet::new();
        if let Ok(configs) = device.supported_input_configs() {
            for config in configs {
                channels = channels.max(config.channels());
                let min = config.min_sample_rate().0;
                let max = config.max_sample_rate().0;
                rates.insert(min);
                rates.insert(max);
            }
        }
        results.push(crate::models::CaptureDeviceInfo {
            id: name.clone(),
            name,
            channels,
            sample_rates: rates.into_iter().collect(),
        });
    }
    Ok(results)
}

pub fn start_capture(
    device_name: &str,
    target_rate: u32,
    resampler_mode: ResamplerMode,
) -> Result<CaptureSession> {
    let host = select_host()?;
    let device = host
        .input_devices()
        .context("enumerate input devices")?
        .find(|dev| dev.name().map(|name| name == device_name).unwrap_or(false))
        .context("capture device not found")?;

    let supported_configs = device
        .supported_input_configs()
        .context("read supported input configs")?;
    let mut selected = None;
    for config in supported_configs {
        if config.channels() != TARGET_CHANNELS {
            continue;
        }
        let min = config.min_sample_rate().0;
        let max = config.max_sample_rate().0;
        if target_rate >= min && target_rate <= max {
            selected = Some(config.with_sample_rate(cpal::SampleRate(target_rate)));
            break;
        }
    }

    let supported = match selected {
        Some(config) => config,
        None => device
            .default_input_config()
            .context("read default input config")?,
    };
    let sample_format = supported.sample_format();
    let config: StreamConfig = supported.into();

    let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (err_tx, err_rx) = mpsc::channel::<String>(4);
    let observed_rate = Arc::new(Mutex::new(None));
    let resampler = Arc::new(Mutex::new(Resampler::new(
        config.sample_rate.0,
        config.channels,
        target_rate,
        resampler_mode,
        Arc::clone(&observed_rate),
    )?));

    let err_fn = move |err| {
        let message = format!("capture error: {}", err);
        warn!("{}", message);
        let _ = err_tx.try_send(message);
    };

    let tx_f32 = tx.clone();
    let tx_i16 = tx.clone();
    let tx_u16 = tx.clone();
    let resampler_f32 = Arc::clone(&resampler);
    let resampler_i16 = Arc::clone(&resampler);
    let resampler_u16 = Arc::clone(&resampler);
    let stream = match sample_format {
        SampleFormat::F32 => device.build_input_stream(
            &config,
            move |data: &[f32], _| {
                handle_samples_f32(data, config.channels, &resampler_f32, tx_f32.clone());
            },
            err_fn,
            None,
        )?,
        SampleFormat::I16 => device.build_input_stream(
            &config,
            move |data: &[i16], _| {
                let mut buffer = Vec::with_capacity(data.len());
                for sample in data {
                    buffer.push(*sample as f32 / i16::MAX as f32);
                }
                handle_samples_f32(&buffer, config.channels, &resampler_i16, tx_i16.clone());
            },
            err_fn,
            None,
        )?,
        SampleFormat::U16 => device.build_input_stream(
            &config,
            move |data: &[u16], _| {
                let mut buffer = Vec::with_capacity(data.len());
                for sample in data {
                    let shifted = *sample as i32 - (i16::MAX as i32 + 1);
                    buffer.push(shifted as f32 / (i16::MAX as f32 + 1.0));
                }
                handle_samples_f32(&buffer, config.channels, &resampler_u16, tx_u16.clone());
            },
            err_fn,
            None,
        )?,
        _ => anyhow::bail!("unsupported sample format"),
    };

    stream.play().context("start capture stream")?;

    Ok(CaptureSession {
        receiver: rx,
        error_receiver: err_rx,
        stream,
        sample_rate: config.sample_rate.0,
        channels: config.channels,
        format: sample_format,
        observed_rate,
    })
}

fn select_host() -> Result<cpal::Host> {
    let hosts = cpal::available_hosts();
    if hosts.contains(&HostId::Alsa) {
        return cpal::host_from_id(HostId::Alsa).context("select ALSA host");
    }
    Ok(cpal::default_host())
}

fn handle_samples_f32(
    data: &[f32],
    channels: u16,
    resampler: &Arc<Mutex<Resampler>>,
    tx: mpsc::UnboundedSender<Vec<u8>>,
) {
    let output = {
        let mut resampler = match resampler.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        resampler.observe_input(data.len(), channels);
        if resampler.needs_resample_rate() {
            resampler.process(data, channels)
        } else {
            convert_direct_to_i16(data, channels)
        }
    };

    if output.is_empty() {
        return;
    }

    let mut bytes = Vec::with_capacity(output.len() * 2);
    for sample in output {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }

    let _ = tx.send(bytes);
}

fn convert_direct_to_i16(data: &[f32], channels: u16) -> Vec<i16> {
    if channels == TARGET_CHANNELS && data.len().is_multiple_of(2) {
        return data.iter().map(|s| f32_to_i16(*s)).collect();
    }

    let mut out = Vec::with_capacity(data.len());
    let mut idx = 0;
    while idx + channels as usize <= data.len() {
        let frame = &data[idx..idx + channels as usize];
        let (left, right) = map_channels(frame, channels);
        out.push(f32_to_i16(left));
        out.push(f32_to_i16(right));
        idx += channels as usize;
    }
    out
}

fn map_channels(frame: &[f32], channels: u16) -> (f32, f32) {
    match channels {
        0 => (0.0, 0.0),
        1 => (frame[0], frame[0]),
        _ => (frame[0], frame[1]),
    }
}

fn f32_to_i16(sample: f32) -> i16 {
    let clamped = sample.clamp(-1.0, 1.0);
    (clamped * i16::MAX as f32) as i16
}

struct Resampler {
    mode: ResamplerMode,
    in_rate: u32,
    target_rate: u32,
    linear: LinearResampler,
    sinc: Option<SincResampler>,
    rate_frames: u64,
    rate_start: Instant,
    last_rate_log: Instant,
    rate_pending: Option<u32>,
    rate_confirmations: u8,
    observed_rate: Arc<Mutex<Option<u32>>>,
}

/// How far the observed rate must sit from the rate we resample against before we believe the
/// device really runs at a different rate. This is a ratio, so 0.005 = 0.5%: well above a sane
/// card's error (~0.005%, i.e. 0.00005) and above this estimator's own noise, but still catches a
/// device that is genuinely mis-declared (e.g. a 44.1 kHz card reported as 48 kHz).
const RATE_DEADBAND_RATIO: f64 = 0.005;
/// Consecutive agreeing windows required outside the deadband before re-rating the resampler.
const RATE_CONFIRMATIONS: u8 = 3;

impl Resampler {
    fn new(
        in_rate: u32,
        in_channels: u16,
        target_rate: u32,
        mode: ResamplerMode,
        observed_rate: Arc<Mutex<Option<u32>>>,
    ) -> Result<Self> {
        let sinc = match mode {
            ResamplerMode::Linear => None,
            ResamplerMode::SincFast => Some(SincResampler::new(
                in_rate,
                target_rate,
                SincQuality::Fast,
                in_channels,
            )?),
            ResamplerMode::SincQuality => Some(SincResampler::new(
                in_rate,
                target_rate,
                SincQuality::Quality,
                in_channels,
            )?),
        };
        Ok(Self {
            mode,
            in_rate,
            target_rate,
            linear: LinearResampler::new(in_channels),
            sinc,
            rate_frames: 0,
            rate_start: Instant::now(),
            last_rate_log: Instant::now(),
            rate_pending: None,
            rate_confirmations: 0,
            observed_rate,
        })
    }

    fn needs_resample_rate(&self) -> bool {
        self.in_rate != self.target_rate
    }

    fn process(&mut self, input: &[f32], in_channels: u16) -> Vec<i16> {
        if input.is_empty() || in_channels == 0 {
            return Vec::new();
        }

        match self.mode {
            ResamplerMode::Linear => {
                self.linear
                    .process(input, in_channels, self.in_rate, self.target_rate)
            }
            ResamplerMode::SincFast | ResamplerMode::SincQuality => {
                if let Some(sinc) = self.sinc.as_mut() {
                    sinc.process(input, in_channels)
                } else {
                    Vec::new()
                }
            }
        }
    }

    fn observe_input(&mut self, samples: usize, in_channels: u16) {
        let frames = samples / in_channels as usize;
        self.rate_frames = self.rate_frames.saturating_add(frames as u64);
        let elapsed = self.rate_start.elapsed();
        if elapsed < Duration::from_secs(2) {
            return;
        }
        let observed = (self.rate_frames as f64 / elapsed.as_secs_f64()).round() as u32;
        self.rate_frames = 0;
        self.rate_start = Instant::now();
        if observed == 0 {
            return;
        }
        if let Ok(mut slot) = self.observed_rate.lock() {
            *slot = Some(observed);
        }

        // This counts frames handed to us by the capture callback over wall-clock time, so it
        // measures OUR throughput, not the card's clock: whenever the TCP send or the VAD stalls,
        // frames pile up in the driver buffer and the next batch reads as "faster than realtime".
        // A real card is good to a few Hz (measured: 47997.67 Hz on an ALSA hardware counter,
        // 0.005% off), which is far below this estimator's own noise floor. So treat it as
        // telemetry and only re-rate the resampler when the device is off by more than
        // RATE_DEADBAND_RATIO for RATE_CONFIRMATIONS consecutive windows -- resampling to a
        // mismeasured rate invents samples, overflowing the very buffer it claims to fix.
        let deviation = (observed as f64 - self.in_rate as f64).abs() / self.in_rate as f64;
        if deviation <= RATE_DEADBAND_RATIO {
            self.rate_pending = None;
            if self.last_rate_log.elapsed() >= Duration::from_secs(10) {
                info!(
                    "observed input rate: {} Hz (using {} Hz, target {} Hz, resampler={})",
                    observed,
                    self.in_rate,
                    self.target_rate,
                    self.mode.label()
                );
                self.last_rate_log = Instant::now();
            }
            return;
        }

        // Outside the deadband: require consecutive windows to agree before believing it, so a
        // single stall cannot re-rate the stream.
        let agrees = self.rate_pending.is_some_and(|pending| {
            (observed as f64 - pending as f64).abs() / pending as f64 <= RATE_DEADBAND_RATIO
        });
        if !agrees {
            self.rate_pending = Some(observed);
            self.rate_confirmations = 1;
            return;
        }
        self.rate_confirmations = self.rate_confirmations.saturating_add(1);
        if self.rate_confirmations < RATE_CONFIRMATIONS {
            return;
        }

        info!(
            "input rate changed: {} Hz (was {} Hz, {:.3}% off, target {} Hz, resampler={})",
            observed,
            self.in_rate,
            deviation * 100.0,
            self.target_rate,
            self.mode.label()
        );
        self.in_rate = observed;
        self.reset_resampler();
        self.rate_pending = None;
        self.rate_confirmations = 0;
        self.last_rate_log = Instant::now();
    }

    fn reset_resampler(&mut self) {
        match self.mode {
            ResamplerMode::Linear => {
                self.linear.reset();
            }
            ResamplerMode::SincFast => {
                if let Some(sinc) = self.sinc.as_mut() {
                    if let Err(err) = sinc.reset(self.in_rate, self.target_rate, SincQuality::Fast)
                    {
                        warn!("resampler reset failed: {}", err);
                    }
                }
            }
            ResamplerMode::SincQuality => {
                if let Some(sinc) = self.sinc.as_mut() {
                    if let Err(err) =
                        sinc.reset(self.in_rate, self.target_rate, SincQuality::Quality)
                    {
                        warn!("resampler reset failed: {}", err);
                    }
                }
            }
        }
    }
}

struct LinearResampler {
    pos: f64,
    buffer: Vec<f32>,
}

impl LinearResampler {
    fn new(in_channels: u16) -> Self {
        Self {
            pos: 0.0,
            buffer: Vec::with_capacity(in_channels as usize * 2048),
        }
    }

    fn reset(&mut self) {
        self.pos = 0.0;
        self.buffer.clear();
    }

    fn process(
        &mut self,
        input: &[f32],
        in_channels: u16,
        in_rate: u32,
        target_rate: u32,
    ) -> Vec<i16> {
        if input.is_empty() || in_channels == 0 {
            return Vec::new();
        }

        self.buffer.extend_from_slice(input);
        let in_channels_usize = in_channels as usize;
        let step = in_rate as f64 / target_rate as f64;
        let max_samples = in_channels_usize * target_rate as usize;

        if self.buffer.len() > max_samples {
            let drop_samples = self.buffer.len() - max_samples;
            let drop_frames = drop_samples / in_channels_usize;
            self.buffer.drain(0..drop_samples);
            self.pos = (self.pos - drop_frames as f64).max(0.0);
        }

        let available_frames = self.buffer.len() / in_channels_usize;
        let mut out = Vec::new();

        while self.pos + 1.0 < available_frames as f64 {
            let idx = self.pos.floor() as usize;
            let frac = self.pos - idx as f64;
            let base = idx * in_channels_usize;
            let next = (idx + 1) * in_channels_usize;

            let frame_a = &self.buffer[base..base + in_channels_usize];
            let frame_b = &self.buffer[next..next + in_channels_usize];

            let (left_a, right_a) = map_channels(frame_a, in_channels);
            let (left_b, right_b) = map_channels(frame_b, in_channels);

            let left = left_a + ((left_b - left_a) * frac as f32);
            let right = right_a + ((right_b - right_a) * frac as f32);

            out.push(f32_to_i16(left));
            out.push(f32_to_i16(right));

            self.pos += step;
        }

        let drop_frames = self.pos.floor() as usize;
        if drop_frames > 0 {
            let drop_samples = drop_frames * in_channels_usize;
            self.buffer.drain(0..drop_samples);
            self.pos -= drop_frames as f64;
        }

        out
    }
}

#[derive(Clone, Copy)]
enum SincQuality {
    Fast,
    Quality,
}

struct SincResampler {
    resampler: SincFixedIn<f32>,
    pending_left: Vec<f32>,
    pending_right: Vec<f32>,
    pending_offset: usize,
}

impl SincResampler {
    fn new(in_rate: u32, target_rate: u32, quality: SincQuality, in_channels: u16) -> Result<Self> {
        let resampler = build_sinc_resampler(in_rate, target_rate, quality)?;
        Ok(Self {
            resampler,
            pending_left: Vec::with_capacity(in_channels as usize * 2048),
            pending_right: Vec::with_capacity(in_channels as usize * 2048),
            pending_offset: 0,
        })
    }

    fn reset(&mut self, in_rate: u32, target_rate: u32, quality: SincQuality) -> Result<()> {
        self.resampler = build_sinc_resampler(in_rate, target_rate, quality)?;
        self.pending_left.clear();
        self.pending_right.clear();
        self.pending_offset = 0;
        Ok(())
    }

    fn process(&mut self, input: &[f32], in_channels: u16) -> Vec<i16> {
        self.push_stereo_frames(input, in_channels);

        let mut out = Vec::new();
        loop {
            let needed = self.resampler.input_frames_next();
            let available = self.pending_left.len().saturating_sub(self.pending_offset);
            if available < needed {
                break;
            }
            let start = self.pending_offset;
            let end = start + needed;
            let input_chunk = vec![
                self.pending_left[start..end].to_vec(),
                self.pending_right[start..end].to_vec(),
            ];
            match self.resampler.process(&input_chunk, None) {
                Ok(output) => out.extend(interleave_to_i16(&output)),
                Err(err) => {
                    warn!("resampler failed: {}", err);
                    break;
                }
            }
            self.pending_offset = end;
            if self.pending_offset >= self.pending_left.len() / 2 {
                self.pending_left.drain(0..self.pending_offset);
                self.pending_right.drain(0..self.pending_offset);
                self.pending_offset = 0;
            }
        }

        out
    }

    fn push_stereo_frames(&mut self, input: &[f32], in_channels: u16) {
        let in_channels_usize = in_channels as usize;
        let frames = input.len() / in_channels_usize;
        let mut idx = 0;
        for _ in 0..frames {
            let frame = &input[idx..idx + in_channels_usize];
            let (left, right) = map_channels(frame, in_channels);
            self.pending_left.push(left);
            self.pending_right.push(right);
            idx += in_channels_usize;
        }
    }
}

fn build_sinc_resampler(
    in_rate: u32,
    target_rate: u32,
    quality: SincQuality,
) -> Result<SincFixedIn<f32>> {
    let (sinc_len, oversampling_factor, interpolation, f_cutoff) = match quality {
        SincQuality::Fast => (128, 64, SincInterpolationType::Quadratic, 0.9),
        SincQuality::Quality => (256, 256, SincInterpolationType::Cubic, 0.95),
    };
    let params = SincInterpolationParameters {
        sinc_len,
        f_cutoff,
        interpolation,
        oversampling_factor,
        window: WindowFunction::BlackmanHarris2,
    };
    let ratio = target_rate as f64 / in_rate as f64;
    let resampler = SincFixedIn::<f32>::new(ratio, 2.0, params, 1024, TARGET_CHANNELS as usize)?;
    Ok(resampler)
}

fn interleave_to_i16(output: &[Vec<f32>]) -> Vec<i16> {
    if output.len() < TARGET_CHANNELS as usize {
        return Vec::new();
    }
    let left = &output[0];
    let right = &output[1];
    let frames = left.len().min(right.len());
    let mut out = Vec::with_capacity(frames * 2);
    for idx in 0..frames {
        out.push(f32_to_i16(left[idx]));
        out.push(f32_to_i16(right[idx]));
    }
    out
}
