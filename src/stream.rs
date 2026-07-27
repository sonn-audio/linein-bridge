use crate::models::BridgeStatusRequest;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
const TRACK_GAP_MS: u64 = 2000;

#[derive(Clone)]
pub struct StatusHandle {
    inner: Arc<Mutex<StatusState>>,
}

struct StatusState {
    state: String,
    device: String,
    ingest: String,
    last_error: Option<String>,
    rate: Option<u32>,
    channels: Option<u16>,
    format: Option<String>,
    observed_rate: Option<u32>,
    rms_db: Option<f32>,
    track_change: bool,
    bytes_sent_total: u64,
    last_chunk_ts: Option<String>,
}

impl StatusHandle {
    pub fn new(device: &str, ingest: &str) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StatusState {
                state: "IDLE".to_string(),
                device: device.to_string(),
                ingest: ingest.to_string(),
                last_error: None,
                rate: None,
                channels: None,
                format: None,
                observed_rate: None,
                rms_db: None,
                track_change: false,
                bytes_sent_total: 0,
                last_chunk_ts: None,
            })),
        }
    }

    pub fn set_state(&self, state: &str) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.state = state.to_string();
        }
    }

    pub fn set_last_error(&self, error: Option<String>) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.last_error = error;
        }
    }

    pub fn set_device(&self, device: &str) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.device = device.to_string();
        }
    }

    pub fn set_capture_info(&self, rate: u32, channels: u16, format: String) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.rate = Some(rate);
            inner.channels = Some(channels);
            inner.format = Some(format);
        }
    }

    pub fn set_observed_rate(&self, rate: u32) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.observed_rate = Some(rate);
        }
    }

    pub fn set_rms_db(&self, rms_db: Option<f32>) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.rms_db = rms_db;
        }
    }

    pub fn set_track_change(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.track_change = true;
        }
    }

    pub fn record_bytes(&self, bytes: usize) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.bytes_sent_total = inner.bytes_sent_total.saturating_add(bytes as u64);
            inner.last_chunk_ts = Some(crate::timestamp::now_rfc3339());
        }
    }

    pub fn health_snapshot(&self) -> crate::health::HealthSnapshot {
        let inner = match self.inner.lock() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };
        crate::health::HealthSnapshot {
            ts: crate::timestamp::now_rfc3339(),
            state: inner.state.clone(),
            device: inner.device.clone(),
            ingest: inner.ingest.clone(),
            last_error: inner.last_error.clone(),
            bytes_sent_total: inner.bytes_sent_total,
            last_chunk_ts: inner.last_chunk_ts.clone(),
        }
    }

    pub fn bridge_status(&self) -> BridgeStatusRequest {
        let mut inner = match self.inner.lock() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };
        let track_change = if inner.track_change {
            inner.track_change = false;
            Some(true)
        } else {
            None
        };
        BridgeStatusRequest {
            state: inner.state.clone(),
            device: if inner.device.is_empty() {
                None
            } else {
                Some(inner.device.clone())
            },
            rate: inner.rate,
            channels: inner.channels,
            format: inner.format.clone(),
            observed_rate: inner.observed_rate,
            rms_db: inner.rms_db,
            last_error: inner.last_error.clone(),
            track_change,
            capture_devices: None,
        }
    }

    pub fn set_ingest(&self, ingest: &str) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.ingest = ingest.to_string();
        }
    }
}

pub enum IngestTarget {
    Tcp {
        host: String,
        port: u16,
        header: String,
    },
    Ws {
        url: String,
    },
}

pub struct StreamParams {
    pub ingest: IngestTarget,
    pub rx: mpsc::UnboundedReceiver<Vec<u8>>,
    pub err_rx: mpsc::Receiver<String>,
    pub threshold_db: f32,
    pub hold_duration: Duration,
    pub vad_updates: Option<tokio::sync::watch::Receiver<(f32, Duration)>>,
    pub status: StatusHandle,
    pub output_rate: u32,
    /// Where commands pushed down the ingest socket are handed off. The WebSocket transport carries
    /// them as text frames alongside the binary audio, so control arrives in milliseconds instead of
    /// waiting for the next status poll. Absent for the TCP transport, which is upstream-only.
    pub commands: Option<Arc<crate::hooks::HookRunner>>,
}

pub async fn stream_audio(mut params: StreamParams) -> Result<()> {
    match &params.ingest {
        IngestTarget::Tcp { .. } => stream_audio_tcp(&mut params).await,
        IngestTarget::Ws { .. } => stream_audio_ws(&mut params).await,
    }
}

async fn stream_audio_tcp(params: &mut StreamParams) -> Result<()> {
    let mut backoff = Backoff::new();
    let (host, port, header) = match &params.ingest {
        IngestTarget::Tcp { host, port, header } => (host.clone(), *port, header.clone()),
        IngestTarget::Ws { .. } => anyhow::bail!("invalid tcp ingest"),
    };
    let addr = format!("{}:{}", host, port);
    let mut gate = VadGate::new();
    let mut threshold_db = params.threshold_db;
    let mut hold_duration = params.hold_duration;
    let mut idle_since: Option<Instant> = None;
    let mut last_rate_log = Instant::now();
    let mut bytes_since_log: u64 = 0;
    let chunk_bytes = chunk_bytes_for_rate(params.output_rate);
    let chunk_interval = chunk_interval();
    let max_pending = max_buffer_bytes_for_rate(params.output_rate);
    let mut pending = VecDeque::with_capacity(max_pending);
    let mut overrun_since = Instant::now();
    let mut overrun_bytes: u64 = 0;
    let mut underrun_since = Instant::now();
    let mut underrun_bytes: u64 = 0;
    let mut tick = tokio::time::interval(chunk_interval);
    // Skip, not Delay: Delay pushes the deadline forward on every late tick, so the writer's
    // schedule drifts permanently behind the capture side and the backlog only ever grows. Skip
    // keeps the original cadence and lets the catch-up drain above absorb the lost tick instead.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut stream: Option<TcpStream> = None;
    loop {
        if stream.is_none() {
            params.status.set_state("RECONNECTING");
            match connect_tcp(&addr, &header).await {
                Ok(connected) => {
                    stream = Some(connected);
                    params.status.set_state("STREAMING");
                    params.status.set_last_error(None);
                    backoff.reset();
                }
                Err(err) => {
                    params.status.set_last_error(Some(err.to_string()));
                    tokio::time::sleep(backoff.next_delay()).await;
                    continue;
                }
            }
        }

        tokio::select! {
            maybe_chunk = params.rx.recv() => {
                match maybe_chunk {
                    Some(chunk) => {
                        let rms_db = rms_db_from_pcm_i16_le(&chunk);
                        pending.extend(chunk);
                        if pending.len() > max_pending {
                            // Oldest-first, so the discontinuity lands as far back as possible.
                            // Accumulated rather than logged per event: the old code reported the
                            // size of one drop on a 5s throttle, which reads like a periodic 4800-byte
                            // loss and hides how much actually went missing.
                            let overflow = pending.len() - max_pending;
                            for _ in 0..overflow {
                                pending.pop_front();
                            }
                            overrun_bytes += overflow as u64;
                        }
                        params.status.set_rms_db(rms_db);
                        if let Some(rms_db) = rms_db {
                            let now = Instant::now();
                            let was_active = gate.active;
                            if rms_db >= threshold_db {
                                gate.set_active(now);
                            } else if gate.should_keep_active(now, hold_duration) {
                            } else {
                                gate.set_inactive();
                            }

                            if gate.active && !was_active {
                                if let Some(idle_start) = idle_since.take() {
                                    if now.duration_since(idle_start)
                                        >= Duration::from_millis(TRACK_GAP_MS)
                                    {
                                        params.status.set_track_change();
                                        info!("track change detected");
                                    }
                                }
                                info!("audio detected, streaming (rms_db={:.1})", rms_db);
                            } else if !gate.active && was_active {
                                idle_since = Some(now);
                                pending.clear();
                                info!("silence detected, pausing stream (rms_db={:.1})", rms_db);
                            }
                        }

                        if !gate.active {
                            params.status.set_state("IDLE");
                            continue;
                        }

                        // paced writes happen on the interval tick
                    }
                    None => {
                        return Err(anyhow::anyhow!("audio capture channel closed"));
                    }
                }
            }
            _ = tick.tick() => {
                if !gate.active {
                    continue;
                }
                if let Some(writer) = stream.as_mut() {
                    if pending.len() < chunk_bytes {
                        // Nothing whole to send yet. Waiting one more tick keeps the samples we do
                        // have intact; padding with silence would splice a gap into the audio and
                        // then *also* push the stream ahead of the source, which is the drift we are
                        // trying not to invent.
                        underrun_bytes += (chunk_bytes - pending.len()) as u64;
                    } else {
                        let send_bytes = drain_bytes_for_tick(pending.len(), chunk_bytes);
                        let mut payload = Vec::with_capacity(send_bytes);
                        for _ in 0..send_bytes {
                            if let Some(value) = pending.pop_front() {
                                payload.push(value);
                            }
                        }
                        let sent = payload.len();
                        if let Err(err) = writer.write_all(&payload).await {
                            params.status.set_last_error(Some(err.to_string()));
                            stream = None;
                        } else {
                            params.status.set_state("STREAMING");
                            params.status.record_bytes(sent);
                            bytes_since_log += sent as u64;
                        }
                    }
                    if last_rate_log.elapsed() >= Duration::from_secs(5) {
                        let secs = last_rate_log.elapsed().as_secs_f64();
                        let bytes_per_sec = (bytes_since_log as f64 / secs).round();
                        let est_rate = bytes_per_sec / 4.0;
                        info!(
                            "stream throughput: {} B/s (~{:.0} Hz)",
                            bytes_per_sec, est_rate
                        );
                        bytes_since_log = 0;
                        last_rate_log = Instant::now();
                    }
                    if overrun_since.elapsed() >= Duration::from_secs(5) && overrun_bytes > 0 {
                        warn!(
                            "audio buffer overrun: {} bytes dropped in last {:.1}s",
                            overrun_bytes,
                            overrun_since.elapsed().as_secs_f64()
                        );
                        overrun_bytes = 0;
                        overrun_since = Instant::now();
                    }
                    if underrun_since.elapsed() >= Duration::from_secs(5) && underrun_bytes > 0 {
                        warn!(
                            "audio buffer short by {} bytes in last {:.1}s (waited, did not pad)",
                            underrun_bytes,
                            underrun_since.elapsed().as_secs_f64()
                        );
                        underrun_bytes = 0;
                        underrun_since = Instant::now();
                    }
                }
            }
            maybe_err = params.err_rx.recv() => {
                let message = match maybe_err {
                    Some(message) => message,
                    None => "audio capture error channel closed".to_string(),
                };
                params.status.set_last_error(Some(message.clone()));
                return Err(anyhow::anyhow!(message));
            }
            _changed = async {
                match params.vad_updates.as_mut() {
                    Some(rx) => rx.changed().await.ok(),
                    None => None,
                }
            }, if params.vad_updates.is_some() => {
                if let Some(rx) = params.vad_updates.as_ref() {
                    let (next_threshold, next_hold) = *rx.borrow();
                    threshold_db = next_threshold;
                    hold_duration = next_hold;
                }
            }
        }
    }
}

async fn stream_audio_ws(params: &mut StreamParams) -> Result<()> {
    let mut backoff = Backoff::new();
    let url = match &params.ingest {
        IngestTarget::Ws { url } => url.clone(),
        IngestTarget::Tcp { .. } => anyhow::bail!("invalid ws ingest"),
    };
    let mut gate = VadGate::new();
    let mut threshold_db = params.threshold_db;
    let mut hold_duration = params.hold_duration;
    let mut idle_since: Option<Instant> = None;
    let mut last_rate_log = Instant::now();
    let mut bytes_since_log: u64 = 0;
    let chunk_bytes = chunk_bytes_for_rate(params.output_rate);
    let chunk_interval = chunk_interval();
    let max_pending = max_buffer_bytes_for_rate(params.output_rate);
    let mut pending = VecDeque::with_capacity(max_pending);
    let mut overrun_since = Instant::now();
    let mut overrun_bytes: u64 = 0;
    let mut underrun_since = Instant::now();
    let mut underrun_bytes: u64 = 0;
    let mut tick = tokio::time::interval(chunk_interval);
    // Skip, not Delay: Delay pushes the deadline forward on every late tick, so the writer's
    // schedule drifts permanently behind the capture side and the backlog only ever grows. Skip
    // keeps the original cadence and lets the catch-up drain above absorb the lost tick instead.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut stream = None;
    loop {
        if stream.is_none() {
            params.status.set_state("RECONNECTING");
            match connect_ws(&url).await {
                Ok(connected) => {
                    stream = Some(connected);
                    params.status.set_state("STREAMING");
                    params.status.set_last_error(None);
                    backoff.reset();
                }
                Err(err) => {
                    params.status.set_last_error(Some(err.to_string()));
                    tokio::time::sleep(backoff.next_delay()).await;
                    continue;
                }
            }
        }

        tokio::select! {
            // Commands arrive as text frames on the same socket that carries the audio, so a button
            // press reaches the hardware in a round trip rather than on the next 5s poll.
            incoming = read_ws_frame(stream.as_mut()) => {
                match incoming {
                    Some(Ok(text)) => {
                        if let Some(hooks) = params.commands.as_ref() {
                            dispatch_command_frame(hooks, &text).await;
                        }
                    }
                    Some(Err(err)) => {
                        params.status.set_last_error(Some(err.to_string()));
                        stream = None;
                    }
                    None => {}
                }
            }
            maybe_chunk = params.rx.recv() => {
                match maybe_chunk {
                    Some(chunk) => {
                        let rms_db = rms_db_from_pcm_i16_le(&chunk);
                        pending.extend(chunk);
                        if pending.len() > max_pending {
                            // Oldest-first, so the discontinuity lands as far back as possible.
                            // Accumulated rather than logged per event: the old code reported the
                            // size of one drop on a 5s throttle, which reads like a periodic 4800-byte
                            // loss and hides how much actually went missing.
                            let overflow = pending.len() - max_pending;
                            for _ in 0..overflow {
                                pending.pop_front();
                            }
                            overrun_bytes += overflow as u64;
                        }
                        params.status.set_rms_db(rms_db);
                        if let Some(rms_db) = rms_db {
                            let now = Instant::now();
                            let was_active = gate.active;
                            if rms_db >= threshold_db {
                                gate.set_active(now);
                            } else if gate.should_keep_active(now, hold_duration) {
                            } else {
                                gate.set_inactive();
                            }

                            if gate.active && !was_active {
                                if let Some(idle_start) = idle_since.take() {
                                    if now.duration_since(idle_start)
                                        >= Duration::from_millis(TRACK_GAP_MS)
                                    {
                                        params.status.set_track_change();
                                        info!("track change detected");
                                    }
                                }
                                info!("audio detected, streaming (rms_db={:.1})", rms_db);
                            } else if !gate.active && was_active {
                                idle_since = Some(now);
                                pending.clear();
                                info!("silence detected, pausing stream (rms_db={:.1})", rms_db);
                            }
                        }

                        if !gate.active {
                            params.status.set_state("IDLE");
                            continue;
                        }

                        // paced writes happen on the interval tick
                    }
                    None => {
                        return Err(anyhow::anyhow!("audio capture channel closed"));
                    }
                }
            }
            _ = tick.tick() => {
                if !gate.active {
                    continue;
                }
                if let Some(writer) = stream.as_mut() {
                    if pending.len() < chunk_bytes {
                        // Wait rather than pad: see the TCP writer for why silence is worse than
                        // arriving a tick late.
                        underrun_bytes += (chunk_bytes - pending.len()) as u64;
                    } else {
                        let send_bytes = drain_bytes_for_tick(pending.len(), chunk_bytes);
                        let mut buffer = Vec::with_capacity(send_bytes);
                        for _ in 0..send_bytes {
                            if let Some(value) = pending.pop_front() {
                                buffer.push(value);
                            }
                        }
                        let sent = buffer.len();
                        if let Err(err) = writer.send(Message::Binary(buffer)).await {
                            params.status.set_last_error(Some(err.to_string()));
                            stream = None;
                        } else {
                            params.status.set_state("STREAMING");
                            params.status.record_bytes(sent);
                            bytes_since_log += sent as u64;
                        }
                    }
                    if last_rate_log.elapsed() >= Duration::from_secs(5) {
                        let secs = last_rate_log.elapsed().as_secs_f64();
                        let bytes_per_sec = (bytes_since_log as f64 / secs).round();
                        let est_rate = bytes_per_sec / 4.0;
                        info!(
                            "stream throughput: {} B/s (~{:.0} Hz)",
                            bytes_per_sec, est_rate
                        );
                        bytes_since_log = 0;
                        last_rate_log = Instant::now();
                    }
                    if overrun_since.elapsed() >= Duration::from_secs(5) && overrun_bytes > 0 {
                        warn!(
                            "audio buffer overrun: {} bytes dropped in last {:.1}s",
                            overrun_bytes,
                            overrun_since.elapsed().as_secs_f64()
                        );
                        overrun_bytes = 0;
                        overrun_since = Instant::now();
                    }
                    if underrun_since.elapsed() >= Duration::from_secs(5) && underrun_bytes > 0 {
                        warn!(
                            "audio buffer short by {} bytes in last {:.1}s (waited, did not pad)",
                            underrun_bytes,
                            underrun_since.elapsed().as_secs_f64()
                        );
                        underrun_bytes = 0;
                        underrun_since = Instant::now();
                    }
                }
            }
            maybe_err = params.err_rx.recv() => {
                let message = match maybe_err {
                    Some(message) => message,
                    None => "audio capture error channel closed".to_string(),
                };
                params.status.set_last_error(Some(message.clone()));
                return Err(anyhow::anyhow!(message));
            }
            _changed = async {
                match params.vad_updates.as_mut() {
                    Some(rx) => rx.changed().await.ok(),
                    None => None,
                }
            }, if params.vad_updates.is_some() => {
                if let Some(rx) = params.vad_updates.as_ref() {
                    let (next_threshold, next_hold) = *rx.borrow();
                    threshold_db = next_threshold;
                    hold_duration = next_hold;
                }
            }
        }
    }
}

async fn connect_tcp(addr: &str, header: &str) -> Result<TcpStream> {
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect to {}", addr))?;
    stream.set_nodelay(true).context("set TCP nodelay")?;
    let header_line = format!("{}\n", header);
    stream
        .write_all(header_line.as_bytes())
        .await
        .context("send input id")?;
    Ok(stream)
}

/// Await one text frame from the ingest socket.
///
/// Resolves to `None` when there is no socket or the frame was not a command, which parks this arm of
/// the select without busy-looping: `tokio::select!` re-polls the others, and a disconnected socket is
/// handled by the reconnect at the top of the loop. Binary frames are not expected downstream (the
/// audio flows the other way) and ping/pong is handled inside tungstenite.
async fn read_ws_frame(stream: Option<&mut WsStream>) -> Option<Result<String>> {
    let stream = match stream {
        Some(stream) => stream,
        // No socket yet: yield so the reconnect arm can make progress instead of spinning.
        None => {
            tokio::time::sleep(Duration::from_millis(50)).await;
            return None;
        }
    };
    match stream.next().await {
        Some(Ok(Message::Text(text))) => Some(Ok(text)),
        Some(Ok(Message::Close(_))) => Some(Err(anyhow::anyhow!("ingest socket closed by server"))),
        Some(Ok(_)) => None,
        Some(Err(err)) => Some(Err(anyhow::Error::new(err))),
        None => Some(Err(anyhow::anyhow!("ingest socket ended"))),
    }
}

/// Hand a command frame to the hook.
///
/// Wire form is JSON, `{"command":"next","args":[]}`, with a bare string accepted as shorthand. The
/// vocabulary is deliberately not validated here: the server owns it, so a command this build has
/// never seen still reaches the script.
async fn dispatch_command_frame(hooks: &crate::hooks::HookRunner, text: &str) {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    match serde_json::from_str::<crate::models::SourceCommand>(trimmed) {
        Ok(parsed) => hooks.command(&parsed.command, &parsed.args).await,
        Err(_) => {
            // Not JSON: treat the whole frame as the command name.
            if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
                hooks.command(trimmed, &[]).await;
            } else {
                warn!("ignoring malformed command frame: {}", trimmed);
            }
        }
    }
}

async fn connect_ws(url: &str) -> Result<WsStream> {
    let (stream, _) = connect_async(url)
        .await
        .with_context(|| format!("connect ws {}", url))?;
    Ok(stream)
}

struct Backoff {
    current: Duration,
}

impl Backoff {
    fn new() -> Self {
        Self {
            current: Duration::from_secs(1),
        }
    }

    fn reset(&mut self) {
        self.current = Duration::from_secs(1);
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = std::cmp::min(self.current * 2, Duration::from_secs(30));
        delay
    }
}

struct VadGate {
    active: bool,
    last_active: Option<Instant>,
}

impl VadGate {
    fn new() -> Self {
        Self {
            active: false,
            last_active: None,
        }
    }

    fn set_active(&mut self, now: Instant) {
        self.active = true;
        self.last_active = Some(now);
    }

    fn set_inactive(&mut self) {
        self.active = false;
    }

    fn should_keep_active(&self, now: Instant, hold: Duration) -> bool {
        match self.last_active {
            Some(ts) => now.duration_since(ts) <= hold,
            None => false,
        }
    }
}

fn rms_db_from_pcm_i16_le(bytes: &[u8]) -> Option<f32> {
    let mut sum = 0f64;
    let mut count = 0u64;
    for chunk in bytes.chunks_exact(2) {
        let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
        let normalized = sample as f64 / i16::MAX as f64;
        sum += normalized * normalized;
        count += 1;
    }
    if count == 0 {
        return None;
    }
    let mean = sum / count as f64;
    let rms = mean.sqrt();
    let db = if rms <= 0.0 {
        -100.0
    } else {
        20.0 * rms.log10()
    };
    Some(db as f32)
}

/// How much audio goes out per write, and therefore the floor on how late it can be.
///
/// A line-in is a live source: everything buffered anywhere on this path is delay between the
/// instrument and the speaker, with no benefit. 10ms is small enough to be inaudible on its own and
/// still several hundred frames per write, so the syscall cost stays irrelevant.
const CHUNK_MS: u32 = 10;

fn chunk_bytes_for_rate(rate: u32) -> usize {
    let bytes_per_sec = rate.saturating_mul(4);
    let bytes = bytes_per_sec.saturating_mul(CHUNK_MS) / 1000;
    bytes.max(4) as usize
}

fn chunk_interval() -> Duration {
    Duration::from_millis(CHUNK_MS as u64)
}

fn max_buffer_bytes_for_rate(rate: u32) -> usize {
    let buffer_seconds = 2u32;
    rate.saturating_mul(4).saturating_mul(buffer_seconds) as usize
}

/// Bytes to send on one tick, given what is queued.
///
/// The writer wakes on a fixed interval and used to send exactly one chunk, which makes it an
/// open-loop consumer: with MissedTickBehavior::Delay a late tick moves the deadline forward for
/// good, so every scheduling hiccup and every blocking write costs airtime that is never won back.
/// The capture side meanwhile runs at the card's rate, so the shortfall accumulates until the buffer
/// saturates and audio is discarded -- which no clock correction can fix, because it is not drift.
///
/// So drain the backlog instead of a fixed slice, capped so a long stall cannot dump seconds of
/// audio into the socket at once. The receiver is a plain stream with no rate expectation, so
/// sending early is harmless; sending too little forever is not.
fn drain_bytes_for_tick(pending_len: usize, chunk_bytes: usize) -> usize {
    const MAX_CHUNKS_PER_TICK: usize = 4;
    let whole_chunks = pending_len / chunk_bytes;
    whole_chunks.clamp(1, MAX_CHUNKS_PER_TICK) * chunk_bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHUNK: usize = 1920; // 10 ms of s16le stereo at 48 kHz

    #[test]
    fn a_backlog_is_drained_faster_than_it_arrives() {
        // The bug: the writer sent exactly one chunk per tick regardless of the backlog, so it could
        // never win back time lost to a late tick or a blocking write. The capture side keeps
        // producing at the card's rate, so the deficit accumulated until the buffer overflowed --
        // which reads like clock drift but is not, and no rate correction fixes it.
        assert_eq!(drain_bytes_for_tick(CHUNK * 3, CHUNK), CHUNK * 3);
        assert_eq!(drain_bytes_for_tick(CHUNK * 2, CHUNK), CHUNK * 2);
    }

    #[test]
    fn catch_up_is_capped_so_a_long_stall_does_not_dump_the_buffer() {
        // A 2 second backlog must not land on the socket in one write.
        assert_eq!(drain_bytes_for_tick(CHUNK * 50, CHUNK), CHUNK * 4);
    }

    #[test]
    fn steady_state_sends_exactly_one_chunk() {
        assert_eq!(drain_bytes_for_tick(CHUNK, CHUNK), CHUNK);
        // A partial chunk on top of a whole one is left for the next tick: only whole chunks go out,
        // so the stream never carries a half frame.
        assert_eq!(drain_bytes_for_tick(CHUNK + 100, CHUNK), CHUNK);
    }

    #[test]
    fn never_asks_for_more_than_is_queued() {
        // Callers only reach this with at least one whole chunk buffered, but the clamp floor must
        // not invent bytes if that ever changes.
        for len in [0usize, 1, CHUNK - 1] {
            assert_eq!(drain_bytes_for_tick(len, CHUNK), CHUNK);
        }
    }

    #[test]
    fn buffer_is_two_seconds_not_one_period() {
        // Pins the sizing that ruled out "the buffer is one period too tight": 4800 dropped bytes is
        // 1.25% of this, so a shortfall that small cannot be what overflows it.
        assert_eq!(max_buffer_bytes_for_rate(48_000), 48_000 * 4 * 2);
        assert_eq!(chunk_bytes_for_rate(48_000), CHUNK);
        assert_eq!(chunk_interval(), Duration::from_millis(CHUNK_MS as u64));
    }
}
