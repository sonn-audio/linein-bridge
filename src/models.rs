use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct CaptureDeviceInfo {
    pub id: String,
    pub name: String,
    pub channels: u16,
    pub sample_rates: Vec<u32>,
}

#[derive(Debug, Serialize)]
pub struct BridgeRegisterRequest {
    pub bridge_id: String,
    pub hostname: String,
    pub version: String,
    pub ip: String,
    pub mac: String,
    pub capture_devices: Vec<CaptureDeviceInfo>,
}

#[derive(Debug, Serialize)]
pub struct BridgeStatusRequest {
    pub state: String,
    pub device: Option<String>,
    pub rate: Option<u32>,
    pub channels: Option<u16>,
    pub format: Option<String>,
    pub observed_rate: Option<u32>,
    pub rms_db: Option<f32>,
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_change: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_devices: Option<Vec<CaptureDeviceInfo>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SourceCommand {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BridgeConfigResponse {
    pub assigned_input_id: Option<String>,
    /// Whether the server currently wants this source on. Drives the start/stop commands.
    /// Absent on servers that predate the field, which reads as "not selected" and leaves the hook
    /// unused rather than firing spuriously.
    #[serde(default)]
    pub source_active: Option<bool>,
    /// Transport commands queued since our last poll, oldest first. The server owns this vocabulary
    /// and we pass each one to the hook untouched, so it can add commands without a bridge release.
    #[serde(default)]
    pub commands: Option<Vec<SourceCommand>>,
    pub ingest_ws_url: Option<String>,
    pub ingest_tcp_host: Option<String>,
    pub ingest_tcp_port: Option<u16>,
    pub capture_device: Option<String>,
    pub vad_threshold_db: Option<f32>,
    pub vad_hold_ms: Option<u64>,
    pub ingest_sample_rate: Option<u32>,
    pub ingest_resampler: Option<String>,
}
