# lox-linein-bridge

`lox-linein-bridge` is a tiny Linux CLI that captures ALSA audio and streams it to a lox-audioserver line-in ingest over TCP or WebSocket. It is designed for Raspberry Pi / SBC and keeps configuration fully automatic through server-side discovery and registration.

## Download

Prebuilt binaries are available for Linux (including Raspberry Pi / SBC). Targets:
- x86_64-unknown-linux-gnu
- aarch64-unknown-linux-gnu
- armv7-unknown-linux-gnueabihf
- arm-unknown-linux-gnueabihf (Pi 1 / Zero)

Raspberry Pi mapping:
- Pi 5 / 4 (64-bit OS): aarch64-unknown-linux-gnu
- Pi 3 (64-bit OS): aarch64-unknown-linux-gnu
- Pi 3 / 2 (32-bit OS): armv7-unknown-linux-gnueabihf
- Pi 1 / Zero: arm-unknown-linux-gnueabihf

Download the latest release for your device and place the `lox-linein-bridge` binary in `/usr/local/bin/`.

## Install (systemd)

```bash
sudo lox-linein-bridge install
```

This writes the systemd unit, reloads systemd, and enables + starts the service.
The systemd unit uses a higher scheduling priority for smoother audio timing.

## Run (systemd)

```bash
lox-linein-bridge
```

This is only meant for systemd or manual troubleshooting; you do not need to run it after `install`.

## Troubleshooting

Start manually with logs:

```bash
lox-linein-bridge --log-level info
```

Log levels: `off` (default), `error`, `warn`, `info`, `debug`, `trace`.

mDNS discovery looks for `_sonncore._tcp` and uses TXT fields:
- `api` (default `/api`)
- `linein_register` (default `/api/linein/bridges/register`)
- `linein_status` (default `/api/linein/bridges/{bridge_id}/status`)

## Audio ingest protocol

The bridge streams raw PCM over TCP:
- Connect to `ingest_tcp_host:ingest_tcp_port`
- First line: `<assigned_input_id>\n`
- Then continuous raw PCM `s16le`, `48 kHz`, `2 channels` (rate and resampler can be overridden by server)

Status updates are sent separately and must not reset the audio stream.
The bridge also reports `observed_rate` in status updates (measured input rate).

## Voice activity detection (VAD)

To reduce bandwidth, the bridge uses a simple RMS-based gate. It only streams when audio is above the threshold, then holds the stream for a short time after the signal drops.

Tuning comes from the server's line-in ingest settings:
- `vad_threshold_db` (default: `-45.0` when unset)
- `vad_hold_ms` (default: `2000` when unset)
- `ingest_sample_rate` (default: `48000` when unset)
- `ingest_resampler` (default: `sinc` when unset, options: `linear`, `sinc-fast`, `sinc`)

Example `GET /api/linein/{id}/ingest` response:
```json
{
  "linein_id": "linein-mke63267",
  "ingest_tcp_host": "192.168.1.209",
  "ingest_tcp_port": 7080,
  "vad_threshold_db": -45.0,
  "vad_hold_ms": 2000,
  "ingest_sample_rate": 48000,
  "ingest_resampler": "sinc"
}
```

## Configuration

The bridge writes:
- `/etc/lox-linein-bridge/config.toml` (preferred)
- `~/.config/lox-linein-bridge/config.toml` (fallback)

Example: `examples/config.toml`

Config fields:
- `bridge_id` (auto-generated if missing)
- `preferred_server_name` (optional mDNS TXT match)
- `preferred_server_mac` (optional mDNS TXT match)
- `on_start` (optional command, run when the input is selected)
- `on_stop` (optional command, run when it is deselected)

## Switching the source on (hooks)

The VAD only streams once there is audio, so a source that has to be switched on manually never
starts by itself: nothing produces audio until it is on, and it is never turned on because nothing
asked for it. The `on_start` / `on_stop` hooks close that loop.

```toml
on_start = "/home/rudy/code/scripts/power_on.sh"
on_stop  = "/home/rudy/code/scripts/power_off.sh"
```

The server reports desired state as `source_active` on the status poll, so a hook runs on a
*change* only -- not on every poll. Selecting the input in any client (the app, a remote, a scene)
runs `on_start`; deselecting it, switching the zone to another source, or turning the zone off runs
`on_stop`. Stopping the service runs `on_stop` too, if the source was still active.

Commands go through `sh -c`, so arguments are fine. A hook that fails is logged and ignored; it
never takes the audio stream down. Because the poll interval is 5 seconds, expect up to that much
delay between selecting the input and the hook running, plus however long the device itself needs.

## Systemd unit

The wizard writes `/etc/systemd/system/lox-linein-bridge.service`.

Example: `examples/lox-linein-bridge.service`

## Build (optional)

If you want to build from source on Raspberry Pi / SBC:

```bash
sudo apt-get install -y libasound2-dev pkg-config
cargo build --release
sudo cp target/release/lox-linein-bridge /usr/local/bin/
```

Then enable the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now lox-linein-bridge
```

Check service status:

```bash
systemctl status lox-linein-bridge
```
