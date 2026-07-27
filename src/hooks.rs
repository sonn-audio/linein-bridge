use std::process::Stdio;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Hands commands from the server to a script on this machine.
///
/// The bridge stays deliberately dumb: it holds no model of what the zone is doing and does not
/// decide what a command means. The server knows which source a zone is on -- it is the only thing
/// that does -- so it says "next" and the script decides how to put that on the wire for whatever
/// hardware is attached here.
///
/// One hook rather than one per event: activation and deactivation are commands too, so `start` and
/// `stop` go through the same script as `play` or `next`. Adding a command then costs a change to the
/// script only, not to the bridge and its config.
pub struct HookRunner {
    on_command: Option<String>,
    /// Last activation state we acted on. `None` until the server first reports, so a bridge that
    /// starts up while its input is deselected does not send `stop` for a source it never started.
    state: Arc<Mutex<Option<bool>>>,
}

impl HookRunner {
    pub fn new(on_command: Option<String>) -> Self {
        Self {
            on_command,
            state: Arc::new(Mutex::new(None)),
        }
    }

    pub fn is_configured(&self) -> bool {
        self.on_command.is_some()
    }

    /// Apply the server's desired activation state, emitting `start`/`stop` only on a change.
    ///
    /// Edge-triggered because the status poll repeats every few seconds and these commands drive
    /// real hardware: re-sending `start` on every poll would keep re-powering a device already on.
    pub async fn apply_active(&self, active: bool) {
        let mut guard = self.state.lock().await;
        if *guard == Some(active) {
            return;
        }
        let first = guard.is_none();
        *guard = Some(active);
        drop(guard);

        if first && !active {
            debug!("source starts out inactive; nothing to send");
            return;
        }
        self.send(if active { "start" } else { "stop" }, &[]).await;
    }

    /// Forward a transport command. Passed through as-is: the server owns the vocabulary, so a
    /// command this build has never heard of still reaches the script.
    pub async fn command(&self, command: &str, args: &[String]) {
        let trimmed = command.trim();
        if trimmed.is_empty() {
            return;
        }
        self.send(trimmed, args).await;
    }

    /// Send `stop` if the source is currently active, so shutting the service down does not leave an
    /// amplifier switched on.
    pub async fn shutdown(&self) {
        let mut guard = self.state.lock().await;
        if *guard != Some(true) {
            return;
        }
        *guard = Some(false);
        drop(guard);
        info!("shutting down with source active; sending stop");
        self.send("stop", &[]).await;
    }

    async fn send(&self, command: &str, args: &[String]) {
        let Some(script) = self.on_command.as_deref() else {
            debug!("command {} dropped; no on_command hook configured", command);
            return;
        };
        info!("running on_command hook: {} {}", command, args.join(" "));
        run(script, command, args).await;
    }
}

/// Spawn via a shell so the config can hold a command with arguments, not just a bare path.
///
/// The command and its arguments are passed as the shell's positional parameters, so a configured
/// script like `/path/ml-cmd.sh` receives them as "$@" the way any program would. The script text is
/// left exactly as configured -- appending `"$@"` to it would double the arguments for any script
/// that already refers to them, and silently change the meaning of a working hook.
///
/// Failures are logged and swallowed: a broken hook must not take the audio path down with it.
async fn run(script: &str, command: &str, args: &[String]) {
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg(format!("exec {} \"$@\"", script))
        // Placeholder for $0, which a `sh -c` script does not otherwise get.
        .arg("linein-bridge")
        .arg(command);
    for arg in args {
        cmd.arg(arg);
    }
    let spawned = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(false)
        .output()
        .await;
    match spawned {
        Ok(output) if output.status.success() => {
            debug!("on_command hook finished: {}", command);
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                "on_command hook for {} exited with {}: {}",
                command,
                output.status,
                stderr.trim()
            );
        }
        Err(err) => {
            warn!("on_command hook for {} failed to run: {}", command, err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn marker(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("linein-hook-test-{}-{}", name, std::process::id()))
    }

    /// Writes a real script to disk and returns its path, so the tests exercise the same contract a
    /// user's hook does: an executable that reads the command and arguments from "$@". A shell
    /// one-liner would not -- a nested `sh -c` swallows the first parameter as its own $0.
    fn record_cmd(marker: &Path) -> String {
        let script = marker.with_extension("sh");
        std::fs::write(
            &script,
            format!("#!/bin/sh\nprintf '%s\\n' \"$*\" >> {}\n", marker.display()),
        )
        .expect("write hook script");
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .expect("chmod hook script");
        script.display().to_string()
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap_or_default()
    }

    #[tokio::test]
    async fn activation_sends_start_and_stop_once_per_transition() {
        let path = marker("edge");
        let _ = std::fs::remove_file(&path);
        let hooks = HookRunner::new(Some(record_cmd(&path)));

        // The server repeats desired state on every poll; only a change may fire.
        hooks.apply_active(true).await;
        hooks.apply_active(true).await;
        hooks.apply_active(true).await;
        assert_eq!(read(&path), "start\n");

        hooks.apply_active(false).await;
        hooks.apply_active(false).await;
        assert_eq!(read(&path), "start\nstop\n");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn starting_up_inactive_sends_nothing() {
        let path = marker("initial");
        let _ = std::fs::remove_file(&path);
        let hooks = HookRunner::new(Some(record_cmd(&path)));
        hooks.apply_active(false).await;
        assert_eq!(
            read(&path),
            "",
            "nothing was started, so there is nothing to stop"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn transport_commands_reach_the_script_with_arguments() {
        let path = marker("cmd");
        let _ = std::fs::remove_file(&path);
        let hooks = HookRunner::new(Some(record_cmd(&path)));
        hooks.command("play", &[]).await;
        hooks.command("next", &[]).await;
        hooks
            .command("disc", &["3".to_string(), "7".to_string()])
            .await;
        // Unknown to this build: the server owns the vocabulary, so it must pass through untouched.
        hooks
            .command("sourcename", &["BeoSound 9000".to_string()])
            .await;
        assert_eq!(
            read(&path),
            "play\nnext\ndisc 3 7\nsourcename BeoSound 9000\n"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn a_blank_command_is_ignored() {
        let path = marker("blank");
        let _ = std::fs::remove_file(&path);
        let hooks = HookRunner::new(Some(record_cmd(&path)));
        hooks.command("   ", &[]).await;
        assert_eq!(read(&path), "");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn shutdown_sends_stop_only_while_active() {
        let path = marker("shutdown");
        let _ = std::fs::remove_file(&path);
        let hooks = HookRunner::new(Some(record_cmd(&path)));
        hooks.shutdown().await;
        assert_eq!(read(&path), "", "never active: shutdown must send nothing");

        hooks.apply_active(true).await;
        hooks.shutdown().await;
        assert_eq!(read(&path), "start\nstop\n");
        hooks.shutdown().await;
        assert_eq!(read(&path), "start\nstop\n", "shutdown is idempotent");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn a_failing_hook_is_contained() {
        let hooks = HookRunner::new(Some("exit 3".to_string()));
        hooks.apply_active(true).await;
        hooks.command("play", &[]).await;
        // Reaching here without panicking is the assertion: a broken hook must not take the audio
        // path down with it.
    }

    #[tokio::test]
    async fn nothing_runs_without_a_configured_hook() {
        let hooks = HookRunner::new(None);
        assert!(!hooks.is_configured());
        hooks.apply_active(true).await;
        hooks.command("play", &[]).await;
        hooks.shutdown().await;
    }
}
