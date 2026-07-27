use std::process::Stdio;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Runs the configured on_start/on_stop scripts when the server selects or deselects this input.
///
/// The server reports desired state (`source_active`) on every status poll, so this is
/// edge-triggered: only a change runs a hook. That matters because the poll repeats every few
/// seconds and the scripts drive real hardware -- re-running on_start each poll would keep
/// re-powering a device that is already on.
pub struct HookRunner {
    on_start: Option<String>,
    on_stop: Option<String>,
    /// Last state we acted on. `None` until the server first tells us, so that a bridge which
    /// starts up while the input is already deselected does not fire on_stop for a source it never
    /// switched on.
    state: Arc<Mutex<Option<bool>>>,
}

impl HookRunner {
    pub fn new(on_start: Option<String>, on_stop: Option<String>) -> Self {
        Self {
            on_start,
            on_stop,
            state: Arc::new(Mutex::new(None)),
        }
    }

    pub fn is_configured(&self) -> bool {
        self.on_start.is_some() || self.on_stop.is_some()
    }

    /// Apply the server's desired state, running a hook only on a transition.
    pub async fn apply(&self, active: bool) {
        let mut guard = self.state.lock().await;
        if *guard == Some(active) {
            return;
        }
        let first = guard.is_none();
        *guard = Some(active);
        drop(guard);

        // Nothing to undo on the very first report of an inactive source.
        if first && !active {
            debug!("source starts out inactive; no hook to run");
            return;
        }

        let script = if active {
            self.on_start.as_deref()
        } else {
            self.on_stop.as_deref()
        };
        let label = if active { "on_start" } else { "on_stop" };
        let Some(script) = script else {
            debug!("source {} but no {} hook configured", active, label);
            return;
        };
        info!("source {}; running {} hook", active, label);
        run(label, script).await;
    }

    /// Run the stop hook if the source is currently active. Used on shutdown so we do not leave
    /// an amplifier switched on after the bridge goes away.
    pub async fn shutdown(&self) {
        let mut guard = self.state.lock().await;
        if *guard != Some(true) {
            return;
        }
        *guard = Some(false);
        drop(guard);
        let Some(script) = self.on_stop.as_deref() else {
            return;
        };
        info!("shutting down with source active; running on_stop hook");
        run("on_stop", script).await;
    }
}

/// Spawn via a shell so the config can hold a command with arguments, not just a bare path.
/// Failures are logged and swallowed: a broken hook must not take the audio path down with it.
async fn run(label: &str, script: &str) {
    let spawned = Command::new("sh")
        .arg("-c")
        .arg(script)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(false)
        .output()
        .await;
    match spawned {
        Ok(output) if output.status.success() => {
            debug!("{} hook finished", label);
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                "{} hook exited with {}: {}",
                label,
                output.status,
                stderr.trim()
            );
        }
        Err(err) => {
            warn!("{} hook failed to run: {}", label, err);
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

    /// Appends a line per invocation so the test can count how often a hook ran.
    fn append_cmd(path: &Path, tag: &str) -> String {
        format!("printf '{}\\n' >> {}", tag, path.display())
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap_or_default()
    }

    #[tokio::test]
    async fn repeated_polls_run_the_hook_once_per_transition() {
        let path = marker("edge");
        let _ = std::fs::remove_file(&path);
        let hooks = HookRunner::new(
            Some(append_cmd(&path, "start")),
            Some(append_cmd(&path, "stop")),
        );

        // The server repeats desired state on every poll; only changes may run a hook.
        hooks.apply(true).await;
        hooks.apply(true).await;
        hooks.apply(true).await;
        assert_eq!(read(&path), "start\n");

        hooks.apply(false).await;
        hooks.apply(false).await;
        assert_eq!(read(&path), "start\nstop\n");

        hooks.apply(true).await;
        assert_eq!(read(&path), "start\nstop\nstart\n");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn starting_up_inactive_does_not_run_the_stop_hook() {
        let path = marker("initial");
        let _ = std::fs::remove_file(&path);
        let hooks = HookRunner::new(None, Some(append_cmd(&path, "stop")));
        hooks.apply(false).await;
        assert_eq!(
            read(&path),
            "",
            "nothing was switched on, so nothing to switch off"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn shutdown_runs_stop_only_while_active() {
        let path = marker("shutdown");
        let _ = std::fs::remove_file(&path);
        let hooks = HookRunner::new(None, Some(append_cmd(&path, "stop")));
        hooks.shutdown().await;
        assert_eq!(
            read(&path),
            "",
            "never active: shutdown must not run the stop hook"
        );

        hooks.apply(true).await;
        hooks.shutdown().await;
        assert_eq!(read(&path), "stop\n");
        hooks.shutdown().await;
        assert_eq!(read(&path), "stop\n", "shutdown is idempotent");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn a_failing_hook_is_contained() {
        let hooks = HookRunner::new(Some("exit 3".to_string()), None);
        hooks.apply(true).await;
        // Reaching here without panicking is the assertion: a broken hook must not take the
        // audio path down with it.
    }
}
