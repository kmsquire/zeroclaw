//! The alias's control socket: how the person who changes the map tells the
//! running channel to re-read it (kmsquire/llm-workspace#181).
//!
//! ZeroClaw has no per-channel reload. Its own reloads rebuild every channel
//! and answer only that a reload started, so the channel listens on a unix
//! socket of its own instead. One request per connection, one line each way:
//!
//! - `reload` re-runs `people_command` and answers with what's loaded after;
//! - `status` answers with what's loaded, without re-reading.
//!
//! Answers are one JSON line of counts and a version number, never a handle.
//! Only a caller whose kernel-reported uid is `control_socket_uid` (root by
//! default) is served. The socket's directory must already exist, owned by
//! that uid and closed to everyone else; the socket itself is made 0600.

use super::source::{PeopleSource, ReloadReport};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

/// Longest request line read; the real ones are a few bytes.
const MAX_REQUEST_BYTES: u64 = 64;

/// How long a caller may take to send its request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Bind the socket. Refuses a directory anyone but `uid` could reach into,
/// and anything already at the path that isn't a stale socket.
pub(crate) fn bind(path: &Path, uid: u32) -> anyhow::Result<UnixListener> {
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .ok_or_else(|| {
            anyhow::Error::msg("control_socket must be an absolute path in a directory")
        })?;
    // symlink_metadata: the directory itself, never what a link points at.
    let meta = std::fs::symlink_metadata(dir).map_err(|e| {
        anyhow::Error::msg(format!(
            "control_socket's directory {} is unusable: {e}",
            dir.display()
        ))
    })?;
    if !meta.is_dir() || meta.uid() != uid || meta.mode() & 0o077 != 0 {
        anyhow::bail!(
            "control_socket's directory {} must be a directory owned by uid {uid} with no group \
             or other access (0700)",
            dir.display()
        );
    }
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.file_type().is_socket() => {
            // A socket someone still answers on belongs to a running channel
            // (another alias with the same path, or an older process): never
            // take it. Only a stale one is replaced.
            if std::os::unix::net::UnixStream::connect(path).is_ok() {
                anyhow::bail!(
                    "{} is already in use by another listener; refusing to take it",
                    path.display()
                );
            }
            std::fs::remove_file(path)?;
        }
        Ok(_) => anyhow::bail!(
            "{} exists and isn't a socket; refusing to replace it",
            path.display()
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let listener = UnixListener::bind(path)?;
    // set_permissions follows a symlink, so check it's still our socket. The
    // directory is closed to everyone but `uid`, so only `uid` could swap it.
    if !std::fs::symlink_metadata(path)?.file_type().is_socket() {
        anyhow::bail!(
            "{} stopped being a socket while it was set up",
            path.display()
        );
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// Connections served at once; more wait their turn.
const MAX_CONNECTIONS: usize = 4;

/// Serve requests until the task is dropped.
pub(crate) async fn serve(listener: UnixListener, uid: u32, source: PeopleSource, alias: String) {
    let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    let mut accept_failed = false;
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                if !accept_failed {
                    note(
                        &alias,
                        "BlueBubbles control socket accept failed; retrying",
                        serde_json::json!({"error": e.kind().to_string()}),
                    );
                    accept_failed = true;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        accept_failed = false;
        let Ok(slot) = slots.clone().acquire_owned().await else {
            return;
        };
        let source = source.clone();
        let alias = alias.clone();
        let handling = async move {
            handle(stream, uid, &source, &alias).await;
            drop(slot);
        };
        zeroclaw_spawn::spawn!(handling);
    }
}

fn note(alias: &str, message: &str, attrs: serde_json::Value) {
    let mut attrs = attrs;
    attrs["alias"] = serde_json::Value::String(alias.to_string());
    ::zeroclaw_log::record!(
        WARN,
        ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
            .with_outcome(::zeroclaw_log::EventOutcome::Failure)
            .with_attrs(attrs),
        message
    );
}

async fn handle(stream: UnixStream, uid: u32, source: &PeopleSource, alias: &str) {
    let caller = stream.peer_cred().map(|c| c.uid()).ok();
    let (read, mut write) = stream.into_split();
    let answer = if caller != Some(uid) {
        note(
            alias,
            "BlueBubbles control socket refused a caller",
            serde_json::json!({"uid": caller, "serves_uid": uid}),
        );
        refusal("not allowed")
    } else {
        let mut line = String::new();
        let mut reader = BufReader::new(read).take(MAX_REQUEST_BYTES);
        match tokio::time::timeout(REQUEST_TIMEOUT, reader.read_line(&mut line)).await {
            Ok(Ok(_)) => match line.trim() {
                "reload" => {
                    let report = source.reload().await;
                    log_reload(alias, "control socket", &report);
                    report
                }
                "status" => source.status(),
                _ => refusal("unknown request; send reload or status"),
            },
            _ => refusal("no request"),
        }
    };
    let mut text = serde_json::to_string(&answer).unwrap_or_else(|_| "{\"ok\":false}".into());
    text.push('\n');
    let _ = write.write_all(text.as_bytes()).await;
    let _ = write.shutdown().await;
}

fn refusal(error: &str) -> ReloadReport {
    ReloadReport {
        ok: false,
        changed: false,
        error: Some(error.to_string()),
        loaded: None,
    }
}

/// Log a reload's outcome: counts, version and the error, never a handle.
pub(crate) fn log_reload(alias: &str, trigger: &str, report: &ReloadReport) {
    let attrs = serde_json::json!({
        "alias": alias,
        "trigger": trigger,
        "changed": report.changed,
        "loaded": report.loaded,
        "error": report.error,
    });
    if report.ok {
        ::zeroclaw_log::record!(
            INFO,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                .with_attrs(attrs),
            "BlueBubbles people map reloaded"
        );
    } else {
        ::zeroclaw_log::record!(
            WARN,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                .with_attrs(attrs),
            "BlueBubbles people map reload failed; the map already loaded stays"
        );
    }
}

/// The configured socket path, if any.
pub(crate) fn socket_path(configured: Option<&str>) -> Option<PathBuf> {
    configured
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
}
