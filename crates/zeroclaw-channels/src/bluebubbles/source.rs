//! Where the alias's people map comes from, and the rules for replacing it.
//!
//! - Until a good map has loaded, the alias admits nobody and doesn't poll.
//! - A reload that fails, or reads an invalid map or one with no handles,
//!   keeps the map already loaded. A bad or empty map never replaces a good one.
//! - Reloads run one at a time.

use super::people::{People, Summary, run_command};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long `people_command` may run.
pub(crate) const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// How often the map is re-read even without a reload request, in case one
/// was missed.
pub(crate) const BACKSTOP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// What a reload did, for the control socket and the log. Never a handle.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ReloadReport {
    pub ok: bool,
    /// Whether the loaded people, roles or handles changed.
    pub changed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// What is loaded now, after this reload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loaded: Option<Summary>,
}

#[derive(Clone)]
pub(crate) struct PeopleSource {
    command: Arc<str>,
    timeout: Duration,
    current: Arc<RwLock<Option<Arc<People>>>>,
    /// When the last reload finished, good or bad; the backstop counts from it.
    attempted_at: Arc<RwLock<Option<Instant>>>,
    reloading: Arc<tokio::sync::Mutex<()>>,
}

impl PeopleSource {
    pub(crate) fn new(command: &str) -> Self {
        Self {
            command: command.into(),
            timeout: COMMAND_TIMEOUT,
            current: Arc::new(RwLock::new(None)),
            attempted_at: Arc::new(RwLock::new(None)),
            reloading: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The loaded map, or `None` before the first good load.
    pub(crate) fn current(&self) -> Option<Arc<People>> {
        self.current.read().clone()
    }

    pub(crate) fn status(&self) -> ReloadReport {
        let current = self.current();
        ReloadReport {
            ok: current.is_some(),
            changed: false,
            error: None,
            loaded: current.map(|p| p.summary()),
        }
    }

    /// Whether a reload finished less than `window` ago, good or bad.
    pub(crate) fn attempted_within(&self, window: Duration) -> bool {
        self.attempted_at
            .read()
            .is_some_and(|at| at.elapsed() < window)
    }

    /// Run the command and, if it gives a good map with at least one handle,
    /// make it current.
    pub(crate) async fn reload(&self) -> ReloadReport {
        let _one_at_a_time = self.reloading.lock().await;
        let fetched = run_command(&self.command, self.timeout)
            .await
            .and_then(|raw| People::parse(&raw))
            .and_then(|people| {
                if people.handle_count() == 0 {
                    Err("the map has no handles; a reload never empties the family".to_string())
                } else {
                    Ok(people)
                }
            });
        *self.attempted_at.write() = Some(Instant::now());
        match fetched {
            Ok(people) => {
                let changed = self.current().is_none_or(|old| !old.same_content(&people));
                let people = Arc::new(people);
                *self.current.write() = Some(people.clone());
                ReloadReport {
                    ok: true,
                    changed,
                    error: None,
                    loaded: Some(people.summary()),
                }
            }
            Err(error) => ReloadReport {
                ok: false,
                changed: false,
                error: Some(error),
                loaded: self.current().map(|p| p.summary()),
            },
        }
    }

    /// Whether the hourly re-read is due: an hour since the last attempt.
    pub(crate) fn backstop_due(&self) -> bool {
        self.attempted_at
            .read()
            .is_none_or(|at| at.elapsed() >= BACKSTOP_INTERVAL)
    }
}
