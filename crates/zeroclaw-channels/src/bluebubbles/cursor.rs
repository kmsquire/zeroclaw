//! The poll cursor: the highest BlueBubbles message row ID this alias has
//! finished with, persisted so a restart resumes where the last run stopped.
//!
//! The contract:
//! - only the poller moves it;
//! - it moves past every row it processes, including rows dropped or unreadable;
//! - for a row handed to the orchestrator, it moves only after the hand-off;
//! - on the first run it is seeded from the server's newest row, never from
//!   this host's clock;
//! - a saved value well above the server's newest row means the Messages
//!   database was rebuilt, and the cursor is reseeded rather than waiting
//!   forever.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Cursor {
    pub rowid: i64,
    /// GUIDs of the most recently handled messages, newest last. Each poll
    /// re-reads a few rows below the cursor, because a message row can be
    /// stored a moment before the row that links it to its chat; these keep
    /// that re-read from handing a message off twice.
    #[serde(default)]
    pub recent: Vec<String>,
}

/// How many recent GUIDs a cursor remembers.
pub(crate) const RECENT_CAPACITY: usize = 256;

impl Cursor {
    pub(crate) fn remember(&mut self, guid: &str) {
        if self.recent.iter().any(|g| g == guid) {
            return;
        }
        self.recent.push(guid.to_string());
        if self.recent.len() > RECENT_CAPACITY {
            let excess = self.recent.len() - RECENT_CAPACITY;
            self.recent.drain(..excess);
        }
    }

    pub(crate) fn seen(&self, guid: &str) -> bool {
        self.recent.iter().any(|g| g == guid)
    }
}

/// Where a cursor for `alias` lives inside the channel's workspace directory.
pub(crate) fn cursor_path(workspace_dir: &Path, alias: &str) -> PathBuf {
    workspace_dir
        .join("bluebubbles")
        .join(format!("{}.cursor.json", sanitize_alias(alias)))
}

/// A file name for an alias that keeps distinct aliases distinct: letters,
/// digits, `-` and `_` stay, and anything else becomes `~` and its code point.
fn sanitize_alias(alias: &str) -> String {
    alias
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c.to_string()
            } else {
                format!("~{:x}", u32::from(c))
            }
        })
        .collect()
}

/// Read a saved cursor. A missing or unreadable file reads as `None`, which
/// reseeds from the server: losing the cursor skips history, never replays it.
pub(crate) fn load(path: &Path) -> Option<Cursor> {
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

/// Save atomically: write a sibling temp file and rename it over the old one,
/// so a crash mid-write leaves the previous cursor intact.
pub(crate) fn save(path: &Path, cursor: &Cursor) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(
        &tmp,
        serde_json::to_vec(cursor).map_err(std::io::Error::other)?,
    )?;
    std::fs::rename(&tmp, path)
}

/// What to start from, given the saved cursor and the server's newest row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Start {
    /// Resume after the saved row.
    Resume(i64),
    /// No usable saved cursor: start after the server's newest row.
    Seed(i64),
    /// The saved row is beyond anything the server has: reseed and warn.
    Reseed { saved: i64, newest: i64 },
}

impl Start {
    pub(crate) fn rowid(self) -> i64 {
        match self {
            Start::Resume(r) | Start::Seed(r) => r,
            Start::Reseed { newest, .. } => newest,
        }
    }
}

pub(crate) fn decide_start(saved: Option<&Cursor>, server_newest: Option<i64>) -> Start {
    let newest = server_newest.unwrap_or(0);
    match saved {
        None => Start::Seed(newest),
        // "Newest" comes from the most recent messages by date, so a late
        // arrival can put the true maximum a little above it. Only a gap
        // larger than one query page means the database was rebuilt.
        Some(c) if c.rowid > newest + i64::from(super::api::QUERY_ROW_LIMIT) => Start::Reseed {
            saved: c.rowid,
            newest,
        },
        Some(c) => Start::Resume(c.rowid),
    }
}

impl Start {
    /// Whether this start continues an earlier run. Only then can a missed
    /// message be answered; a fresh or reseeded start reads it as context.
    pub(crate) fn is_resume(self) -> bool {
        matches!(self, Start::Resume(_))
    }
}
