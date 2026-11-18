//! iMessage through a BlueBubbles server.
//!
//! The channel polls the server's message database over HTTP, keyed on the
//! message row ID, and sends through its AppleScript endpoint. Nothing listens
//! on this host. Each alias admits only its own peers and group chats; see
//! [`inbound::classify`] for the rules and [`cursor`] for the poll cursor.

mod api;
mod cursor;
mod inbound;
mod overlap;

#[cfg(test)]
mod tests;

use api::{BlueBubblesApi, PolledRow};
use async_trait::async_trait;
use inbound::{AliasPolicy, CatchUp, ChatKind, Disposition, DropReason, OwnGuids};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use zeroclaw_api::channel::{Channel, ChannelMessage, ListenerHealth, SendMessage};
use zeroclaw_config::schema::BlueBubblesConfig;

pub(crate) use overlap::{AliasPeers, find_overlaps};

/// Row IDs covered by one query window. Kept below the server's row limit, so
/// a window is never cut short by the server's date ordering.
const WINDOW_ROWS: i64 = 500;

/// Windows read before a backlog is planned and handed off. A longer
/// backlog is handled in several batches; this only bounds memory.
const MAX_WINDOWS_PER_POLL: usize = 40;

/// Row IDs re-read below the cursor on every poll, for a message whose link
/// to its chat is stored a moment after the message itself.
const OVERLAP_ROWS: i64 = 50;

/// How many unlisted groups are named in the log, once each.
const MAX_REPORTED_GROUPS: usize = 64;

/// Longest single iMessage this channel sends. Longer replies are split on
/// paragraph boundaries; long messages read badly on a watch.
const MAX_MESSAGE_CHARS: usize = 2000;

const BACKOFF_START: Duration = Duration::from_secs(2);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// A poll success older than this no longer vouches for the listener.
const HEALTH_STALE_AFTER: Duration = Duration::from_secs(120);

#[derive(Debug, Default)]
struct HealthState {
    last_success: Option<Instant>,
    last_failed: bool,
}

#[derive(Clone)]
pub struct BlueBubblesChannel {
    alias: String,
    api: BlueBubblesApi,
    self_handles: Vec<String>,
    group_chat_guids: Vec<String>,
    mention_names: Vec<String>,
    poll_interval: Duration,
    /// Resolves the alias's peers from live config at message time.
    peer_resolver: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    /// Where the poll cursor is kept. `None` keeps it in memory only.
    workspace_dir: Option<PathBuf>,
    own_guids: Arc<Mutex<OwnGuids>>,
    health: Arc<Mutex<HealthState>>,
    /// Unlisted group chats already reported, so each is named once.
    reported_groups: Arc<Mutex<HashSet<String>>>,
}

#[derive(Debug)]
enum PollOutcome {
    Continue,
    ReceiverClosed,
}

/// Messages handled as catch-up during one poll.
#[derive(Debug, Default)]
struct CatchUpTally {
    read: u32,
    answered: u32,
}

impl CatchUpTally {
    fn count(&mut self, catch_up: CatchUp, answered: bool) {
        if catch_up != CatchUp::Live {
            self.read += 1;
            if answered {
                self.answered += 1;
            }
        }
    }
}

impl BlueBubblesChannel {
    pub fn new(
        alias: impl Into<String>,
        config: &BlueBubblesConfig,
        peer_resolver: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            alias: alias.into(),
            api: BlueBubblesApi::new(&config.server_url, &config.password)?,
            self_handles: config
                .self_handles
                .iter()
                .map(|h| inbound::normalize_handle(h))
                .filter(|h| !h.is_empty())
                .collect(),
            group_chat_guids: config.group_chat_guids.clone(),
            mention_names: config.mention_names.clone(),
            poll_interval: Duration::from_secs(config.poll_interval_secs.max(1)),
            peer_resolver,
            workspace_dir: None,
            own_guids: Arc::new(Mutex::new(OwnGuids::default())),
            health: Arc::new(Mutex::new(HealthState::default())),
            reported_groups: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Keep the poll cursor in this directory, so a restart resumes where the
    /// last run stopped. Pass the owning agent's workspace, which is backed up.
    #[must_use]
    pub fn with_workspace_dir(mut self, dir: PathBuf) -> Self {
        self.workspace_dir = Some(dir);
        self
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    #[cfg(test)]
    fn with_send_timeout(mut self, timeout: Duration) -> Self {
        self.api = self.api.with_send_timeout(timeout);
        self
    }

    fn cursor_path(&self) -> Option<PathBuf> {
        self.workspace_dir
            .as_deref()
            .map(|d| cursor::cursor_path(d, &self.alias))
    }

    fn record_success(&self) {
        let mut h = self.health.lock();
        h.last_success = Some(Instant::now());
        h.last_failed = false;
    }

    fn record_failure(&self) {
        self.health.lock().last_failed = true;
    }

    fn note(&self, level: Level, message: &str, attrs: serde_json::Value) {
        let attrs = merge_alias(attrs, &self.alias);
        match level {
            Level::Debug => ::zeroclaw_log::record!(
                DEBUG,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                    .with_attrs(attrs),
                message
            ),
            Level::Info => ::zeroclaw_log::record!(
                INFO,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                    .with_attrs(attrs),
                message
            ),
            Level::Warn => ::zeroclaw_log::record!(
                WARN,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                    .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                    .with_attrs(attrs),
                message
            ),
        }
    }

    /// Where to start polling: the saved cursor, or the server's newest row.
    async fn start_cursor(&self) -> anyhow::Result<(cursor::Cursor, cursor::Start)> {
        let saved = self.cursor_path().as_deref().and_then(cursor::load);
        let (newest, recent_guids) = self.api.newest().await?;
        let start = cursor::decide_start(saved.as_ref(), newest);
        match start {
            cursor::Start::Reseed { saved, newest } => self.note(
                Level::Warn,
                "BlueBubbles cursor is ahead of the server's newest message; the Messages \
                 database was probably rebuilt. Starting from the newest message.",
                serde_json::json!({"saved_rowid": saved, "newest_rowid": newest}),
            ),
            cursor::Start::Seed(rowid) => self.note(
                Level::Info,
                "BlueBubbles channel starting from the server's newest message",
                serde_json::json!({"rowid": rowid}),
            ),
            cursor::Start::Resume(rowid) => self.note(
                Level::Info,
                "BlueBubbles channel resuming from its saved cursor",
                serde_json::json!({"rowid": rowid}),
            ),
        }
        let mut c = saved.unwrap_or_else(|| cursor::Cursor {
            rowid: 0,
            recent: Vec::new(),
        });
        c.rowid = start.rowid();
        if !start.is_resume() {
            // The rows just below a fresh cursor are history, not news: mark
            // them seen, or the first poll's re-read would hand them off.
            for guid in &recent_guids {
                c.remember(guid);
            }
        }
        self.save_cursor(&c);
        Ok((c, start))
    }

    fn save_cursor(&self, c: &cursor::Cursor) {
        if let Some(path) = self.cursor_path()
            && let Err(e) = cursor::save(&path, c)
        {
            self.note(
                Level::Warn,
                "BlueBubbles cursor could not be saved; a restart may skip or repeat messages",
                serde_json::json!({"error": e.to_string()}),
            );
        }
    }

    /// Whether an inline reply points at one of the bot's own messages. The
    /// server is asked only when the answer could change the outcome: a group
    /// message from a peer, in a listed group, that does not already name the
    /// bot.
    async fn replies_to_bot(&self, m: &api::BbMessage, policy: &AliasPolicy<'_>) -> bool {
        let Some(target) = inbound::reply_target_guid(m) else {
            return false;
        };
        let is_group =
            m.chats.first().and_then(|c| inbound::chat_kind(&c.guid)) == Some(ChatKind::Group);
        let could_matter = is_group
            && matches!(
                inbound::classify(m, CatchUp::Live, true, policy),
                Disposition::Turn(_)
            )
            && !matches!(
                inbound::classify(m, CatchUp::Live, false, policy),
                Disposition::Turn(_)
            );
        if !could_matter {
            return false;
        }
        if self.own_guids.lock().contains(target) {
            return true;
        }
        match self.api.is_own_message(target).await {
            Ok(own) => {
                if own {
                    self.own_guids.lock().insert(target.to_string());
                }
                own
            }
            Err(e) => {
                self.note(
                    Level::Debug,
                    "BlueBubbles reply target lookup failed; treating as not addressed",
                    serde_json::json!({"error": e.to_string()}),
                );
                false
            }
        }
    }

    /// Fetch everything after the cursor, one row-ID window at a time and
    /// re-reading a few rows below it, then plan and hand off the whole
    /// backlog at once, so a long outage gets one answer per conversation
    /// rather than one per window.
    async fn poll_once(
        &self,
        tx: &mpsc::Sender<ChannelMessage>,
        cur: &mut cursor::Cursor,
        allow_answers: bool,
    ) -> anyhow::Result<PollOutcome> {
        let mut tally = CatchUpTally::default();
        loop {
            let mut backlog: Vec<PolledRow> = Vec::new();
            let mut guids: HashSet<String> = HashSet::new();
            let mut scan = cur.rowid;
            let mut first = true;
            let mut windows = 0;
            let mut more = false;
            loop {
                let lo = if first {
                    (scan - OVERLAP_ROWS).max(0)
                } else {
                    scan
                };
                first = false;
                let hi = scan.saturating_add(WINDOW_ROWS);
                let rows = self.fetch_window(lo, hi, cur).await?;
                let top = rows.iter().filter_map(PolledRow::rowid).max();
                for row in rows {
                    if let PolledRow::Message(m) = &row
                        && !guids.insert(m.guid.clone())
                    {
                        continue;
                    }
                    backlog.push(row);
                }
                windows += 1;
                match top {
                    Some(top) if top > scan => scan = top,
                    // Nothing new in this window. Rows above it mean the
                    // window held only gaps (deleted rows, or rows not linked
                    // to a chat), so move past it; otherwise the scan is done.
                    _ if self.api.any_after(hi).await? => scan = hi,
                    _ => break,
                }
                if windows >= MAX_WINDOWS_PER_POLL {
                    more = true;
                    break;
                }
            }
            backlog.sort_by_key(|r| r.rowid().unwrap_or(i64::MIN));
            if self
                .hand_off_window(&backlog, tx, cur, allow_answers, &mut tally)
                .await
                .is_some()
            {
                return Ok(PollOutcome::ReceiverClosed);
            }
            self.record_success();
            if scan > cur.rowid {
                cur.rowid = scan;
                self.save_cursor(cur);
            }
            if !more {
                break;
            }
        }
        if tally.read > 0 {
            self.note(
                Level::Warn,
                "BlueBubbles caught up on messages that arrived while the channel was down",
                serde_json::json!({"read": tally.read, "answered": tally.answered}),
            );
        }
        Ok(PollOutcome::Continue)
    }

    /// The rows in `(lo, hi]` not yet handled, in the server's date order;
    /// the caller sorts the whole backlog by row ID.
    async fn fetch_window(
        &self,
        lo: i64,
        hi: i64,
        cur: &cursor::Cursor,
    ) -> anyhow::Result<Vec<PolledRow>> {
        let mut rows = self.api.messages_in_window(lo, hi).await?;
        // The server is trusted to apply the window, but not relied on: one
        // that ignored it would hand back the newest rows on every query,
        // and they would be answered again each time.
        let asked = rows.len();
        rows.retain(|r| r.rowid().is_some_and(|id| id > lo && id <= hi));
        if rows.len() < asked {
            self.note(
                Level::Warn,
                "BlueBubbles returned rows outside the requested window; ignoring them",
                serde_json::json!({"outside": asked - rows.len(), "lo": lo, "hi": hi}),
            );
        }
        rows.retain(|r| match r {
            PolledRow::Message(m) => !cur.seen(&m.guid),
            PolledRow::Unreadable { rowid, .. } => rowid.is_some_and(|r| r > cur.rowid),
        });
        Ok(rows)
    }

    /// Classify and hand off one window's rows, moving the cursor past each.
    /// Returns `Some` only when the orchestrator's receiver has gone away.
    async fn hand_off_window(
        &self,
        rows: &[PolledRow],
        tx: &mpsc::Sender<ChannelMessage>,
        cur: &mut cursor::Cursor,
        allow_answers: bool,
        tally: &mut CatchUpTally,
    ) -> Option<PollOutcome> {
        let messages: Vec<&api::BbMessage> = rows
            .iter()
            .filter_map(|r| match r {
                PolledRow::Message(m) => Some(m.as_ref()),
                PolledRow::Unreadable { .. } => None,
            })
            .collect();
        for m in &messages {
            if m.is_from_me && m.error.unwrap_or(0) == 0 {
                self.own_guids.lock().insert(m.guid.clone());
            }
        }
        let peers = (self.peer_resolver)();
        let policy = AliasPolicy {
            alias: &self.alias,
            self_handles: &self.self_handles,
            group_chat_guids: &self.group_chat_guids,
            mention_names: &self.mention_names,
            peers: &peers,
        };
        // Which messages reply to the bot, and which could start a turn at
        // all; both are settled before catch-up chooses what to answer.
        let mut replying: HashSet<i64> = HashSet::new();
        let mut eligible: HashSet<i64> = HashSet::new();
        for m in &messages {
            let replies = self.replies_to_bot(m, &policy).await;
            if replies {
                replying.insert(m.original_rowid);
            }
            if matches!(
                inbound::classify(m, CatchUp::Live, replies, &policy),
                Disposition::Turn(_)
            ) {
                eligible.insert(m.original_rowid);
            }
        }
        let plan = inbound::plan_catch_up(&messages, now_ms(), allow_answers, &|m| {
            eligible.contains(&m.original_rowid)
        });

        for row in rows {
            if let PolledRow::Message(m) = row {
                let catch_up = plan
                    .get(&m.original_rowid)
                    .copied()
                    .unwrap_or(CatchUp::Live);
                let replies = replying.contains(&m.original_rowid);
                match inbound::classify(m, catch_up, replies, &policy) {
                    Disposition::Drop(reason) => self.report_drop(m.original_rowid, &reason),
                    Disposition::Context(msg) => {
                        tally.count(catch_up, false);
                        if tx.send(msg).await.is_err() {
                            return Some(PollOutcome::ReceiverClosed);
                        }
                    }
                    Disposition::Turn(msg) => {
                        tally.count(catch_up, true);
                        if tx.send(msg).await.is_err() {
                            return Some(PollOutcome::ReceiverClosed);
                        }
                    }
                }
                cur.remember(&m.guid);
            } else if let PolledRow::Unreadable { rowid, error } = row {
                self.note(
                    Level::Debug,
                    "BlueBubbles row skipped: unreadable",
                    serde_json::json!({"rowid": rowid, "error": error}),
                );
            }
            if let Some(r) = row.rowid()
                && r > cur.rowid
            {
                cur.rowid = r;
            }
            self.save_cursor(cur);
        }
        None
    }

    fn report_drop(&self, rowid: i64, reason: &DropReason) {
        if let DropReason::UnlistedGroup(guid) = reason {
            // Anyone can add the bot to a group, so the set is capped.
            let mut reported = self.reported_groups.lock();
            if reported.len() < MAX_REPORTED_GROUPS && reported.insert(guid.clone()) {
                drop(reported);
                self.note(
                    Level::Info,
                    "BlueBubbles message from a group chat this alias does not answer in; \
                     if this alias should answer there, add its GUID to group_chat_guids",
                    serde_json::json!({"chat_guid": guid}),
                );
            }
            return;
        }
        self.note(
            Level::Debug,
            "BlueBubbles message dropped",
            serde_json::json!({"rowid": rowid, "reason": reason.as_str()}),
        );
    }
}

#[derive(Clone, Copy)]
enum Level {
    Debug,
    Info,
    Warn,
}

fn merge_alias(attrs: serde_json::Value, alias: &str) -> serde_json::Value {
    match attrs {
        serde_json::Value::Object(mut map) => {
            map.insert("alias".into(), serde_json::Value::String(alias.to_string()));
            serde_json::Value::Object(map)
        }
        other => serde_json::json!({"alias": alias, "detail": other}),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// The chat GUID to send to. A reply carries the inbound chat's own GUID; a
/// bare handle, from a scheduled delivery, becomes a one-to-one iMessage chat.
fn chat_guid_for(recipient: &str) -> String {
    if recipient.contains(';') {
        recipient.to_string()
    } else {
        format!("iMessage;-;{}", recipient.trim())
    }
}

/// Split text into messages of at most `max` characters, preferring paragraph
/// breaks, then line breaks, and cutting mid-line only when a single line is
/// longer than `max`.
fn split_message(text: &str, max: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    for paragraph in text.split("\n\n") {
        for piece in split_long(paragraph, max) {
            let joined_len = current.chars().count() + 2 + piece.chars().count();
            if current.is_empty() {
                current = piece;
            } else if joined_len <= max {
                current.push_str("\n\n");
                current.push_str(&piece);
            } else {
                chunks.push(std::mem::take(&mut current));
                current = piece;
            }
        }
    }
    if !current.trim().is_empty() {
        chunks.push(current);
    }
    chunks.retain(|c| !c.trim().is_empty());
    chunks
}

fn split_long(paragraph: &str, max: usize) -> Vec<String> {
    if paragraph.chars().count() <= max {
        return vec![paragraph.to_string()];
    }
    let mut out = Vec::new();
    let mut current = String::new();
    for line in paragraph.split('\n') {
        let line_len = line.chars().count();
        if line_len > max {
            if !current.is_empty() {
                out.push(std::mem::take(&mut current));
            }
            let chars: Vec<char> = line.chars().collect();
            out.extend(chars.chunks(max).map(|c| c.iter().collect::<String>()));
            continue;
        }
        let joined = current.chars().count() + 1 + line_len;
        if current.is_empty() {
            current = line.to_string();
        } else if joined <= max {
            current.push('\n');
            current.push_str(line);
        } else {
            out.push(std::mem::take(&mut current));
            current = line.to_string();
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

impl ::zeroclaw_api::attribution::Attributable for BlueBubblesChannel {
    fn role(&self) -> ::zeroclaw_api::attribution::Role {
        ::zeroclaw_api::attribution::Role::Channel(
            ::zeroclaw_api::attribution::ChannelKind::BlueBubbles,
        )
    }
    fn alias(&self) -> &str {
        &self.alias
    }
}

#[async_trait]
impl Channel for BlueBubblesChannel {
    fn name(&self) -> &str {
        inbound::CHANNEL_NAME
    }

    fn self_handle(&self) -> Option<String> {
        self.self_handles.first().cloned()
    }

    fn self_addressed_mention(&self) -> Option<String> {
        self.mention_names.first().cloned()
    }

    fn is_direct_message(&self, msg: &ChannelMessage) -> bool {
        inbound::chat_kind(&msg.reply_target) == Some(ChatKind::Direct)
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        let chat_guid = chat_guid_for(&message.recipient);
        if !api::is_valid_chat_guid(&chat_guid) {
            anyhow::bail!("BlueBubbles refused to send: recipient is not a valid chat GUID");
        }
        let chunks = split_message(&message.content, MAX_MESSAGE_CHARS);
        let total = chunks.len();
        for (index, chunk) in chunks.into_iter().enumerate() {
            let temp_guid = format!("temp-{}", uuid::Uuid::new_v4());
            // BlueBubbles' own clock decides the match window, so allow for
            // skew between the two hosts.
            let started_ms = now_ms() - 60_000;
            // The bot's messages known before this attempt; none of them can
            // be the one this attempt sent.
            let known_before = self.own_guids.lock().snapshot();
            let result = match self.api.send_text(&chat_guid, &chunk, &temp_guid).await {
                Err(api::SendError::Timeout(e)) => {
                    // A send the server did not confirm in time may still have
                    // gone out. Check before reporting it as failed, ignoring
                    // the bot's messages that existed before this attempt.
                    match self
                        .api
                        .sent_since(&chat_guid, &chunk, started_ms, &known_before)
                        .await
                    {
                        Ok(true) => {
                            self.note(
                                Level::Info,
                                "BlueBubbles send was not confirmed, but the message is in the chat",
                                serde_json::json!({}),
                            );
                            Ok(None)
                        }
                        _ => Err(api::SendError::Other(e)),
                    }
                }
                other => other,
            };
            match result {
                Ok(Some(guid)) => self.own_guids.lock().insert(guid),
                Ok(None) => {}
                Err(e) => {
                    let e = e.into_inner();
                    if index > 0 {
                        self.note(
                            Level::Warn,
                            "BlueBubbles reply partly sent: a later part failed",
                            serde_json::json!({"sent_parts": index, "total_parts": total}),
                        );
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    async fn listen(&self, tx: mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        let mut backoff = BACKOFF_START;
        let (mut cur, start) = loop {
            match self.start_cursor().await {
                Ok(started) => {
                    self.record_success();
                    break started;
                }
                Err(e) => {
                    self.record_failure();
                    self.note(
                        Level::Warn,
                        "BlueBubbles server unreachable at startup; retrying",
                        serde_json::json!({"error": e.to_string(), "retry_secs": backoff.as_secs()}),
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
        };
        backoff = BACKOFF_START;
        // A fresh start has no record of what was already answered, so its
        // first poll reads missed messages as context only.
        let mut allow_answers = start.is_resume();
        loop {
            match self.poll_once(&tx, &mut cur, allow_answers).await {
                Ok(PollOutcome::ReceiverClosed) => return Ok(()),
                Ok(PollOutcome::Continue) => {
                    allow_answers = true;
                    self.record_success();
                    backoff = BACKOFF_START;
                    tokio::time::sleep(self.poll_interval).await;
                }
                Err(e) => {
                    self.record_failure();
                    self.note(
                        Level::Warn,
                        "BlueBubbles poll failed; retrying",
                        serde_json::json!({"error": e.to_string(), "retry_secs": backoff.as_secs()}),
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
        }
    }

    async fn health_check(&self) -> bool {
        self.api.server_info().await.is_ok()
    }

    fn listener_health(&self) -> Option<ListenerHealth> {
        let h = self.health.lock();
        let stale_after = HEALTH_STALE_AFTER.max(self.poll_interval * 4);
        Some(match h.last_success {
            None if !h.last_failed => ListenerHealth::Pending,
            None => ListenerHealth::Unhealthy,
            Some(_) if h.last_failed => ListenerHealth::Unhealthy,
            Some(at) if at.elapsed() > stale_after => ListenerHealth::Unhealthy,
            Some(_) => ListenerHealth::Healthy,
        })
    }
}
