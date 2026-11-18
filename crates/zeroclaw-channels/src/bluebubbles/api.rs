//! REST client for a BlueBubbles server.
//!
//! BlueBubbles authenticates every request with a `password` query parameter
//! and reads no header, so every request URL carries the secret. Errors from
//! this module never contain a URL, and any text that could echo one back is
//! passed through [`BlueBubblesApi::redact`] first.

use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::LazyLock;
use std::time::Duration;

/// Phone numbers and emails, as they might appear in an error body: a
/// failed query can echo a row or a chat GUID, and its excerpt is logged.
static HANDLE_LIKE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"\+?[0-9][0-9 ().-]{5,}[0-9]|[^\s@;,'\x22<>]+@[^\s@;,'\x22<>]+")
        .expect("static regex")
});

/// An error body excerpt with anything shaped like a handle replaced.
pub(crate) fn mask_handles(text: &str) -> String {
    HANDLE_LIKE.replace_all(text, "[handle]").into_owned()
}

/// How long a send may take. BlueBubbles answers an AppleScript send only
/// once the message row appears in the Messages database, and waits up to
/// two minutes for it, so a shorter limit reports slow successes as failures.
pub(crate) const SEND_TIMEOUT: Duration = Duration::from_secs(130);

/// How long a read (poll, ping) may take.
pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to wait for the TCP connection itself.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The server's maximum rows per query.
pub(crate) const QUERY_ROW_LIMIT: u32 = 1000;

/// Longest error-body excerpt kept for a log line.
const ERROR_BODY_EXCERPT_CHARS: usize = 200;

/// The most of a response body read: a broken or hostile server could
/// otherwise stream without end inside the timeout. A query of 1,000 rows
/// with their chats is well under a megabyte.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_ERROR_BODY_BYTES: usize = 4096;

const REDACTED: &str = "[REDACTED]";

/// A message as returned by `POST /api/v1/message/query`. Only the fields
/// this channel reads are declared; BlueBubbles sends many more.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BbMessage {
    pub guid: String,
    #[serde(rename = "originalROWID")]
    pub original_rowid: i64,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub is_from_me: bool,
    /// Milliseconds since the Unix epoch.
    #[serde(default)]
    pub date_created: Option<i64>,
    #[serde(default)]
    pub handle: Option<BbHandle>,
    #[serde(default)]
    pub chats: Vec<BbChat>,
    /// Set on an inline reply: the GUID of the message being replied to.
    #[serde(default)]
    pub thread_originator_guid: Option<String>,
    /// Set on tapbacks, stickers and link previews, which point at another
    /// message rather than carrying one.
    #[serde(default)]
    pub associated_message_guid: Option<String>,
    /// The kind of reaction. BlueBubbles sends a string (`"love"`, `"-love"`,
    /// `"sticker"`) or a number rendered as a string; only presence matters.
    #[serde(default)]
    pub associated_message_type: Option<Value>,
    /// Set on rich-link previews and app messages.
    #[serde(default)]
    pub balloon_bundle_id: Option<String>,
    /// Non-zero for group events (renames, members joining or leaving).
    #[serde(default)]
    pub item_type: Option<i64>,
    /// Non-zero for group membership changes.
    #[serde(default)]
    pub group_action_type: Option<i64>,
    /// Non-zero when the message failed to send.
    #[serde(default)]
    pub error: Option<i64>,
    #[serde(default)]
    pub is_system_message: bool,
    #[serde(default)]
    pub is_service_message: bool,
}

/// One row of a message query: a message this channel can read, or one it
/// cannot, kept so the cursor still moves past it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PolledRow {
    Message(Box<BbMessage>),
    Unreadable { rowid: Option<i64>, error: String },
}

impl PolledRow {
    pub(crate) fn rowid(&self) -> Option<i64> {
        match self {
            PolledRow::Message(m) => Some(m.original_rowid),
            PolledRow::Unreadable { rowid, .. } => *rowid,
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub(crate) struct BbHandle {
    pub address: String,
    /// `iMessage` or `SMS`.
    #[serde(default)]
    pub service: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub(crate) struct BbChat {
    pub guid: String,
}

/// Outcome of a send attempt, for the caller's log and health state.
#[derive(Debug)]
pub(crate) enum SendError {
    /// The request never reached the server: safe to retry.
    Connect(anyhow::Error),
    /// The server did not answer in time. The message may still have gone out.
    Timeout(anyhow::Error),
    /// The request was sent and may have been acted on: never retried.
    Other(anyhow::Error),
}

impl SendError {
    pub(crate) fn into_inner(self) -> anyhow::Error {
        match self {
            SendError::Connect(e) | SendError::Timeout(e) | SendError::Other(e) => e,
        }
    }
}

/// Whether a chat GUID has the shape BlueBubbles produces
/// (`service;-;handle` or `service;+;chat-id`). The server splices the GUID
/// into an AppleScript that it runs through a shell, so only the characters
/// a handle or chat ID can contain are allowed: letters, digits and
/// `+ @ . _ -`. Anything else, including `$`, backticks and quotes, is
/// refused before sending.
pub(crate) fn is_valid_chat_guid(guid: &str) -> bool {
    let mut parts = guid.splitn(3, ';');
    let (Some(service), Some(kind), Some(id)) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    !service.is_empty()
        && service.chars().all(|c| c.is_ascii_alphanumeric())
        && matches!(kind, "-" | "+")
        && !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '@' | '.' | '_' | '-'))
}

#[derive(Clone)]
pub(crate) struct BlueBubblesApi {
    /// Server base URL without credentials.
    base: Url,
    password: String,
    client: Client,
    send_timeout: Duration,
}

impl BlueBubblesApi {
    pub(crate) fn new(server_url: &str, password: &str) -> anyhow::Result<Self> {
        // A trailing slash keeps any path in `server_url` when endpoint paths
        // are joined onto it.
        let base = Url::parse(&format!("{}/", server_url.trim().trim_end_matches('/')))
            .map_err(|e| anyhow::Error::msg(format!("invalid BlueBubbles server_url: {e}")))?;
        // No redirects: a redirect would carry the password-bearing query
        // string to wherever it points.
        let builder = Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none());
        let builder = zeroclaw_config::schema::apply_channel_proxy_to_builder(
            builder,
            "channel.bluebubbles",
            None,
        );
        let client = builder.build().map_err(|e| {
            anyhow::Error::msg(format!(
                "BlueBubbles HTTP client failed to build: {}",
                e.without_url()
            ))
        })?;
        Ok(Self {
            base,
            password: password.to_string(),
            client,
            send_timeout: SEND_TIMEOUT,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = timeout;
        self
    }

    /// Remove the password from text bound for a log or an error, both as
    /// the literal value and in any credential shape the leak detector knows.
    pub(crate) fn redact(&self, text: &str) -> String {
        let replaced = if self.password.is_empty() {
            text.to_string()
        } else {
            text.replace(&self.password, REDACTED)
        };
        zeroclaw_runtime::security::scrub(&replaced)
    }

    fn url(&self, path: &str) -> anyhow::Result<Url> {
        let mut url = self
            .base
            .join(path)
            .map_err(|e| anyhow::Error::msg(format!("invalid BlueBubbles path {path}: {e}")))?;
        url.query_pairs_mut()
            .append_pair("password", &self.password);
        Ok(url)
    }

    /// `path` (ending in `/`) with one more path segment, percent-encoded as a
    /// path segment.
    fn url_with_segment(&self, path: &str, segment: &str) -> anyhow::Result<Url> {
        let mut url = self.url(path)?;
        url.path_segments_mut()
            .map_err(|()| anyhow::Error::msg("BlueBubbles server_url cannot take a path"))?
            .pop_if_empty()
            .push(segment);
        Ok(url)
    }

    /// Up to `cap` bytes of a body; an error past that.
    async fn body(
        &self,
        what: &str,
        mut resp: reqwest::Response,
        cap: usize,
    ) -> anyhow::Result<Vec<u8>> {
        if resp.content_length().is_some_and(|n| n > cap as u64) {
            anyhow::bail!("{what}: response larger than {cap} bytes");
        }
        let mut out = Vec::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| self.transport_error(what, e))?
        {
            if out.len() + chunk.len() > cap {
                anyhow::bail!("{what}: response larger than {cap} bytes");
            }
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    /// A JSON body of at most `MAX_BODY_BYTES`.
    async fn json_body(&self, what: &str, resp: reqwest::Response) -> anyhow::Result<Value> {
        let bytes = self.body(what, resp, MAX_BODY_BYTES).await?;
        serde_json::from_slice(&bytes)
            .map_err(|e| anyhow::Error::msg(format!("{what}: bad JSON ({:?})", e.classify())))
    }

    fn transport_error(&self, what: &str, e: reqwest::Error) -> anyhow::Error {
        let e = e.without_url();
        // reqwest's own message is generic ("error sending request"); the
        // cause (refused, timed out, DNS) is in its source chain.
        let mut detail = e.to_string();
        let mut source = std::error::Error::source(&e);
        while let Some(cause) = source {
            detail.push_str(": ");
            detail.push_str(&cause.to_string());
            source = cause.source();
        }
        anyhow::Error::msg(format!("{what}: {}", self.redact(&detail)))
    }

    /// A failed request's status and a redacted excerpt of its body. Not for
    /// sends, whose body echoes the message: see `send_error`.
    async fn status_error(&self, what: &str, resp: reqwest::Response) -> anyhow::Error {
        let status = resp.status();
        let body = match self.body(what, resp, MAX_ERROR_BODY_BYTES).await {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(_) => String::new(),
        };
        // Redact before cutting, so a cut can never leave part of the
        // password behind where the redaction no longer recognizes it.
        let excerpt: String = mask_handles(&self.redact(&body))
            .chars()
            .filter(|c| !c.is_control())
            .take(ERROR_BODY_EXCERPT_CHARS)
            .collect();
        anyhow::Error::msg(format!("{what}: HTTP {status}: {excerpt}"))
    }

    /// A failed send's error. BlueBubbles' body echoes the message and the
    /// chat, in forms (escaped, cut) a redaction can miss, so none of it is
    /// kept: only the status, the error's type, and AppleScript error numbers
    /// such as -1743, the missing Automation permission.
    async fn send_error(&self, resp: reqwest::Response) -> anyhow::Error {
        let status = resp.status();
        let body = self
            .body("BlueBubbles send failed", resp, MAX_ERROR_BODY_BYTES)
            .await
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default();
        let kind = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|v| {
                v.pointer("/error/type")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .filter(|t| {
                t.len() <= 40
                    && t.chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ' ')
            });
        let codes: Vec<&str> = body
            .split('(')
            .skip(1)
            .filter_map(|s| s.split_once(')').map(|(code, _)| code))
            .filter(|c| {
                c.len() <= 6
                    && c.starts_with('-')
                    && c[1..].chars().all(|d| d.is_ascii_digit())
                    && c.len() > 1
            })
            .collect();
        let mut detail = format!("BlueBubbles send failed: HTTP {status}");
        if let Some(kind) = kind {
            detail.push_str(&format!(": {kind}"));
        }
        if !codes.is_empty() {
            detail.push_str(&format!(" (AppleScript error {})", codes.join(", ")));
        }
        anyhow::Error::msg(detail)
    }

    /// `GET /api/v1/server/info`: the server is up and the password is right.
    pub(crate) async fn server_info(&self) -> anyhow::Result<()> {
        let resp = self
            .client
            .get(self.url("api/v1/server/info")?)
            .timeout(READ_TIMEOUT)
            .send()
            .await
            .map_err(|e| self.transport_error("BlueBubbles server info failed", e))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(self
                .status_error("BlueBubbles server info failed", resp)
                .await)
        }
    }

    /// Whether the message with this GUID was sent by this account. Used to
    /// tell an inline reply to the bot from a reply to someone else when the
    /// replied-to message is older than this process.
    pub(crate) async fn is_own_message(&self, guid: &str) -> anyhow::Result<bool> {
        let resp = self
            .client
            .get(self.url_with_segment("api/v1/message/", guid)?)
            .timeout(READ_TIMEOUT)
            .send()
            .await
            .map_err(|e| self.transport_error("BlueBubbles message lookup failed", e))?;
        if !resp.status().is_success() {
            return Err(self
                .status_error("BlueBubbles message lookup failed", resp)
                .await);
        }
        let envelope = self.json_body("BlueBubbles message lookup", resp).await?;
        Ok(envelope
            .pointer("/data/isFromMe")
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }

    /// Send text to a chat with AppleScript. A failed connection is retried
    /// once, because the request never reached the server; anything after
    /// the request was sent is not, because a send the server finishes late
    /// would go out twice. BlueBubbles 1.9.9 does not deduplicate on
    /// `tempGuid`: two sends with the same one deliver two messages.
    pub(crate) async fn send_text(
        &self,
        chat_guid: &str,
        text: &str,
        temp_guid: &str,
    ) -> Result<Option<String>, SendError> {
        match self.send_text_once(chat_guid, text, temp_guid).await {
            Err(SendError::Connect(_)) => self.send_text_once(chat_guid, text, temp_guid).await,
            other => other,
        }
    }

    async fn send_text_once(
        &self,
        chat_guid: &str,
        text: &str,
        temp_guid: &str,
    ) -> Result<Option<String>, SendError> {
        let url = self.url("api/v1/message/text").map_err(SendError::Other)?;
        let body = json!({
            "chatGuid": chat_guid,
            "message": text,
            "method": "apple-script",
            "tempGuid": temp_guid,
        });
        let resp = match self
            .client
            .post(url)
            .timeout(self.send_timeout)
            .json(&body)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) if e.is_connect() => {
                return Err(SendError::Connect(
                    self.transport_error("BlueBubbles send could not connect", e),
                ));
            }
            Err(e) if e.is_timeout() => {
                return Err(SendError::Timeout(
                    self.transport_error("BlueBubbles send timed out", e),
                ));
            }
            Err(e) => {
                return Err(SendError::Other(
                    self.transport_error("BlueBubbles send failed", e),
                ));
            }
        };
        if !resp.status().is_success() {
            return Err(SendError::Other(self.send_error(resp).await));
        }
        // The sent message's GUID, when the server reports one, lets a later
        // inline reply to it be recognized as a reply to the bot.
        let envelope = self
            .json_body("BlueBubbles send reply", resp)
            .await
            .unwrap_or(Value::Null);
        Ok(envelope
            .pointer("/data/guid")
            .and_then(Value::as_str)
            .map(str::to_string))
    }

    async fn query(&self, body: Value) -> anyhow::Result<Vec<PolledRow>> {
        let resp = self
            .client
            .post(self.url("api/v1/message/query")?)
            .timeout(READ_TIMEOUT)
            .json(&body)
            .send()
            .await
            .map_err(|e| self.transport_error("BlueBubbles message query failed", e))?;
        if resp.status() != StatusCode::OK {
            return Err(self
                .status_error("BlueBubbles message query failed", resp)
                .await);
        }
        let envelope = self.json_body("BlueBubbles message query", resp).await?;
        let data = envelope
            .get("data")
            .cloned()
            .unwrap_or(Value::Array(vec![]));
        let rows = data.as_array().ok_or_else(|| {
            anyhow::Error::msg("BlueBubbles message query returned no data array")
        })?;
        Ok(rows.iter().map(parse_row).collect())
    }

    /// Rows with `lo < ROWID <= hi`. Keyed on the row ID, which only grows as
    /// messages are stored, rather than the message date: a message that
    /// reaches the Mac late keeps its original date and would fall behind a
    /// date cursor. The server orders results by date whatever the query
    /// says, so the window is kept smaller than the row limit and can never be
    /// cut short; callers sort by row ID themselves.
    pub(crate) async fn messages_in_window(
        &self,
        lo: i64,
        hi: i64,
    ) -> anyhow::Result<Vec<PolledRow>> {
        self.query(json!({
            "where": [
                {"statement": "message.ROWID > :lo", "args": {"lo": lo}},
                {"statement": "message.ROWID <= :hi", "args": {"hi": hi}},
            ],
            "sort": "ASC",
            "limit": QUERY_ROW_LIMIT,
            "with": ["chat"],
        }))
        .await
    }

    /// Whether any row exists above `rowid`. The row IDs are checked rather
    /// than trusting the `where`, so a server that ignored it can't keep a
    /// scan climbing.
    pub(crate) async fn any_after(&self, rowid: i64) -> anyhow::Result<bool> {
        let rows = self
            .query(json!({
                "where": [{"statement": "message.ROWID > :lo", "args": {"lo": rowid}}],
                "limit": 1,
                "with": ["chat"],
            }))
            .await?;
        Ok(rows.iter().any(|r| r.rowid().is_some_and(|id| id > rowid)))
    }

    /// The highest row ID among the most recent messages (`None` on an empty
    /// database), and those messages' GUIDs in row order. Results come back
    /// in date order, so the maximum is taken over a recent page rather than
    /// read off the first row. The GUIDs let a fresh cursor treat the rows
    /// just below it as already seen.
    pub(crate) async fn newest(&self) -> anyhow::Result<(Option<i64>, Vec<String>)> {
        let mut recent: Vec<(i64, String)> = self
            .query(json!({"sort": "DESC", "limit": QUERY_ROW_LIMIT, "with": ["chat"]}))
            .await?
            .into_iter()
            .filter_map(|r| match r {
                PolledRow::Message(m) => Some((m.original_rowid, m.guid)),
                PolledRow::Unreadable { .. } => None,
            })
            .collect();
        recent.sort_by_key(|(rowid, _)| *rowid);
        let newest = recent.last().map(|(rowid, _)| *rowid);
        Ok((newest, recent.into_iter().map(|(_, guid)| guid).collect()))
    }

    /// Whether a new message from the bot with exactly this text appeared in
    /// the chat since `since_ms`. Used after a send timed out: BlueBubbles may
    /// have delivered it without confirming in time. `known` names messages
    /// that existed before the attempt, so an identical earlier reply ("OK")
    /// is never mistaken for this one.
    pub(crate) async fn sent_since(
        &self,
        chat_guid: &str,
        text: &str,
        since_ms: i64,
        known: &std::collections::HashSet<String>,
    ) -> anyhow::Result<bool> {
        let rows = self
            .query(json!({
                "chatGuid": chat_guid,
                "after": since_ms,
                "sort": "DESC",
                "limit": 25,
                "with": ["chat"],
            }))
            .await?;
        Ok(rows.iter().any(|r| match r {
            PolledRow::Message(m) => {
                m.is_from_me
                    && m.error.unwrap_or(0) == 0
                    && !known.contains(&m.guid)
                    && m.text.as_deref().map(str::trim) == Some(text.trim())
            }
            PolledRow::Unreadable { .. } => false,
        }))
    }
}

/// Parse one query row. A row that does not fit [`BbMessage`] is kept as
/// [`PolledRow::Unreadable`] with whatever row ID it carries, so one odd row
/// can never stall the cursor.
pub(crate) fn parse_row(row: &Value) -> PolledRow {
    match serde_json::from_value::<BbMessage>(row.clone()) {
        Ok(m) => PolledRow::Message(Box::new(m)),
        // Serde's message quotes the offending value, which can be message
        // text, so only its category is kept.
        Err(e) => PolledRow::Unreadable {
            rowid: row.get("originalROWID").and_then(Value::as_i64),
            error: format!("{:?}", e.classify()),
        },
    }
}
