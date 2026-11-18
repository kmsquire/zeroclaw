//! Decide what to do with each polled BlueBubbles message.
//!
//! [`classify`] is a pure function of the message, the alias's policy and the
//! catch-up plan, so every admission rule is testable without a server.

use super::api::BbMessage;
use std::collections::{HashMap, HashSet};
use zeroclaw_api::channel::{ChannelConversationScope, ChannelMessage};

/// A message older than this when the poller first sees it was missed while
/// the channel or the Mac was down, and is handled as catch-up.
pub(crate) const CATCH_UP_AGE_MS: i64 = 5 * 60 * 1000;

/// A missed message older than this is kept as context but never answered.
/// It covers a long outage, but not old history that Messages in iCloud
/// syncs down later with new row IDs.
pub(crate) const MAX_ANSWER_AGE_MS: i64 = 48 * 60 * 60 * 1000;

pub(crate) const CHANNEL_NAME: &str = "bluebubbles";

/// The parts of an alias's configuration that decide admission.
pub(crate) struct AliasPolicy<'a> {
    pub alias: &'a str,
    /// The bot's own handles, already normalized.
    pub self_handles: &'a [String],
    pub group_chat_guids: &'a [String],
    /// Names that address the bot in a group chat.
    pub mention_names: &'a [String],
    /// The alias's resolved peer list, as configured (may hold `*` and
    /// `!handle` deny markers).
    pub peers: &'a [String],
}

/// How a message relates to a stretch of downtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatchUp {
    /// Seen within moments of arriving.
    Live,
    /// Missed during downtime, and the newest such message in its
    /// conversation with no reply from the bot after it: answered once.
    Answer,
    /// Missed during downtime and superseded: kept as context only.
    ContextOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DropReason {
    OwnMessage,
    Reaction,
    SystemEvent,
    Empty,
    NoChat,
    NoSender,
    UnknownSender,
    UnlistedGroup(String),
    /// SMS, RCS, or a service this channel doesn't know. Carries nothing:
    /// an SMS chat's GUID holds the sender's number.
    NotIMessage,
}

impl DropReason {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            DropReason::OwnMessage => "own_message",
            DropReason::Reaction => "reaction",
            DropReason::SystemEvent => "system_event",
            DropReason::Empty => "empty",
            DropReason::NoChat => "no_chat",
            DropReason::NoSender => "no_sender",
            DropReason::UnknownSender => "unknown_sender",
            DropReason::UnlistedGroup(_) => "unlisted_group",
            DropReason::NotIMessage => "not_imessage",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Disposition {
    Drop(DropReason),
    /// Record as context; never starts a turn.
    Context(ChannelMessage),
    Turn(ChannelMessage),
}

/// Normalize a handle so one person's forms compare equal: emails are
/// lowercased, and phone numbers keep only a leading `+` and their digits.
/// No country code is guessed for a number written without one.
pub(crate) fn normalize_handle(raw: &str) -> String {
    let mut trimmed = raw.trim();
    // BlueBubbles and Messages sometimes prefix a handle with its service.
    loop {
        let lower = trimmed.to_ascii_lowercase();
        let Some(prefix) = ["imessage:", "sms:", "auto:"]
            .iter()
            .find(|p| lower.starts_with(*p))
        else {
            break;
        };
        trimmed = trimmed[prefix.len()..].trim_start();
    }
    if trimmed.contains('@') {
        return trimmed.to_lowercase();
    }
    let is_phone_like = trimmed
        .chars()
        .all(|c| c.is_ascii_digit() || matches!(c, '+' | '-' | ' ' | '(' | ')' | '.'));
    if !is_phone_like || !trimmed.chars().any(|c| c.is_ascii_digit()) {
        return trimmed.to_lowercase();
    }
    let digits: String = trimmed.chars().filter(char::is_ascii_digit).collect();
    if trimmed.starts_with('+') {
        format!("+{digits}")
    } else {
        digits
    }
}

/// A one-to-one chat GUID has `-` as its middle field (`iMessage;-;+1555...`),
/// a group chat `+` (`iMessage;+;chat123...`). The service prefix varies
/// (`iMessage`, `SMS`, `any`), so only the middle field is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChatKind {
    Direct,
    Group,
}

pub(crate) fn chat_kind(chat_guid: &str) -> Option<ChatKind> {
    let mut parts = chat_guid.splitn(3, ';');
    let _service = parts.next()?;
    match parts.next()? {
        "-" => Some(ChatKind::Direct),
        "+" => Some(ChatKind::Group),
        _ => None,
    }
}

/// Whether a message came over iMessage, the only service whose sender
/// Apple vouches for. An SMS sender's number can be forged, and with Text
/// Message Forwarding on, the Mac receives SMS too. The chat GUID's first
/// field is the service; newer macOS writes `any` there, and then only the
/// sender's handle can say.
pub(crate) fn is_imessage(chat_guid: &str, handle_service: Option<&str>) -> bool {
    match chat_guid.split(';').next() {
        Some("iMessage") => handle_service.is_none_or(|s| s == "iMessage"),
        Some("any") => handle_service == Some("iMessage"),
        _ => false,
    }
}

/// The handle in a one-to-one chat GUID, used when a message carries no
/// handle of its own. Never taken from a group GUID.
fn direct_chat_handle(chat_guid: &str) -> Option<&str> {
    let mut parts = chat_guid.splitn(3, ';');
    let _service = parts.next()?;
    (parts.next()? == "-").then_some(())?;
    parts.next().filter(|h| !h.is_empty())
}

/// Greetings that address the bot when followed by its name ("hey Early",
/// "oye Early"). English and Spanish, since a family chat may mix both.
const GREETINGS: &[&str] = &["hey", "hi", "hello", "oye", "hola", "ey"];

/// Whether group text addresses the bot by name. Plain substring matching
/// is not enough when the name is also an ordinary word ("home early"), so
/// only these count: the name at the very start followed by `,` or `:`
/// (leading `¿`, `¡` and quotes are skipped), an `@name` mention, or a
/// greeting followed by the name.
pub(crate) fn addresses_by_name(text: &str, names: &[String]) -> bool {
    let lower = text.to_lowercase();
    let opening = lower.trim_start_matches(|c: char| {
        c.is_whitespace() || matches!(c, '¿' | '¡' | '"' | '\'' | '“' | '«')
    });
    names.iter().any(|name| {
        let name = name.trim().to_lowercase();
        if name.is_empty() {
            return false;
        }
        if let Some(rest) = opening.strip_prefix(&name)
            && rest.starts_with([',', ':'])
        {
            return true;
        }
        contains_word(&lower, &format!("@{name}"))
            || GREETINGS.iter().any(|g| {
                contains_word(&lower, &format!("{g} {name}"))
                    || contains_word(&lower, &format!("{g}, {name}"))
            })
    })
}

/// `needle` occurs in `haystack` and is not followed by a letter or digit,
/// and not preceded by one either.
fn contains_word(haystack: &str, needle: &str) -> bool {
    haystack.match_indices(needle).any(|(start, _)| {
        let before = haystack[..start].chars().next_back();
        let after = haystack[start + needle.len()..].chars().next();
        let is_word_char = |c: char| c.is_alphanumeric() || c == '-' || c == '_';
        !before.is_some_and(is_word_char) && !after.is_some_and(is_word_char)
    })
}

/// Whether a row is a group event (a rename, a member joining or leaving) or
/// a system or service row.
fn is_event(m: &BbMessage) -> bool {
    m.item_type.unwrap_or(0) != 0
        || m.group_action_type.unwrap_or(0) != 0
        || m.is_system_message
        || m.is_service_message
}

/// Whether a row is a message someone wrote, as opposed to a reaction, a
/// link preview, a group event or a system row.
pub(crate) fn is_conversational(m: &BbMessage) -> bool {
    // Tapbacks and stickers point at another message, and their text quotes
    // it ("Loved ..."), so they must never be read as a message someone
    // wrote. A link on its own carries a balloon ID but no such pointer, and
    // is an ordinary message.
    let is_reaction = m
        .associated_message_type
        .as_ref()
        .is_some_and(|v| !v.is_null())
        || m.associated_message_guid.is_some();
    !is_reaction && !is_event(m)
}

/// Decide, for one poll's messages, which ones count as catch-up and which
/// single message in each conversation gets an answer. Only written messages
/// count, and only the bot's messages that actually sent count as a reply.
///
/// Only a message that could start a turn (`can_start_turn`: an admitted DM,
/// or an addressed message from a peer in a listed group) is chosen, so a
/// question in a group is still answered when chatter followed it.
///
/// With `allow_answers` false (the first poll after a fresh start, when the
/// cursor was just seeded) nothing missed is answered.
pub(crate) fn plan_catch_up(
    messages: &[&BbMessage],
    now_ms: i64,
    allow_answers: bool,
    can_start_turn: &dyn Fn(&BbMessage) -> bool,
) -> HashMap<i64, CatchUp> {
    let mut plan = HashMap::new();
    // Newest catch-up message from someone else, per chat.
    let mut newest_other: HashMap<&str, i64> = HashMap::new();
    // Newest message from the bot, per chat, to see whether it already replied.
    let mut newest_own: HashMap<&str, i64> = HashMap::new();
    for m in messages {
        let Some(chat) = m.chats.first() else {
            continue;
        };
        let rowid = m.original_rowid;
        if !is_conversational(m) {
            continue;
        }
        if m.is_from_me {
            if m.error.unwrap_or(0) != 0 {
                continue;
            }
            let e = newest_own.entry(chat.guid.as_str()).or_insert(rowid);
            *e = (*e).max(rowid);
            continue;
        }
        if m.text.as_deref().is_none_or(|t| t.trim().is_empty()) {
            continue;
        }
        let age = m.date_created.map_or(0, |d| now_ms - d);
        if age > CATCH_UP_AGE_MS {
            plan.insert(rowid, CatchUp::ContextOnly);
            if allow_answers && age <= MAX_ANSWER_AGE_MS && can_start_turn(m) {
                let e = newest_other.entry(chat.guid.as_str()).or_insert(rowid);
                *e = (*e).max(rowid);
            }
        } else {
            plan.insert(rowid, CatchUp::Live);
        }
    }
    for (chat, rowid) in newest_other {
        let replied_after = newest_own.get(chat).is_some_and(|own| *own > rowid);
        if !replied_after {
            plan.insert(rowid, CatchUp::Answer);
        }
    }
    plan
}

/// Classify one message for one alias.
pub(crate) fn classify(
    m: &BbMessage,
    catch_up: CatchUp,
    replies_to_bot: bool,
    policy: &AliasPolicy<'_>,
) -> Disposition {
    let handle = m.handle.as_ref().map(|h| normalize_handle(&h.address));
    if m.is_from_me
        || handle
            .as_deref()
            .is_some_and(|h| policy.self_handles.iter().any(|s| s == h))
    {
        return Disposition::Drop(DropReason::OwnMessage);
    }
    if !is_conversational(m) {
        return Disposition::Drop(if is_event(m) {
            DropReason::SystemEvent
        } else {
            DropReason::Reaction
        });
    }
    let Some(chat) = m.chats.first() else {
        return Disposition::Drop(DropReason::NoChat);
    };
    let Some(kind) = chat_kind(&chat.guid) else {
        return Disposition::Drop(DropReason::NoChat);
    };
    let service = m.handle.as_ref().and_then(|h| h.service.as_deref());
    if !is_imessage(&chat.guid, service) {
        return Disposition::Drop(DropReason::NotIMessage);
    }
    let text = m.text.as_deref().map(str::trim).unwrap_or_default();
    if text.is_empty() {
        return Disposition::Drop(DropReason::Empty);
    }
    let sender = match (handle, kind) {
        (Some(h), _) if !h.is_empty() => h,
        (_, ChatKind::Direct) => match direct_chat_handle(&chat.guid) {
            Some(h) => normalize_handle(h),
            None => return Disposition::Drop(DropReason::NoSender),
        },
        _ => return Disposition::Drop(DropReason::NoSender),
    };
    let admitted = crate::allowlist::is_user_allowed_by(policy.peers, &sender, |peer, user| {
        normalize_handle(peer) == normalize_handle(user)
    });

    let (scope, addressed) = match kind {
        ChatKind::Direct => {
            if !admitted {
                return Disposition::Drop(DropReason::UnknownSender);
            }
            (ChannelConversationScope::Sender, true)
        }
        ChatKind::Group => {
            if !policy.group_chat_guids.iter().any(|g| g == &chat.guid) {
                return Disposition::Drop(DropReason::UnlistedGroup(chat.guid.clone()));
            }
            let named = addresses_by_name(text, policy.mention_names);
            (
                ChannelConversationScope::ReplyTarget,
                named || replies_to_bot,
            )
        }
    };

    let timestamp_secs = m
        .date_created
        .map_or(0, |ms| u64::try_from(ms / 1000).unwrap_or(0));
    let mut msg = ChannelMessage::new(
        m.guid.clone(),
        sender.clone(),
        chat.guid.clone(),
        text.to_string(),
        CHANNEL_NAME,
        timestamp_secs,
    );
    msg.platform_sender_id = Some(sender);
    msg.channel_alias = Some(policy.alias.to_string());
    msg.conversation_scope = scope;
    msg.explicitly_addressed = addressed && kind == ChatKind::Group;

    let starts_turn = admitted && addressed && catch_up != CatchUp::ContextOnly;
    if starts_turn {
        Disposition::Turn(msg)
    } else {
        msg.passive_context = true;
        Disposition::Context(msg)
    }
}

/// The GUID an inline reply points at, without the `p:N/` part prefix
/// BlueBubbles adds for multi-part messages.
pub(crate) fn reply_target_guid(m: &BbMessage) -> Option<&str> {
    let raw = m.thread_originator_guid.as_deref()?;
    let guid = raw.split_once('/').map_or(
        raw,
        |(prefix, rest)| {
            if prefix.starts_with("p:") { rest } else { raw }
        },
    );
    (!guid.is_empty() && guid != m.guid).then_some(guid)
}

/// GUIDs of messages the bot sent, remembered so a reply to one is
/// recognized without asking the server.
#[derive(Debug, Default)]
pub(crate) struct OwnGuids {
    set: HashSet<String>,
    order: std::collections::VecDeque<String>,
}

const OWN_GUIDS_CAPACITY: usize = 2048;

impl OwnGuids {
    pub(crate) fn insert(&mut self, guid: String) {
        if self.set.insert(guid.clone()) {
            self.order.push_back(guid);
            if self.order.len() > OWN_GUIDS_CAPACITY
                && let Some(old) = self.order.pop_front()
            {
                self.set.remove(&old);
            }
        }
    }

    pub(crate) fn contains(&self, guid: &str) -> bool {
        self.set.contains(guid)
    }

    pub(crate) fn snapshot(&self) -> HashSet<String> {
        self.set.clone()
    }
}
