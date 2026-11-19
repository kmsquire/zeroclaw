use super::api::{self, BbMessage, BlueBubblesApi, PolledRow};
use super::cursor::{self, Cursor, Start};
use super::inbound::{
    self, AliasPolicy, CatchUp, ChatKind, Disposition, DropReason, addresses_by_name,
    normalize_handle, plan_catch_up,
};
use super::overlap::{AliasPeers, find_overlaps};
use super::{BlueBubblesChannel, split_message};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};
use zeroclaw_api::channel::{Channel, ChannelConversationScope, SendMessage};
use zeroclaw_config::schema::BlueBubblesConfig;

/// Messages compare by the fields a test can see differ; `ChannelMessage`
/// has no `PartialEq` of its own.
impl PartialEq for Disposition {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Disposition::Drop(a), Disposition::Drop(b)) => a == b,
            (Disposition::Context(a), Disposition::Context(b))
            | (Disposition::Turn(a), Disposition::Turn(b)) => {
                a.id == b.id && a.sender == b.sender && a.content == b.content
            }
            _ => false,
        }
    }
}

const DM: &str = "iMessage;-;+15551230001";
const FAMILY_GROUP: &str = "iMessage;+;chat100";
const OTHER_GROUP: &str = "iMessage;+;chat200";
const KID: &str = "+15551230001";
const PARENT: &str = "parent@example.com";
const STRANGER: &str = "+15559990000";
const BOT: &str = "+15550000000";

/// A password made up per test run, so no literal credential sits in source.
fn test_password() -> String {
    format!("bb-{}", uuid::Uuid::new_v4().simple())
}

fn now_ms() -> i64 {
    super::now_ms()
}

/// A message row as BlueBubbles 1.9.9 serializes it, with the defaults of an
/// ordinary inbound text: from someone else, with a handle and one chat.
fn row(rowid: i64, chat: &str, sender: Option<&str>, text: &str) -> Value {
    json!({
        "guid": format!("guid-{rowid}"),
        "originalROWID": rowid,
        "text": text,
        "isFromMe": false,
        "dateCreated": now_ms(),
        "handle": sender.map(|s| json!({"address": s})),
        "chats": [{"guid": chat}],
        "threadOriginatorGuid": null,
        "associatedMessageGuid": null,
        "associatedMessageType": null,
        "balloonBundleId": null,
        "itemType": 0,
        "groupActionType": 0,
        "error": 0,
        "isSystemMessage": false,
        "isServiceMessage": false,
    })
}

fn msg(v: &Value) -> BbMessage {
    match api::parse_row(v) {
        PolledRow::Message(m) => *m,
        PolledRow::Unreadable { error, .. } => panic!("fixture did not parse: {error}"),
    }
}

fn with(mut v: Value, key: &str, value: Value) -> Value {
    v[key] = value;
    v
}

fn policy<'a>(peers: &'a [String], self_handles: &'a [String]) -> AliasPolicy<'a> {
    static GROUPS: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    static NAMES: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    AliasPolicy {
        alias: "family",
        self_handles,
        group_chat_guids: GROUPS.get_or_init(|| vec![FAMILY_GROUP.to_string()]),
        mention_names: NAMES.get_or_init(|| vec!["Early".to_string()]),
        peers,
        people: None,
    }
}

fn family_peers() -> Vec<String> {
    vec![KID.to_string(), PARENT.to_string()]
}

fn self_handles() -> Vec<String> {
    vec![BOT.to_string()]
}

fn classify(v: &Value, catch_up: CatchUp, replies_to_bot: bool) -> Disposition {
    let peers = family_peers();
    let own = self_handles();
    inbound::classify(&msg(v), catch_up, replies_to_bot, &policy(&peers, &own))
}

fn kind(d: &Disposition) -> &'static str {
    match d {
        Disposition::Drop(_) => "drop",
        Disposition::Context(_) => "context",
        Disposition::Turn(_) => "turn",
    }
}

// ── handles, chats, addressing ──────────────────────────────────

#[test]
fn handle_forms_of_one_person_normalize_to_one() {
    assert_eq!(normalize_handle("+1 (555) 123-0001"), "+15551230001");
    assert_eq!(normalize_handle("+1.555.123.0001"), "+15551230001");
    assert_eq!(normalize_handle("imessage:+15551230001"), "+15551230001");
    assert_eq!(normalize_handle("SMS:+15551230001"), "+15551230001");
    assert_eq!(
        normalize_handle(" Parent@Example.COM "),
        "parent@example.com"
    );
    assert_eq!(
        normalize_handle("auto:parent@example.com"),
        "parent@example.com"
    );
    // No country code is invented for a number written without one.
    assert_eq!(normalize_handle("555-123-0001"), "5551230001");
}

#[test]
fn chat_kind_reads_the_middle_field_whatever_the_service() {
    assert_eq!(
        inbound::chat_kind("iMessage;-;+15551230001"),
        Some(ChatKind::Direct)
    );
    assert_eq!(
        inbound::chat_kind("SMS;-;+15551230001"),
        Some(ChatKind::Direct)
    );
    assert_eq!(
        inbound::chat_kind("any;-;+15551230001"),
        Some(ChatKind::Direct)
    );
    assert_eq!(
        inbound::chat_kind("iMessage;+;chat100"),
        Some(ChatKind::Group)
    );
    assert_eq!(inbound::chat_kind("+15551230001"), None);
    assert_eq!(inbound::chat_kind(""), None);
}

#[test]
fn only_real_forms_of_address_name_the_bot() {
    let names = vec!["Early".to_string()];
    for addressed in [
        "Early, what's for dinner?",
        "early: remind me",
        "  Early, hi",
        "@Early can you help",
        "ok @early",
        "hey Early what time is it",
        "Hey early!",
        "Hey, Early, are you there?",
        "Hola, Early",
        "oye Early, ¿qué hay de cenar?",
        "¿Early, puedes recordarme algo?",
        "¡Hola Early!",
        "Early: ¿a qué hora es la práctica de Luna?",
        "EARLY, ayúdame con la tarea de Mateo",
    ] {
        assert!(
            addresses_by_name(addressed, &names),
            "{addressed:?} should address"
        );
    }
    for ordinary in [
        "I'll be home early",
        "Early dismissal tomorrow",
        "early",
        "Earlybird special",
        "they early-registered",
        "hey earlybird",
        "they early",
        "it's early, go back to sleep",
        "@early-bird specials end today",
        "Mateo llegó early hoy",
        "mañana salimos early",
        "¿llegas early?",
        "earlyé, no",
        "las clases terminan early: a las 2",
    ] {
        assert!(
            !addresses_by_name(ordinary, &names),
            "{ordinary:?} must not address"
        );
    }
    assert!(!addresses_by_name("Early, hi", &[]));
}

#[test]
fn spanish_and_accented_text_passes_through_unchanged() {
    let text = "¿Mañana a qué hora? 🎒 Ñandú, pingüino";
    let d = classify(&row(1, DM, Some(KID), text), CatchUp::Live, false);
    let Disposition::Turn(m) = d else {
        panic!("expected a turn, got {d:?}")
    };
    assert_eq!(m.content, text);
}

#[test]
fn reply_target_strips_the_part_prefix() {
    let v = with(
        row(1, FAMILY_GROUP, Some(KID), "yes"),
        "threadOriginatorGuid",
        json!("p:0/ABC"),
    );
    assert_eq!(inbound::reply_target_guid(&msg(&v)), Some("ABC"));
    let v = with(
        row(1, FAMILY_GROUP, Some(KID), "yes"),
        "threadOriginatorGuid",
        json!("ABC"),
    );
    assert_eq!(inbound::reply_target_guid(&msg(&v)), Some("ABC"));
    let v = row(1, FAMILY_GROUP, Some(KID), "yes");
    assert_eq!(inbound::reply_target_guid(&msg(&v)), None);
}

// ── classify ────────────────────────────────────────────────────

#[test]
fn a_dm_from_a_peer_starts_a_turn_with_sender_scope() {
    let d = classify(&row(1, DM, Some(KID), "hi"), CatchUp::Live, false);
    let Disposition::Turn(m) = d else {
        panic!("expected a turn, got {d:?}")
    };
    assert_eq!(m.sender, KID);
    assert_eq!(m.platform_sender_id.as_deref(), Some(KID));
    assert_eq!(m.reply_target, DM);
    assert_eq!(m.channel, "bluebubbles");
    assert_eq!(m.channel_alias.as_deref(), Some("family"));
    assert_eq!(m.conversation_scope, ChannelConversationScope::Sender);
    assert!(!m.passive_context);
    assert_eq!(m.content, "hi");
}

/// The peer list as the orchestrator resolves it for the `family` alias.
fn resolved_peers(config_toml: &str) -> Vec<String> {
    let config: zeroclaw_config::schema::Config =
        toml::from_str(config_toml).expect("config should parse");
    config.channel_external_peers("bluebubbles", "family")
}

/// The alias fails closed: with no peer group for it, or an empty one, nobody
/// can start a turn, in a DM or in a listed group. A deployment can render the
/// alias before the family's handles exist and rely on this.
#[test]
fn an_alias_with_no_or_empty_peer_group_admits_nobody() {
    let cases = [
        ("no peer group", ""),
        (
            "a group for another alias only",
            r#"
            [peer_groups.bluebubbles_other]
            channel = "bluebubbles.other"
            external_peers = ["+15551230001"]
            "#,
        ),
        (
            "an empty peer group",
            r#"
            [peer_groups.bluebubbles_family]
            channel = "bluebubbles.family"
            external_peers = []
            "#,
        ),
        (
            "a peer group with no external_peers",
            r#"
            [peer_groups.bluebubbles_family]
            channel = "bluebubbles.family"
            "#,
        ),
    ];
    for (what, toml) in cases {
        let peers = resolved_peers(toml);
        let own = self_handles();
        let policy = policy(&peers, &own);
        for (chat, text) in [(DM, "hi"), (FAMILY_GROUP, "Early, are you there?")] {
            let d = inbound::classify(
                &msg(&row(1, chat, Some(KID), text)),
                CatchUp::Live,
                false,
                &policy,
            );
            assert_ne!(kind(&d), "turn", "{what}: {chat} started a turn");
        }
        let dm = inbound::classify(
            &msg(&row(2, DM, Some(KID), "hi")),
            CatchUp::Live,
            false,
            &policy,
        );
        assert_eq!(dm, Disposition::Drop(DropReason::UnknownSender), "{what}");
    }

    // Positive control: the same path with the handle granted does admit it.
    let peers = resolved_peers(
        r#"
        [peer_groups.bluebubbles_family]
        channel = "bluebubbles.family"
        external_peers = ["+15551230001"]
        "#,
    );
    let own = self_handles();
    let d = inbound::classify(
        &msg(&row(3, DM, Some(KID), "hi")),
        CatchUp::Live,
        false,
        &policy(&peers, &own),
    );
    assert_eq!(kind(&d), "turn");
}

#[test]
fn only_imessage_is_admitted_since_an_sms_sender_can_be_forged() {
    let sms_dm = "SMS;-;+15551230001";
    let sms_group = "SMS;+;chat100";
    // A peer's number, over SMS: dropped, in a DM and in a group, however it
    // is addressed and whatever the handle says.
    for v in [
        row(1, sms_dm, Some(KID), "hi"),
        row(2, sms_dm, None, "hi"),
        row(3, sms_group, Some(KID), "Early, hi"),
        row(4, "RCS;-;+15551230001", Some(KID), "hi"),
        with(
            row(5, DM, Some(KID), "hi"),
            "handle",
            json!({"address": KID, "service": "SMS"}),
        ),
        with(
            row(6, "any;-;+15551230001", Some(KID), "hi"),
            "handle",
            json!({"address": KID, "service": "SMS"}),
        ),
        row(7, "any;-;+15551230001", Some(KID), "hi"),
    ] {
        assert_eq!(
            classify(&v, CatchUp::Live, false),
            Disposition::Drop(DropReason::NotIMessage),
            "{v}"
        );
    }
    // The same sender over iMessage, and an `any` chat whose handle says
    // iMessage, start a turn.
    for v in [
        row(8, DM, Some(KID), "hi"),
        with(
            row(9, DM, Some(KID), "hi"),
            "handle",
            json!({"address": KID, "service": "iMessage"}),
        ),
        with(
            row(10, "any;-;+15551230001", Some(KID), "hi"),
            "handle",
            json!({"address": KID, "service": "iMessage"}),
        ),
    ] {
        assert_eq!(kind(&classify(&v, CatchUp::Live, false)), "turn", "{v}");
    }
    // The reason names no number: an SMS chat's GUID holds one.
    assert_eq!(DropReason::NotIMessage.as_str(), "not_imessage");
}

#[test]
fn a_dm_from_someone_not_in_the_peers_is_dropped() {
    let d = classify(
        &row(1, "iMessage;-;+15559990000", Some(STRANGER), "hi"),
        CatchUp::Live,
        false,
    );
    assert_eq!(d, Disposition::Drop(DropReason::UnknownSender));
}

#[test]
fn a_peer_written_in_another_form_is_still_admitted() {
    let d = classify(
        &row(1, DM, Some("+1 (555) 123-0001"), "hi"),
        CatchUp::Live,
        false,
    );
    assert_eq!(kind(&d), "turn");
}

#[test]
fn rows_nobody_wrote_never_start_a_turn() {
    let base = || {
        row(
            1,
            DM,
            Some(KID),
            r#"Loved “Early, hey Early what time is it”"#,
        )
    };
    let cases = [
        (
            "tapback, string type",
            with(base(), "associatedMessageType", json!("love")),
        ),
        (
            "removed tapback",
            with(base(), "associatedMessageType", json!("-love")),
        ),
        (
            "emoji tapback",
            with(base(), "associatedMessageType", json!("2006")),
        ),
        (
            "sticker",
            with(base(), "associatedMessageType", json!("sticker")),
        ),
        (
            "points at a message",
            with(base(), "associatedMessageGuid", json!("p:0/X")),
        ),
    ];
    for (label, v) in cases {
        assert_eq!(
            classify(&v, CatchUp::Live, true),
            Disposition::Drop(DropReason::Reaction),
            "{label}"
        );
    }
    for (label, v) in [
        (
            "group event",
            with(row(1, FAMILY_GROUP, Some(KID), ""), "itemType", json!(1)),
        ),
        (
            "member change",
            with(
                row(1, FAMILY_GROUP, Some(KID), "x"),
                "groupActionType",
                json!(1),
            ),
        ),
        (
            "system",
            with(row(1, DM, Some(KID), "x"), "isSystemMessage", json!(true)),
        ),
        (
            "service",
            with(row(1, DM, Some(KID), "x"), "isServiceMessage", json!(true)),
        ),
    ] {
        assert_eq!(
            classify(&v, CatchUp::Live, false),
            Disposition::Drop(DropReason::SystemEvent),
            "{label}"
        );
    }
}

#[test]
fn a_link_on_its_own_is_an_ordinary_message() {
    let link = with(
        row(1, DM, Some(KID), "https://example.com/recipe"),
        "balloonBundleId",
        json!("com.apple.messages.URLBalloonProvider"),
    );
    assert_eq!(kind(&classify(&link, CatchUp::Live, false)), "turn");
}

#[test]
fn the_bots_own_messages_are_dropped() {
    let mine = with(row(1, DM, None, "hello"), "isFromMe", json!(true));
    assert_eq!(
        classify(&mine, CatchUp::Live, false),
        Disposition::Drop(DropReason::OwnMessage)
    );
    let from_bot_handle = row(2, DM, Some(BOT), "hello");
    assert_eq!(
        classify(&from_bot_handle, CatchUp::Live, false),
        Disposition::Drop(DropReason::OwnMessage)
    );
}

#[test]
fn empty_text_and_missing_chats_are_dropped() {
    assert_eq!(
        classify(&row(1, DM, Some(KID), "   "), CatchUp::Live, false),
        Disposition::Drop(DropReason::Empty)
    );
    let no_chat = with(row(1, DM, Some(KID), "hi"), "chats", json!([]));
    assert_eq!(
        classify(&no_chat, CatchUp::Live, false),
        Disposition::Drop(DropReason::NoChat)
    );
    let odd_chat = with(
        row(1, DM, Some(KID), "hi"),
        "chats",
        json!([{"guid": "nonsense"}]),
    );
    assert_eq!(
        classify(&odd_chat, CatchUp::Live, false),
        Disposition::Drop(DropReason::NoChat)
    );
}

#[test]
fn a_dm_without_a_handle_takes_the_sender_from_the_chat() {
    let d = classify(&row(1, DM, None, "hi"), CatchUp::Live, false);
    let Disposition::Turn(m) = d else {
        panic!("expected a turn, got {d:?}")
    };
    assert_eq!(m.sender, KID);
}

#[test]
fn a_group_message_without_a_handle_has_no_sender() {
    let d = classify(
        &row(1, FAMILY_GROUP, None, "Early, hi"),
        CatchUp::Live,
        false,
    );
    assert_eq!(d, Disposition::Drop(DropReason::NoSender));
}

#[test]
fn an_unlisted_group_is_dropped_and_named() {
    let d = classify(
        &row(1, OTHER_GROUP, Some(KID), "Early, hi"),
        CatchUp::Live,
        false,
    );
    assert_eq!(
        d,
        Disposition::Drop(DropReason::UnlistedGroup(OTHER_GROUP.to_string()))
    );
}

#[test]
fn a_listed_group_starts_a_turn_only_when_the_bot_is_addressed() {
    let addressed = classify(
        &row(1, FAMILY_GROUP, Some(KID), "Early, what's for dinner?"),
        CatchUp::Live,
        false,
    );
    let Disposition::Turn(m) = addressed else {
        panic!("expected a turn, got {addressed:?}")
    };
    assert_eq!(m.conversation_scope, ChannelConversationScope::ReplyTarget);
    assert!(m.explicitly_addressed);
    assert_eq!(m.reply_target, FAMILY_GROUP);

    let chatter = classify(
        &row(2, FAMILY_GROUP, Some(KID), "I'll be home early"),
        CatchUp::Live,
        false,
    );
    let Disposition::Context(m) = chatter else {
        panic!("expected context, got {chatter:?}")
    };
    assert!(m.passive_context);
    assert!(!m.explicitly_addressed);
    assert_eq!(m.conversation_scope, ChannelConversationScope::ReplyTarget);

    let reply = classify(
        &row(3, FAMILY_GROUP, Some(PARENT), "sounds good"),
        CatchUp::Live,
        true,
    );
    assert_eq!(kind(&reply), "turn");
}

#[test]
fn a_group_member_who_is_not_a_peer_is_context_only() {
    let d = classify(
        &row(1, FAMILY_GROUP, Some(STRANGER), "Early, hi"),
        CatchUp::Live,
        false,
    );
    assert_eq!(kind(&d), "context");
}

#[test]
fn catch_up_marks_superseded_messages_as_context() {
    let v = row(1, DM, Some(KID), "hi");
    assert_eq!(kind(&classify(&v, CatchUp::ContextOnly, false)), "context");
    assert_eq!(kind(&classify(&v, CatchUp::Answer, false)), "turn");
}

#[test]
fn a_deny_marker_overrides_a_wildcard() {
    let peers = vec!["*".to_string(), format!("!{KID}")];
    let own = self_handles();
    let d = inbound::classify(
        &msg(&row(1, DM, Some(KID), "hi")),
        CatchUp::Live,
        false,
        &policy(&peers, &own),
    );
    assert_eq!(d, Disposition::Drop(DropReason::UnknownSender));
}

#[test]
fn the_message_timestamp_is_the_mac_send_time_in_seconds() {
    let v = with(
        row(1, DM, Some(KID), "hi"),
        "dateCreated",
        json!(1_700_000_123_456_i64),
    );
    let Disposition::Turn(m) = classify(&v, CatchUp::Live, false) else {
        panic!()
    };
    assert_eq!(m.timestamp, 1_700_000_123);
}

// ── catch-up planning ───────────────────────────────────────────

fn aged(rowid: i64, chat: &str, sender: Option<&str>, text: &str, age_ms: i64) -> BbMessage {
    msg(&with(
        row(rowid, chat, sender, text),
        "dateCreated",
        json!(now_ms() - age_ms),
    ))
}

const HOUR: i64 = 60 * 60 * 1000;

#[test]
fn after_an_outage_only_the_newest_message_per_chat_is_answered() {
    let m1 = aged(1, DM, Some(KID), "are you there", 3 * HOUR);
    let m2 = aged(2, DM, Some(KID), "what's for dinner", 2 * HOUR);
    let g1 = aged(3, FAMILY_GROUP, Some(PARENT), "Early, hi", 2 * HOUR);
    let live = aged(4, "iMessage;-;parent@example.com", Some(PARENT), "now", 0);
    let plan = plan_catch_up(&[&m1, &m2, &g1, &live], now_ms(), true, &|_| true);
    assert_eq!(plan[&1], CatchUp::ContextOnly);
    assert_eq!(plan[&2], CatchUp::Answer);
    assert_eq!(plan[&3], CatchUp::Answer);
    assert_eq!(plan[&4], CatchUp::Live);
}

#[test]
fn a_chat_the_bot_already_answered_gets_no_second_answer() {
    let q = aged(1, DM, Some(KID), "question", 2 * HOUR);
    let mut a = aged(2, DM, None, "answer", HOUR);
    a.is_from_me = true;
    let plan = plan_catch_up(&[&q, &a], now_ms(), true, &|_| true);
    assert_eq!(plan[&1], CatchUp::ContextOnly);
}

#[test]
fn a_failed_reply_from_the_bot_does_not_count_as_answering() {
    let q = aged(1, DM, Some(KID), "question", 2 * HOUR);
    let mut a = aged(2, DM, None, "answer", HOUR);
    a.is_from_me = true;
    a.error = Some(22);
    let plan = plan_catch_up(&[&q, &a], now_ms(), true, &|_| true);
    assert_eq!(plan[&1], CatchUp::Answer);
}

#[test]
fn a_tapback_is_never_the_message_answered() {
    let q = aged(1, DM, Some(KID), "question", 2 * HOUR);
    let mut tap = aged(2, DM, Some(KID), "Loved “question”", HOUR);
    tap.associated_message_type = Some(json!("love"));
    let plan = plan_catch_up(&[&q, &tap], now_ms(), true, &|_| true);
    assert_eq!(plan[&1], CatchUp::Answer);
    assert!(!plan.contains_key(&2));
}

#[test]
fn group_catch_up_answers_the_question_not_the_chatter_after_it() {
    let question = aged(
        1,
        FAMILY_GROUP,
        Some(KID),
        "Early, what's for dinner?",
        3 * HOUR,
    );
    let chatter = aged(
        2,
        FAMILY_GROUP,
        Some(PARENT),
        "I'll be home early",
        2 * HOUR,
    );
    let stranger = aged(3, FAMILY_GROUP, Some(STRANGER), "Early, hi", HOUR);
    let peers = family_peers();
    let own = self_handles();
    let p = policy(&peers, &own);
    let eligible = |m: &BbMessage| {
        matches!(
            inbound::classify(m, CatchUp::Live, false, &p),
            Disposition::Turn(_)
        )
    };
    let plan = plan_catch_up(&[&question, &chatter, &stranger], now_ms(), true, &eligible);
    assert_eq!(plan[&1], CatchUp::Answer);
    assert_eq!(plan[&2], CatchUp::ContextOnly);
    assert_eq!(plan[&3], CatchUp::ContextOnly);
}

#[test]
fn a_fresh_start_answers_nothing_missed() {
    let q = aged(1, DM, Some(KID), "question", 2 * HOUR);
    let plan = plan_catch_up(&[&q], now_ms(), false, &|_| true);
    assert_eq!(plan[&1], CatchUp::ContextOnly);
}

#[test]
fn a_message_older_than_two_days_is_context_even_when_newest() {
    let q = aged(1, DM, Some(KID), "old history", 49 * HOUR);
    let recent = aged(2, FAMILY_GROUP, Some(KID), "Early, hi", 47 * HOUR);
    let plan = plan_catch_up(&[&q, &recent], now_ms(), true, &|_| true);
    assert_eq!(plan[&1], CatchUp::ContextOnly);
    assert_eq!(plan[&2], CatchUp::Answer);
}

// ── alias overlap ───────────────────────────────────────────────

fn alias<'a>(
    name: &'a str,
    server: &'a str,
    peers: &[&str],
    groups: &'a [String],
) -> AliasPeers<'a> {
    AliasPeers {
        alias: name,
        server_url: server,
        peers: peers.iter().map(|p| (*p).to_string()).collect(),
        group_chat_guids: groups,
    }
}

#[test]
fn disjoint_aliases_do_not_overlap() {
    let none: Vec<String> = vec![];
    let found = find_overlaps(&[
        alias("family", "http://mac:1234", &[KID, PARENT], &none),
        alias("owner", "http://mac:1234/", &["+15557770000"], &none),
    ]);
    assert!(found.is_empty(), "{found:?}");
}

#[test]
fn one_handle_in_two_aliases_on_one_server_overlaps_in_any_form() {
    let none: Vec<String> = vec![];
    let found = find_overlaps(&[
        alias("family", "http://mac:1234", &[KID, PARENT], &none),
        alias("owner", "HTTP://MAC:1234", &["+1 (555) 123-0001"], &none),
    ]);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].what, KID);
}

#[test]
fn a_wildcard_overlaps_any_admitted_handle_but_respects_denies() {
    let none: Vec<String> = vec![];
    let overlap = find_overlaps(&[
        alias("family", "http://mac:1234", &["*"], &none),
        alias("owner", "http://mac:1234", &[KID], &none),
    ]);
    assert_eq!(overlap.len(), 1);
    let denied = find_overlaps(&[
        alias(
            "family",
            "http://mac:1234",
            &["*", &format!("!{KID}")],
            &none,
        ),
        alias("owner", "http://mac:1234", &[KID], &none),
    ]);
    assert!(denied.is_empty(), "{denied:?}");
    let both_wild = find_overlaps(&[
        alias("family", "http://mac:1234", &["*"], &none),
        alias("owner", "http://mac:1234", &["*"], &none),
    ]);
    assert_eq!(both_wild.len(), 1);
    assert_eq!(both_wild[0].what, "*");
}

#[test]
fn a_shared_group_chat_overlaps() {
    let a = vec![FAMILY_GROUP.to_string()];
    let b = vec![FAMILY_GROUP.to_string(), OTHER_GROUP.to_string()];
    let found = find_overlaps(&[
        alias("family", "http://mac:1234", &[KID], &a),
        alias("owner", "http://mac:1234", &["+15557770000"], &b),
    ]);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].what, FAMILY_GROUP);
}

#[test]
fn aliases_on_different_servers_never_overlap() {
    let groups = vec![FAMILY_GROUP.to_string()];
    let found = find_overlaps(&[
        alias("a", "http://mac-one:1234", &[KID], &groups),
        alias("b", "http://mac-two:1234", &[KID], &groups),
    ]);
    assert!(found.is_empty());
}

// ── cursor ──────────────────────────────────────────────────────

fn saved(rowid: i64) -> Cursor {
    Cursor {
        rowid,
        recent: vec![],
    }
}

#[test]
fn the_cursor_seeds_from_the_server_and_resumes_from_disk() {
    assert_eq!(cursor::decide_start(None, Some(900)), Start::Seed(900));
    assert_eq!(cursor::decide_start(None, None), Start::Seed(0));
    assert_eq!(
        cursor::decide_start(Some(&saved(850)), Some(900)),
        Start::Resume(850)
    );
    // A little above the server's newest by date is a late arrival, not a rebuild.
    assert_eq!(
        cursor::decide_start(Some(&saved(1_500)), Some(900)),
        Start::Resume(1_500)
    );
    assert_eq!(
        cursor::decide_start(Some(&saved(50_000)), Some(900)),
        Start::Reseed {
            saved: 50_000,
            newest: 900
        }
    );
    assert!(Start::Resume(1).is_resume());
    assert!(!Start::Seed(1).is_resume());
    assert!(
        !Start::Reseed {
            saved: 9,
            newest: 1
        }
        .is_resume()
    );
}

#[test]
fn the_cursor_round_trips_and_a_bad_file_reads_as_none() {
    let dir = tempfile::tempdir().unwrap();
    let path = cursor::cursor_path(dir.path(), "fam/ily");
    assert!(path.ends_with("bluebubbles/fam~2fily.cursor.json"));
    assert_ne!(
        cursor::cursor_path(dir.path(), "a.b"),
        cursor::cursor_path(dir.path(), "a_b"),
        "distinct aliases must not share a cursor file"
    );
    assert_eq!(cursor::load(&path), None);
    let mut c = saved(42);
    c.remember("g1");
    cursor::save(&path, &c).unwrap();
    assert_eq!(cursor::load(&path), Some(c));
    std::fs::write(&path, "not json").unwrap();
    assert_eq!(cursor::load(&path), None);
}

#[test]
fn the_cursor_remembers_a_bounded_number_of_guids() {
    let mut c = saved(0);
    for i in 0..(cursor::RECENT_CAPACITY + 10) {
        c.remember(&format!("g{i}"));
    }
    assert_eq!(c.recent.len(), cursor::RECENT_CAPACITY);
    assert!(!c.seen("g0"));
    assert!(c.seen(&format!("g{}", cursor::RECENT_CAPACITY + 9)));
}

// ── sending helpers ─────────────────────────────────────────────

#[test]
fn long_replies_split_on_paragraphs_within_the_limit() {
    assert_eq!(split_message("short", 10), vec!["short"]);
    let text = "aaaa\n\nbbbb\n\ncccc";
    assert_eq!(split_message(text, 10), vec!["aaaa\n\nbbbb", "cccc"]);
    let long_line = "x".repeat(25);
    let parts = split_message(&long_line, 10);
    assert_eq!(parts, vec!["x".repeat(10), "x".repeat(10), "x".repeat(5)]);
    let unicode = "é".repeat(15);
    assert!(
        split_message(&unicode, 10)
            .iter()
            .all(|p| p.chars().count() <= 10)
    );
    assert!(split_message("   \n\n  ", 10).is_empty());
}

#[test]
fn only_well_formed_chat_guids_are_sent_to() {
    for ok in [
        DM,
        FAMILY_GROUP,
        "SMS;-;+15551230001",
        "any;-;parent@example.com",
    ] {
        assert!(api::is_valid_chat_guid(ok), "{ok}");
    }
    for bad in [
        "+15551230001",
        "iMessage;x;abc",
        "iMessage;-;",
        r#"iMessage;-;a" & do shell script "x"#,
        "iMessage;+;chat$(curl x|sh)",
        "iMessage;-;a$(id)@b.c",
        "iMessage;-;a`id`@b.c",
        "iMessage;-;a b",
        "iMessage;-;a\\b",
        "i Message;-;a",
        "iMessage;-;a\nb",
    ] {
        assert!(!api::is_valid_chat_guid(bad), "{bad:?}");
    }
    assert_eq!(
        super::chat_guid_for("+15551230001"),
        "iMessage;-;+15551230001"
    );
    assert_eq!(super::chat_guid_for(FAMILY_GROUP), FAMILY_GROUP);
}

// ── HTTP: a fake BlueBubbles server ─────────────────────────────

/// Answers `message/query` from a fixed set of rows the way BlueBubbles
/// does: filtered by the ROWID `where` clauses, ordered by `dateCreated`
/// (never by ROWID), and capped at `limit`.
struct FakeQuery {
    rows: Vec<Value>,
    password: String,
}

impl Respond for FakeQuery {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let authed = req
            .url
            .query_pairs()
            .any(|(k, v)| k == "password" && v == self.password.as_str());
        if !authed {
            return ResponseTemplate::new(401);
        }
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let mut lo = i64::MIN;
        let mut hi = i64::MAX;
        for clause in body["where"].as_array().cloned().unwrap_or_default() {
            let stmt = clause["statement"].as_str().unwrap_or_default();
            if stmt.contains(":lo") {
                lo = clause["args"]["lo"].as_i64().unwrap();
            }
            if stmt.contains(":hi") {
                hi = clause["args"]["hi"].as_i64().unwrap();
            }
        }
        let chat = body["chatGuid"].as_str();
        let after = body["after"].as_i64();
        let mut rows: Vec<Value> = self
            .rows
            .iter()
            .filter(|r| {
                let id = r["originalROWID"].as_i64().unwrap();
                id > lo
                    && id <= hi
                    && chat.is_none_or(|c| r["chats"][0]["guid"] == c)
                    && after.is_none_or(|a| r["dateCreated"].as_i64().unwrap_or(0) >= a)
            })
            .cloned()
            .collect();
        rows.sort_by_key(|r| r["dateCreated"].as_i64().unwrap_or(0));
        if body["sort"] == "DESC" {
            rows.reverse();
        }
        let limit = body["limit"].as_u64().unwrap_or(1000) as usize;
        rows.truncate(limit);
        ResponseTemplate::new(200).set_body_json(json!({"status": 200, "data": rows}))
    }
}

async fn fake_server(rows: Vec<Value>, password: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/query"))
        .respond_with(FakeQuery {
            rows,
            password: password.to_string(),
        })
        .mount(&server)
        .await;
    server
}

fn config(server: &str, password: &str) -> BlueBubblesConfig {
    BlueBubblesConfig {
        enabled: true,
        server_url: server.to_string(),
        password: password.to_string(),
        self_handles: vec![BOT.to_string()],
        group_chat_guids: vec![FAMILY_GROUP.to_string()],
        mention_names: vec!["Early".to_string()],
        ..BlueBubblesConfig::default()
    }
}

fn channel(server: &str, password: &str) -> BlueBubblesChannel {
    let peers = family_peers();
    BlueBubblesChannel::new(
        "family",
        &config(server, password),
        Arc::new(move || peers.clone()),
    )
    .unwrap()
}

#[tokio::test]
async fn polling_hands_off_every_row_in_row_order_across_windows() {
    let pw = test_password();
    // 1,200 rows whose dates run opposite to their row IDs, so the server's
    // date ordering would truncate any query that is not a bounded window.
    let base = now_ms();
    let rows: Vec<Value> = (1..=1_200)
        .map(|id| {
            with(
                row(id, DM, Some(KID), &format!("m{id}")),
                "dateCreated",
                json!(base - id),
            )
        })
        .collect();
    let server = fake_server(rows, &pw).await;
    let ch = channel(&server.uri(), &pw);
    let (tx, mut rx) = mpsc::channel(2_000);
    let mut cur = saved(0);
    ch.poll_once(&tx, &mut cur, true).await.unwrap();
    drop(tx);
    let mut got = Vec::new();
    while let Some(m) = rx.recv().await {
        got.push(m.content);
    }
    let want: Vec<String> = (1..=1_200).map(|id| format!("m{id}")).collect();
    assert_eq!(got, want);
    assert_eq!(cur.rowid, 1_200);
}

#[tokio::test]
async fn a_late_message_with_an_old_date_is_still_picked_up() {
    let pw = test_password();
    let rows = vec![
        row(10, DM, Some(KID), "first"),
        // Stored after row 10, but dated two days earlier.
        with(
            row(11, DM, Some(KID), "late"),
            "dateCreated",
            json!(now_ms() - 48 * HOUR),
        ),
    ];
    let server = fake_server(rows, &pw).await;
    let ch = channel(&server.uri(), &pw);
    let (tx, mut rx) = mpsc::channel(10);
    let mut cur = saved(10);
    cur.remember("guid-10");
    ch.poll_once(&tx, &mut cur, true).await.unwrap();
    drop(tx);
    let got = rx.recv().await.expect("the late message");
    assert_eq!(got.content, "late");
    assert_eq!(cur.rowid, 11);
}

#[tokio::test]
async fn polling_moves_past_dropped_and_unreadable_rows_and_gaps() {
    let pw = test_password();
    let rows = vec![
        row(1, "iMessage;-;+15559990000", Some(STRANGER), "spam"),
        json!({"originalROWID": 2, "guid": 7}),
        // A gap wider than one window, then a real message.
        row(2_000, DM, Some(KID), "after the gap"),
    ];
    let server = fake_server(rows, &pw).await;
    let ch = channel(&server.uri(), &pw);
    let (tx, mut rx) = mpsc::channel(10);
    let mut cur = saved(0);
    ch.poll_once(&tx, &mut cur, true).await.unwrap();
    drop(tx);
    assert_eq!(rx.recv().await.unwrap().content, "after the gap");
    assert!(rx.recv().await.is_none());
    assert_eq!(cur.rowid, 2_000);
}

/// A server that ignores the query's `where` and `limit` and answers every
/// query with the same rows.
struct IgnoresWindow(Vec<Value>);

impl Respond for IgnoresWindow {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({"status": 200, "data": self.0}))
    }
}

#[tokio::test]
async fn a_server_that_ignores_the_window_gets_each_row_handed_off_once() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/query"))
        .respond_with(IgnoresWindow(vec![
            row(1, DM, Some(KID), "one"),
            row(2, DM, Some(KID), "two"),
            row(900, DM, Some(KID), "nine hundred"),
        ]))
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw);
    let (tx, mut rx) = mpsc::channel(100);
    let mut cur = saved(0);
    tokio::time::timeout(Duration::from_secs(10), ch.poll_once(&tx, &mut cur, true))
        .await
        .expect("the scan ends")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), ch.poll_once(&tx, &mut cur, true))
        .await
        .expect("the scan ends")
        .unwrap();
    drop(tx);
    let mut got = Vec::new();
    while let Some(m) = rx.recv().await {
        got.push(m.content);
    }
    assert_eq!(got, ["one", "two", "nine hundred"]);
    assert_eq!(cur.rowid, 900);
}

#[tokio::test]
async fn rows_below_the_window_are_not_handed_off_again() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/query"))
        .respond_with(IgnoresWindow(vec![
            row(1, DM, Some(KID), "one"),
            row(2, DM, Some(KID), "two"),
        ]))
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw);
    let (tx, mut rx) = mpsc::channel(100);
    // Long past these rows, and no longer remembering their GUIDs.
    let mut cur = saved(5_000);
    tokio::time::timeout(Duration::from_secs(10), ch.poll_once(&tx, &mut cur, true))
        .await
        .expect("the scan ends")
        .unwrap();
    drop(tx);
    assert!(rx.recv().await.is_none(), "an old row was handed off again");
}

#[tokio::test]
async fn a_response_past_the_size_cap_is_an_error_not_a_crash() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(17 * 1024 * 1024)))
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw);
    let (tx, _rx) = mpsc::channel(10);
    let mut cur = saved(0);
    let err = ch.poll_once(&tx, &mut cur, true).await.unwrap_err();
    assert!(format!("{err:#}").contains("larger than"), "{err:#}");
}

#[test]
fn an_unreadable_row_keeps_no_text_from_it() {
    let row = json!({"originalROWID": 5, "guid": "g", "text": "secret plans", "isFromMe": "secret plans"});
    match api::parse_row(&row) {
        PolledRow::Unreadable { rowid, error } => {
            assert_eq!(rowid, Some(5));
            assert!(!error.contains("secret"), "{error}");
        }
        PolledRow::Message(_) => panic!("should not parse"),
    }
}

#[tokio::test]
async fn a_failed_send_keeps_only_the_status_type_and_applescript_error() {
    let pw = test_password();
    let server = MockServer::start().await;
    // The message JSON-escaped, which a plain replace of the text would miss.
    Mock::given(method("POST"))
        .and(path("/api/v1/message/text"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "status": 500,
            "error": {"type": "Server Error",
                      "message": "osascript: send \"plans\nfor Friday\" to participant +15551230001 (-1743)"}
        })))
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw);
    let err = ch
        .send(&SendMessage::new("\"plans\"\nfor Friday", DM))
        .await
        .unwrap_err();
    let shown = format!("{err:#}");
    assert!(
        !shown.contains("plans") && !shown.contains("Friday"),
        "message leaked: {shown}"
    );
    assert!(!shown.contains("5551230001"), "handle leaked: {shown}");
    assert!(
        shown.contains("Server Error") && shown.contains("-1743"),
        "{shown}"
    );
}

#[tokio::test]
async fn a_row_seen_late_below_the_cursor_is_handed_off_once() {
    let pw = test_password();
    let rows = vec![
        row(9, DM, Some(KID), "linked late"),
        row(10, DM, Some(KID), "seen"),
    ];
    let server = fake_server(rows, &pw).await;
    let ch = channel(&server.uri(), &pw);
    let (tx, mut rx) = mpsc::channel(10);
    // Row 10 was handled; row 9 only became visible afterwards.
    let mut cur = saved(10);
    cur.remember("guid-10");
    ch.poll_once(&tx, &mut cur, true).await.unwrap();
    ch.poll_once(&tx, &mut cur, true).await.unwrap();
    drop(tx);
    assert_eq!(rx.recv().await.unwrap().content, "linked late");
    assert!(rx.recv().await.is_none());
}

#[tokio::test]
async fn the_cursor_is_written_to_the_workspace() {
    let pw = test_password();
    let server = fake_server(vec![row(5, DM, Some(KID), "hi")], &pw).await;
    let dir = tempfile::tempdir().unwrap();
    let ch = channel(&server.uri(), &pw).with_workspace_dir(dir.path().to_path_buf());
    let (tx, _rx) = mpsc::channel(10);
    let (mut cur, start) = ch.start_cursor().await.unwrap();
    assert_eq!(start, Start::Seed(5));
    ch.poll_once(&tx, &mut cur, true).await.unwrap();
    let on_disk = cursor::load(&cursor::cursor_path(dir.path(), "family")).unwrap();
    assert_eq!(on_disk.rowid, 5);
}

#[tokio::test]
async fn a_fresh_start_hands_off_none_of_the_existing_history() {
    let pw = test_password();
    // Recent messages, some only seconds old, already on the server.
    let rows: Vec<Value> = (1..=40)
        .map(|id| row(id, DM, Some(KID), &format!("old {id}")))
        .collect();
    let server = fake_server(rows, &pw).await;
    let ch = channel(&server.uri(), &pw);
    let (tx, mut rx) = mpsc::channel(100);
    let (mut cur, start) = ch.start_cursor().await.unwrap();
    assert_eq!(start, Start::Seed(40));
    ch.poll_once(&tx, &mut cur, start.is_resume())
        .await
        .unwrap();
    drop(tx);
    assert!(
        rx.recv().await.is_none(),
        "a fresh start must not replay history"
    );
}

#[tokio::test]
async fn a_send_posts_an_applescript_text_to_the_chat() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/text"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"status": 200, "data": {"guid": "SENT-1"}})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw);
    ch.send(&SendMessage::new("hello", DM)).await.unwrap();
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let req = &requests[0];
    assert!(
        req.url
            .query_pairs()
            .any(|(k, v)| k == "password" && v == pw.as_str())
    );
    let body: Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body["chatGuid"], DM);
    assert_eq!(body["message"], "hello");
    assert_eq!(body["method"], "apple-script");
    assert!(body["tempGuid"].as_str().unwrap().starts_with("temp-"));
    // Nothing that would quietly switch the server to the Private API.
    for forbidden in [
        "effectId",
        "subject",
        "selectedMessageGuid",
        "ddScan",
        "attributedBody",
    ] {
        assert!(body.get(forbidden).is_none(), "{forbidden}");
    }
    assert!(ch.own_guids.lock().contains("SENT-1"));
}

#[tokio::test]
async fn a_failed_send_never_shows_the_password() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/text"))
        .respond_with(ResponseTemplate::new(500).set_body_string(format!(
            "error at /api/v1/message/text?password={pw}: Not authorized to send Apple events (-1743); auth was {pw}"
        )))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": 200, "data": []})))
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw);
    let err = ch.send(&SendMessage::new("hello", DM)).await.unwrap_err();
    let shown = format!("{err} | {err:?} | {err:#}");
    assert!(!shown.contains(&pw), "password leaked: {shown}");
    assert!(
        shown.contains("-1743"),
        "the cause should survive redaction: {shown}"
    );
}

#[tokio::test]
async fn a_failed_send_never_shows_the_handle_or_the_message() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/text"))
        .respond_with(ResponseTemplate::new(500).set_body_string(format!(
            "osascript -e send \"secret plans for Friday\" to participant \"{KID}\" of {DM}: (-1743)"
        )))
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw);
    let err = ch
        .send(&SendMessage::new("secret plans for Friday", DM))
        .await
        .unwrap_err();
    let shown = format!("{err:#}");
    assert!(
        !shown.contains("secret plans"),
        "message text leaked: {shown}"
    );
    assert!(!shown.contains(KID), "handle leaked: {shown}");
    assert!(shown.contains("-1743"), "the cause should survive: {shown}");
}

#[tokio::test]
async fn an_unreachable_server_error_never_shows_the_password_or_url() {
    let pw = test_password();
    // Bind and drop a listener so the port is closed.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let ch = channel(&format!("http://127.0.0.1:{port}"), &pw);
    let err = ch.send(&SendMessage::new("hello", DM)).await.unwrap_err();
    let shown = format!("{err} | {err:?} | {err:#}");
    assert!(!shown.contains(&pw), "password leaked: {shown}");
    assert!(!shown.contains("password="), "URL leaked: {shown}");
    let (tx, _rx) = mpsc::channel(1);
    let mut cur = saved(0);
    let err = ch.poll_once(&tx, &mut cur, true).await.unwrap_err();
    let shown = format!("{err} | {err:?} | {err:#}");
    assert!(!shown.contains(&pw), "password leaked: {shown}");
}

#[tokio::test]
async fn a_definite_failure_is_not_mistaken_for_a_landed_send() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/text"))
        .respond_with(ResponseTemplate::new(500).set_body_string("-1743"))
        .mount(&server)
        .await;
    // The same short reply already sits in the chat from a minute ago.
    let earlier = with(
        with(row(7, DM, None, "OK"), "isFromMe", json!(true)),
        "handle",
        Value::Null,
    );
    Mock::given(method("POST"))
        .and(path("/api/v1/message/query"))
        .respond_with(FakeQuery {
            rows: vec![earlier],
            password: pw.clone(),
        })
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw);
    assert!(ch.send(&SendMessage::new("OK", DM)).await.is_err());
    let queried = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .any(|r| r.url.path() == "/api/v1/message/query");
    assert!(
        !queried,
        "a 500 is a definite failure; the chat is not searched"
    );
}

#[tokio::test]
async fn a_timed_out_send_is_not_confirmed_by_an_earlier_identical_reply() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/text"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(400)))
        .mount(&server)
        .await;
    let earlier = with(
        with(row(7, DM, None, "OK"), "isFromMe", json!(true)),
        "handle",
        Value::Null,
    );
    Mock::given(method("POST"))
        .and(path("/api/v1/message/query"))
        .respond_with(FakeQuery {
            rows: vec![earlier],
            password: pw.clone(),
        })
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw).with_send_timeout(Duration::from_millis(100));
    // The poller already saw the earlier "OK".
    ch.own_guids.lock().insert("guid-7".to_string());
    assert!(ch.send(&SendMessage::new("OK", DM)).await.is_err());
}

#[tokio::test]
async fn a_server_url_with_a_path_keeps_it() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/bb/api/v1/server/info"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": 200})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::path_regex(
            r"^/bb/api/v1/message/[^/]+$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": {"isFromMe": true}})))
        .mount(&server)
        .await;
    for url in [
        format!("{}/bb", server.uri()),
        format!("{}/bb/", server.uri()),
    ] {
        let api = BlueBubblesApi::new(&url, &pw).unwrap();
        api.server_info().await.unwrap();
        assert!(api.is_own_message("p:0/A B").await.unwrap());
    }
    let lookups: Vec<String> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| r.url.path().to_string())
        .filter(|p| p.contains("/message/"))
        .collect();
    assert!(
        lookups.iter().all(|p| p.ends_with("%2FA%20B")),
        "the GUID is one path segment: {lookups:?}"
    );
}

#[test]
fn config_needs_a_server_and_password_only_when_enabled() {
    let mut cfg = BlueBubblesConfig::default();
    assert!(!cfg.has_required_credentials());
    assert!(
        cfg.validate_required("family").is_ok(),
        "a disabled alias may be incomplete"
    );
    cfg.enabled = true;
    assert!(cfg.validate_required("family").is_err());
    cfg.server_url = "http://mac:1234".into();
    cfg.password = test_password();
    assert!(cfg.has_required_credentials());
    assert!(cfg.validate_required("family").is_ok());
    assert_eq!(cfg.poll_interval_secs, 3);
}

#[test]
fn a_config_block_parses_from_toml_with_its_defaults() {
    let cfg: BlueBubblesConfig = toml::from_str(
        r#"
        enabled = true
        server_url = "http://mac:1234"
        password = "x"
        self_handles = ["+15550000000"]
        group_chat_guids = ["iMessage;+;chat100"]
        mention_names = ["Early"]
        "#,
    )
    .unwrap();
    assert!(cfg.enabled);
    assert_eq!(cfg.poll_interval_secs, 3);
    assert_eq!(cfg.mention_names, vec!["Early"]);
    assert!(cfg.excluded_tools.is_empty());
}

#[tokio::test]
async fn a_refused_connection_is_retried_once_and_a_timeout_never() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/text"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(400)))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": 200, "data": []})))
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw).with_send_timeout(Duration::from_millis(100));
    assert!(ch.send(&SendMessage::new("hello", DM)).await.is_err());
    let sends = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path() == "/api/v1/message/text")
        .count();
    assert_eq!(sends, 1, "a send that timed out must not be repeated");

    let api = BlueBubblesApi::new("http://127.0.0.1:9", &pw).unwrap();
    let err = api.send_text(DM, "hi", "temp-1").await.unwrap_err();
    let api::SendError::Connect(e) = err else {
        panic!("expected a connect error, got {err:?}");
    };
    // The URL carries the password in its query string, so it is dropped
    // whole rather than trusted to redaction.
    let text = format!("{e:#}");
    assert!(!text.contains("127.0.0.1:9"), "URL leaked into: {text}");
    assert!(!text.contains("password="), "URL leaked into: {text}");
}

/// BlueBubbles 1.9.9 does not deduplicate on `tempGuid`, so any resend of a
/// request the server read would text twice. A server that reads the request
/// and then drops the connection, without answering, must see it once.
#[tokio::test]
async fn a_request_the_server_read_is_never_sent_again() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = seen.clone();
    zeroclaw_spawn::spawn!(async move {
        while let Ok((mut conn, _)) = listener.accept().await {
            let mut buf = vec![0u8; 64 * 1024];
            let mut got = Vec::new();
            while let Ok(n) = conn.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..n]);
                if got.windows(4).any(|w| w == b"\r\n\r\n") && got.ends_with(b"}") {
                    break;
                }
            }
            if got.starts_with(b"POST /api/v1/message/text") {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            drop(conn);
        }
    });
    let api = BlueBubblesApi::new(&format!("http://{addr}"), &test_password()).unwrap();
    let err = api.send_text(DM, "hello", "temp-1").await.unwrap_err();
    assert!(
        !matches!(err, api::SendError::Connect(_)),
        "a dropped connection after the request was written is not a connect error: {err:?}"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_server_error_is_never_sent_again() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/text"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw);
    assert!(ch.send(&SendMessage::new("hello", DM)).await.is_err());
    let sends = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path() == "/api/v1/message/text")
        .count();
    assert_eq!(
        sends, 1,
        "a send the server answered with an error must not be repeated"
    );
}

#[tokio::test]
async fn a_timed_out_send_that_landed_is_reported_as_sent() {
    let pw = test_password();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/message/text"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(400)))
        .mount(&server)
        .await;
    let landed = with(
        with(row(7, DM, None, "hello"), "isFromMe", json!(true)),
        "handle",
        Value::Null,
    );
    Mock::given(method("POST"))
        .and(path("/api/v1/message/query"))
        .respond_with(FakeQuery {
            rows: vec![landed],
            password: pw.clone(),
        })
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw).with_send_timeout(Duration::from_millis(100));
    ch.send(&SendMessage::new("hello", DM)).await.unwrap();
}

#[tokio::test]
async fn an_injected_chat_guid_is_refused_before_any_request() {
    let pw = test_password();
    let server = MockServer::start().await;
    let ch = channel(&server.uri(), &pw);
    let err = ch
        .send(&SendMessage::new(
            "hi",
            r#"iMessage;-;a" & do shell script "x"#,
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not a valid chat GUID"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_redirect_is_not_followed() {
    let pw = test_password();
    let server = MockServer::start().await;
    let elsewhere = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/server/info"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/steal", elsewhere.uri()).as_str()),
        )
        .mount(&server)
        .await;
    let ch = channel(&server.uri(), &pw);
    assert!(!ch.health_check().await);
    assert!(elsewhere.received_requests().await.unwrap().is_empty());
}

#[test]
fn the_listener_reports_pending_then_tracks_poll_results() {
    let ch = channel("http://127.0.0.1:9", &test_password());
    use zeroclaw_api::channel::ListenerHealth;
    assert_eq!(ch.listener_health(), Some(ListenerHealth::Pending));
    ch.record_success();
    assert_eq!(ch.listener_health(), Some(ListenerHealth::Healthy));
    ch.record_failure();
    assert_eq!(ch.listener_health(), Some(ListenerHealth::Unhealthy));
    ch.record_success();
    assert_eq!(ch.listener_health(), Some(ListenerHealth::Healthy));
}

#[tokio::test]
async fn pacing_keeps_who_the_channel_is() {
    let pw = test_password();
    let cfg = BlueBubblesConfig {
        reply_min_interval_secs: 1,
        ..config("http://127.0.0.1:9", &pw)
    };
    let inner: Arc<dyn Channel> = Arc::new(channel("http://127.0.0.1:9", &pw));
    let paced = crate::paced_channel::PacedChannel::wrap(inner.clone(), &cfg);
    assert!(
        !Arc::ptr_eq(&paced, &inner),
        "pacing should wrap the channel"
    );
    let dm = zeroclaw_api::channel::ChannelMessage::new("1", KID, DM, "hi", "bluebubbles", 0);
    let group =
        zeroclaw_api::channel::ChannelMessage::new("2", KID, FAMILY_GROUP, "hi", "bluebubbles", 0);
    assert_eq!(paced.self_handle(), inner.self_handle());
    assert_eq!(paced.self_handle().as_deref(), Some(BOT));
    assert_eq!(
        paced.self_addressed_mention(),
        inner.self_addressed_mention()
    );
    assert!(paced.self_addressed_mention().is_some());
    assert!(paced.is_direct_message(&dm));
    assert!(!paced.is_direct_message(&group));
}

#[test]
fn direct_messages_are_recognized_from_the_reply_target() {
    let ch = channel("http://127.0.0.1:9", &test_password());
    let dm = zeroclaw_api::channel::ChannelMessage::new("1", KID, DM, "hi", "bluebubbles", 0);
    let group =
        zeroclaw_api::channel::ChannelMessage::new("2", KID, FAMILY_GROUP, "hi", "bluebubbles", 0);
    assert!(ch.is_direct_message(&dm));
    assert!(!ch.is_direct_message(&group));
    assert_eq!(ch.self_handle().as_deref(), Some(BOT));
}
