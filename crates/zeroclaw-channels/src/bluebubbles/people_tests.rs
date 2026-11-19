//! Tests for the people map, its source and the control socket
//! (kmsquire/llm-workspace#181). Every handle here is made up: 555-01xx
//! numbers and example.com addresses. Expected values are written out, not
//! derived from the code under test.

use super::control;
use super::people::{People, Role};
use super::source::PeopleSource;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const GOOD: &str = r#"{"vault_version": 7, "people": {
  "test-owner": {"role": "owner", "handles": ["+15550100001"], "preferred": "+15550100001"},
  "test-kid": {"role": "child", "handles": ["+15550100002", "kid@example.com"],
               "preferred": "kid@example.com"}}}"#;

const VECTORS: &str = include_str!("family-handle-vectors.json");
/// The canonical file is kmsquire/llm-workspace's scripts/family-handle-vectors.json. A copy that
/// drifts from it fails here; update both together.
const VECTORS_SHA256: &str = "9aad4d6cb06045ef3ad05c42d2578141ebf7b2c9a8c4aefecec98f96bc9b0ff5";

#[test]
fn the_shared_vectors_are_llm_workspaces_byte_for_byte() {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(VECTORS.as_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, VECTORS_SHA256);
}

#[test]
fn the_shared_vectors_pass() {
    let doc: serde_json::Value = serde_json::from_str(VECTORS).unwrap();
    let vectors = doc["vectors"].as_array().unwrap();
    assert!(vectors.len() >= 35, "the vectors file lost entries");
    for v in vectors {
        let name = v["name"].as_str().unwrap();
        let text = v["text"].as_str().unwrap();
        let ok = v["ok"].as_bool().unwrap();
        let got = People::parse(text.as_bytes());
        assert_eq!(got.is_ok(), ok, "vector {name:?}");
        if let Err(e) = got {
            assert!(
                !e.contains("555") && !e.contains('@'),
                "vector {name:?}: the error names a handle"
            );
        }
    }
}

#[test]
fn the_shared_normalize_vectors_pass() {
    let doc: serde_json::Value = serde_json::from_str(VECTORS).unwrap();
    let cases = doc["normalize"].as_array().unwrap();
    assert!(cases.len() >= 20, "the normalize vectors lost entries");
    for v in cases {
        let raw = v["raw"].as_str().unwrap();
        assert_eq!(
            super::people::canonical_handle(raw).as_deref(),
            v["want"].as_str(),
            "normalize {raw:?}"
        );
    }
}

#[test]
fn a_sender_in_any_reported_form_finds_its_person() {
    let p = People::parse(GOOD.as_bytes()).unwrap();
    // What BlueBubbles reports, after the channel drops a service prefix.
    for reported in [
        "+15550100002",
        "5550100002",
        "iMessage:5550100002",
        "Kid@Example.com",
    ] {
        let normalized = super::inbound::normalize_handle(reported);
        assert_eq!(
            p.by_handle(&normalized).map(|x| x.id.as_str()),
            Some("test-kid"),
            "{reported:?}"
        );
    }
    assert!(p.by_handle("5550100009").is_none());
}

#[test]
fn a_good_map_answers_by_handle_and_by_id() {
    let p = People::parse(GOOD.as_bytes()).unwrap();
    let kid = p.by_handle("kid@example.com").unwrap();
    assert_eq!(kid.id, "test-kid");
    assert_eq!(kid.role, Role::Child);
    assert_eq!(kid.preferred.as_deref(), Some("kid@example.com"));
    assert_eq!(p.by_handle("+15550100002").unwrap().id, "test-kid");
    assert_eq!(p.by_id("test-owner").unwrap().role, Role::Owner);
    assert!(p.by_handle("+15550100009").is_none());
    assert_eq!(p.handle_count(), 3);
    assert_eq!(p.vault_version, Some(7));
    assert_eq!(
        p.summary().to_string(),
        "2 people (1 child, 1 owner), 3 handles, Vault version 7"
    );
}

#[test]
fn debug_and_summary_never_print_a_handle() {
    let p = People::parse(GOOD.as_bytes()).unwrap();
    for text in [
        format!("{p:?}"),
        serde_json::to_string(&p.summary()).unwrap(),
    ] {
        assert!(!text.contains("555") && !text.contains('@'), "{text}");
    }
}

#[test]
fn the_vault_version_doesnt_count_as_a_change() {
    let a = People::parse(GOOD.as_bytes()).unwrap();
    let b = People::parse(
        GOOD.replace("\"vault_version\": 7", "\"vault_version\": 8")
            .as_bytes(),
    )
    .unwrap();
    assert!(a.same_content(&b));
    let c = People::parse(
        GOOD.replace("\"role\": \"child\"", "\"role\": \"adult\"")
            .as_bytes(),
    )
    .unwrap();
    assert!(!a.same_content(&c));
}

/// A command for `people_command`: a shell script in `dir` that prints
/// `stdout`, writes `stderr` to stderr and exits `code`.
fn script(dir: &Path, name: &str, stdout: &str, stderr: &str, code: i32) -> String {
    let path = dir.join(name);
    let body = format!(
        "#!/bin/sh\ncat <<'EOF_OUT'\n{stdout}\nEOF_OUT\necho '{stderr}' >&2\nexit {code}\n"
    );
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path.to_string_lossy().into_owned()
}

/// A source whose command can be switched between scripts by renaming.
fn switchable(dir: &Path) -> (PeopleSource, PathBuf) {
    let cmd = dir.join("people-command");
    (
        PeopleSource::new(cmd.to_str().unwrap()).with_timeout(Duration::from_secs(5)),
        cmd,
    )
}

fn use_script(cmd: &Path, script: &str) {
    std::fs::copy(script, cmd).unwrap();
    std::fs::set_permissions(cmd, std::fs::Permissions::from_mode(0o700)).unwrap();
}

#[tokio::test]
async fn nothing_is_loaded_until_a_good_map_arrives() {
    let dir = tempfile::tempdir().unwrap();
    let (source, cmd) = switchable(dir.path());
    assert!(source.current().is_none());
    use_script(&cmd, &script(dir.path(), "fail", "", "vault said no", 1));
    let r = source.reload().await;
    assert!(!r.ok && r.loaded.is_none());
    assert_eq!(
        r.error.as_deref(),
        Some("people_command exited with status 1")
    );
    assert!(source.current().is_none());
    use_script(&cmd, &script(dir.path(), "good", GOOD, "", 0));
    let r = source.reload().await;
    assert!(r.ok && r.changed);
    assert_eq!(r.loaded.unwrap().handles, 3);
    assert!(source.current().is_some());
}

#[tokio::test]
async fn a_bad_or_empty_map_never_replaces_a_good_one() {
    let dir = tempfile::tempdir().unwrap();
    let (source, cmd) = switchable(dir.path());
    use_script(&cmd, &script(dir.path(), "good", GOOD, "", 0));
    assert!(source.reload().await.ok);
    let no_handles = r#"{"people": {"test-owner": {"role": "owner", "handles": []}}}"#;
    let cases = [
        script(dir.path(), "fail", "", "", 3),
        script(dir.path(), "invalid", r#"{"people": {}}"#, "", 0),
        script(dir.path(), "truncated", &GOOD[..40], "", 0),
        script(dir.path(), "empty", no_handles, "", 0),
    ];
    for case in &cases {
        use_script(&cmd, case);
        let r = source.reload().await;
        assert!(!r.ok, "{case}");
        assert_eq!(r.loaded.as_ref().unwrap().handles, 3, "{case}");
        assert_eq!(source.current().unwrap().handle_count(), 3, "{case}");
    }
}

#[tokio::test]
async fn reloading_the_same_map_reports_no_change() {
    let dir = tempfile::tempdir().unwrap();
    let (source, cmd) = switchable(dir.path());
    use_script(&cmd, &script(dir.path(), "good", GOOD, "", 0));
    assert!(source.reload().await.changed);
    let again = source.reload().await;
    assert!(again.ok && !again.changed);
}

#[tokio::test]
async fn the_commands_stderr_never_reaches_the_report() {
    let dir = tempfile::tempdir().unwrap();
    let (source, cmd) = switchable(dir.path());
    use_script(
        &cmd,
        &script(dir.path(), "noisy", "", "+15550100001 secret", 2),
    );
    let r = serde_json::to_string(&source.reload().await).unwrap();
    assert!(!r.contains("555") && !r.contains("secret"), "{r}");
}

#[tokio::test]
async fn a_slow_or_huge_command_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let slow = dir.path().join("slow");
    std::fs::write(&slow, "#!/bin/sh\nsleep 30\n").unwrap();
    std::fs::set_permissions(&slow, std::fs::Permissions::from_mode(0o700)).unwrap();
    let source = PeopleSource::new(slow.to_str().unwrap()).with_timeout(Duration::from_millis(300));
    let r = source.reload().await;
    assert_eq!(
        r.error.as_deref(),
        Some("people_command took longer than 300ms")
    );

    let huge = dir.path().join("huge");
    std::fs::write(
        &huge,
        "#!/bin/sh\nhead -c 300000 /dev/zero | tr '\\0' ' '\n",
    )
    .unwrap();
    std::fs::set_permissions(&huge, std::fs::Permissions::from_mode(0o700)).unwrap();
    let r = PeopleSource::new(huge.to_str().unwrap()).reload().await;
    assert_eq!(
        r.error.as_deref(),
        Some("people_command printed more than 262144 bytes")
    );
}

#[tokio::test]
async fn a_missing_command_fails_closed() {
    let r = PeopleSource::new("/nonexistent/people-command")
        .reload()
        .await;
    assert!(!r.ok);
    assert!(
        r.error
            .unwrap()
            .starts_with("people_command could not start")
    );
}

fn private_dir() -> (tempfile::TempDir, u32) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let uid = std::fs::metadata(dir.path()).unwrap().uid();
    (dir, uid)
}

async fn ask(path: &Path, request: &str) -> serde_json::Value {
    let mut s = tokio::net::UnixStream::connect(path).await.unwrap();
    s.write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    assert!(
        out.ends_with('\n') && out.matches('\n').count() == 1,
        "{out:?}"
    );
    assert!(!out.contains("555") && !out.contains('@'), "{out}");
    serde_json::from_str(&out).unwrap()
}

#[tokio::test]
async fn the_control_socket_reloads_and_reports_counts() {
    let (dir, uid) = private_dir();
    let (source, cmd) = switchable(dir.path());
    use_script(&cmd, &script(dir.path(), "good", GOOD, "", 0));
    let sock = dir.path().join("family.sock");
    let listener = control::bind(&sock, uid).unwrap();
    assert_eq!(
        std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let serving = control::serve(listener, uid, source.clone(), "family".into());
    let task = zeroclaw_spawn::spawn!(serving);

    assert_eq!(
        ask(&sock, "status").await,
        serde_json::json!({"ok": false, "changed": false})
    );
    assert_eq!(
        ask(&sock, "reload").await,
        serde_json::json!({"ok": true, "changed": true, "loaded": {
            "people": 2, "handles": 3, "roles": {"child": 1, "owner": 1}, "vault_version": 7}})
    );
    assert_eq!(ask(&sock, "status").await["loaded"]["handles"], 3);
    assert_eq!(
        ask(&sock, "rm -rf /").await["error"],
        "unknown request; send reload or status"
    );
    task.abort();
}

#[tokio::test]
async fn the_control_socket_refuses_any_other_uid() {
    let (dir, uid) = private_dir();
    let (source, cmd) = switchable(dir.path());
    use_script(&cmd, &script(dir.path(), "good", GOOD, "", 0));
    let sock = dir.path().join("family.sock");
    // The directory check is for `uid`; the server is told to serve someone else.
    let listener = control::bind(&sock, uid).unwrap();
    let serving = control::serve(listener, uid + 1, source.clone(), "family".into());
    let task = zeroclaw_spawn::spawn!(serving);
    assert_eq!(
        ask(&sock, "reload").await,
        serde_json::json!({"ok": false, "changed": false, "error": "not allowed"})
    );
    assert!(
        source.current().is_none(),
        "a refused caller must not trigger a reload"
    );
    task.abort();
}

#[test]
fn the_control_socket_wants_a_private_directory_and_wont_clobber_a_file() {
    let (dir, uid) = private_dir();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let err = control::bind(&dir.path().join("s.sock"), uid).unwrap_err();
    assert!(err.to_string().contains("0700"), "{err}");

    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let err = control::bind(&dir.path().join("s.sock"), uid + 1).unwrap_err();
    assert!(err.to_string().contains("owned by uid"), "{err}");

    let file = dir.path().join("not-a-socket");
    std::fs::write(&file, "keep me").unwrap();
    let err = control::bind(&file, uid).unwrap_err();
    assert!(err.to_string().contains("isn't a socket"), "{err}");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "keep me");
}

#[tokio::test]
async fn a_live_socket_is_never_taken() {
    let (dir, uid) = private_dir();
    let (source, _) = switchable(dir.path());
    let sock = dir.path().join("family.sock");
    let listener = control::bind(&sock, uid).unwrap();
    let serving = control::serve(listener, uid, source, "family".into());
    let task = zeroclaw_spawn::spawn!(serving);
    let err = control::bind(&sock, uid).unwrap_err().to_string();
    assert!(err.contains("already in use"), "{err}");
    task.abort();
}

#[test]
fn error_bodies_lose_anything_shaped_like_a_handle() {
    let masked = super::api::mask_handles(
        "no row for iMessage;-;+1 (555) 010-0002 or kid@example.com; status 500, row 12",
    );
    assert!(!masked.contains("555") && !masked.contains('@'), "{masked}");
    assert!(
        masked.contains("status 500, row 12"),
        "short numbers stay: {masked}"
    );
}

#[test]
fn two_aliases_cant_share_a_control_socket() {
    let mut config = zeroclaw_config::schema::Config::default();
    for alias in ["family", "owner"] {
        config.channels.bluebubbles.insert(
            alias.to_string(),
            toml::from_str(
                "server_url = \"http://127.0.0.1:1\"\npassword = \"p\"\n\
                 people_command = \"/f\"\ncontrol_socket = \"/run/x/family.sock\"",
            )
            .unwrap(),
        );
    }
    let err = config.validate().unwrap_err().to_string();
    assert!(err.contains("are the same path"), "{err}");
}

#[tokio::test]
async fn a_stale_socket_is_replaced() {
    let (dir, uid) = private_dir();
    let sock = dir.path().join("family.sock");
    // A socket file nobody listens on, left by a process that has exited. Made
    // in another process: a listener dropped here could live on in a process
    // another test forks meanwhile (macOS sets close-on-exec after socket()).
    let made = std::process::Command::new("python3")
        .args([
            "-c",
            "import socket, sys; socket.socket(socket.AF_UNIX).bind(sys.argv[1])",
        ])
        .arg(&sock)
        .status()
        .unwrap();
    assert!(made.success() && sock.exists());
    control::bind(&sock, uid).unwrap();
}

#[test]
fn log_content_defaults_off_with_a_people_map_and_on_without() {
    use zeroclaw_api::channel::Channel;
    let base = zeroclaw_config::schema::BlueBubblesConfig {
        server_url: "http://127.0.0.1:1".into(),
        password: "test-password".into(),
        ..Default::default()
    };
    let peers: std::sync::Arc<dyn Fn() -> Vec<String> + Send + Sync> =
        std::sync::Arc::new(Vec::new);
    let logs = |cfg: &zeroclaw_config::schema::BlueBubblesConfig| {
        super::BlueBubblesChannel::new("family", cfg, peers.clone())
            .unwrap()
            .logs_message_content()
    };
    assert!(logs(&base), "without a people map, upstream's behavior");
    let family = zeroclaw_config::schema::BlueBubblesConfig {
        people_command: Some("/usr/local/sbin/fetch".into()),
        ..base.clone()
    };
    assert!(
        !logs(&family),
        "a people map keeps its text out of the log by default"
    );
    for (people_map, set, want) in [(false, false, false), (true, true, true)] {
        let cfg = zeroclaw_config::schema::BlueBubblesConfig {
            log_content: Some(set),
            ..if people_map {
                family.clone()
            } else {
                base.clone()
            }
        };
        assert_eq!(logs(&cfg), want, "an explicit log_content = {set} wins");
    }
}

#[test]
fn people_command_and_control_socket_are_checked_at_load() {
    let parse = |extra: &str| -> anyhow::Result<()> {
        let cfg: zeroclaw_config::schema::BlueBubblesConfig = toml::from_str(&format!(
            "enabled = true\nserver_url = \"http://127.0.0.1:1\"\npassword = \"p\"\n{extra}"
        ))?;
        cfg.validate_required("family")
    };
    assert!(parse("").is_ok());
    assert!(parse("people_command = \"/usr/local/sbin/fetch\"\ncontrol_socket = \"/run/x/family.sock\"\nlog_content = false").is_ok());
    let relative = parse("people_command = \"fetch\"").unwrap_err().to_string();
    assert!(
        relative.contains("people_command must be an absolute path"),
        "{relative}"
    );
    let alone = parse("control_socket = \"/run/x/family.sock\"")
        .unwrap_err()
        .to_string();
    assert!(alone.contains("control_socket needs"), "{alone}");
    let rel_sock = parse("people_command = \"/f\"\ncontrol_socket = \"family.sock\"")
        .unwrap_err()
        .to_string();
    assert!(
        rel_sock.contains("control_socket must be an absolute path"),
        "{rel_sock}"
    );
}

// ---- The channel with a people map -------------------------------------------------------

mod channel {
    use super::{GOOD, script, switchable, use_script};
    use crate::bluebubbles::BlueBubblesChannel;
    use crate::bluebubbles::cursor;
    use serde_json::{Value, json};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};
    use zeroclaw_api::channel::{Channel, ChannelMessage, SendMessage};
    use zeroclaw_config::schema::BlueBubblesConfig;

    const KID_PHONE: &str = "+15550100002";
    const KID_EMAIL: &str = "kid@example.com";
    const STRANGER: &str = "+15559990000";
    const GROUP: &str = "iMessage;+;chat100";
    const OTHER_GROUP: &str = "iMessage;+;chat200";

    fn dm(handle: &str) -> String {
        format!("iMessage;-;{handle}")
    }

    fn row(rowid: i64, chat: &str, sender: &str, text: &str) -> Value {
        json!({
            "guid": format!("guid-{rowid}"), "originalROWID": rowid, "text": text,
            "isFromMe": false, "dateCreated": crate::bluebubbles::now_ms(),
            "handle": {"address": sender, "service": "iMessage"}, "chats": [{"guid": chat}],
            "threadOriginatorGuid": null, "associatedMessageGuid": null,
            "associatedMessageType": null, "balloonBundleId": null, "itemType": 0,
            "groupActionType": 0, "error": 0, "isSystemMessage": false, "isServiceMessage": false,
        })
    }

    /// Answers message queries with `rows` in the window asked for, newest
    /// first when sorted DESC, and accepts every send.
    struct Server(Vec<Value>);

    impl Respond for Server {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            if req.url.path() == "/api/v1/message/text" {
                return ResponseTemplate::new(200)
                    .set_body_json(json!({"status": 200, "data": {"guid": "SENT"}}));
            }
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
            let (mut lo, mut hi) = (i64::MIN, i64::MAX);
            for clause in body["where"].as_array().cloned().unwrap_or_default() {
                let stmt = clause["statement"].as_str().unwrap_or_default();
                if stmt.contains(":lo") {
                    lo = clause["args"]["lo"].as_i64().unwrap();
                }
                if stmt.contains(":hi") {
                    hi = clause["args"]["hi"].as_i64().unwrap();
                }
            }
            let mut rows: Vec<Value> = self
                .0
                .iter()
                .filter(|r| {
                    let id = r["originalROWID"].as_i64().unwrap();
                    id > lo && id <= hi
                })
                .cloned()
                .collect();
            if body["sort"] == "DESC" {
                rows.reverse();
            }
            rows.truncate(body["limit"].as_u64().unwrap_or(1000) as usize);
            ResponseTemplate::new(200).set_body_json(json!({"status": 200, "data": rows}))
        }
    }

    async fn server(rows: Vec<Value>) -> MockServer {
        let s = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(Server(rows))
            .mount(&s)
            .await;
        s
    }

    fn config(server: &str, people_command: &str) -> BlueBubblesConfig {
        BlueBubblesConfig {
            enabled: true,
            server_url: server.to_string(),
            password: "test-password".into(),
            group_chat_guids: vec![GROUP.to_string()],
            mention_names: vec!["Early".to_string()],
            people_command: Some(people_command.to_string()),
            ..BlueBubblesConfig::default()
        }
    }

    fn channel(server: &str, people_command: &str) -> BlueBubblesChannel {
        BlueBubblesChannel::new(
            "family",
            &config(server, people_command),
            Arc::new(Vec::new),
        )
        .unwrap()
    }

    fn assert_no_handle(m: &ChannelMessage) {
        for field in [
            &m.sender,
            &m.reply_target,
            &m.id,
            m.platform_sender_id.as_ref().unwrap(),
        ] {
            assert!(
                !field.contains("555") && !field.contains('@'),
                "a handle left the channel: {field}"
            );
        }
    }

    async fn sent_to(server: &MockServer) -> Vec<String> {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.url.path() == "/api/v1/message/text")
            .map(|r| {
                serde_json::from_slice::<Value>(&r.body).unwrap()["chatGuid"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    #[tokio::test]
    async fn messages_name_the_person_never_the_handle() {
        let dir = tempfile::tempdir().unwrap();
        let cmd = script(dir.path(), "good", GOOD, "", 0);
        let rows = vec![
            row(1, &dm(KID_PHONE), KID_PHONE, "hi from my phone"),
            row(2, &dm(KID_EMAIL), KID_EMAIL, "hi from my laptop"),
            row(3, GROUP, KID_PHONE, "Early, what's for dinner?"),
            row(4, GROUP, STRANGER, "Early, who are you?"),
            row(5, &dm(STRANGER), STRANGER, "hello?"),
        ];
        let s = server(rows).await;
        let ch = channel(&s.uri(), &cmd);
        assert!(ch.people_ready().await);
        let (tx, mut rx) = mpsc::channel(10);
        let mut cur = cursor::Cursor {
            rowid: 0,
            recent: Vec::new(),
        };
        ch.poll_once(&tx, &mut cur, true).await.unwrap();
        drop(tx);
        let mut got = Vec::new();
        while let Some(m) = rx.recv().await {
            assert_no_handle(&m);
            got.push((
                m.content.clone(),
                m.sender.clone(),
                m.reply_target.clone(),
                m.passive_context,
            ));
        }
        assert_eq!(
            got,
            vec![
                (
                    "hi from my phone".into(),
                    "test-kid".into(),
                    "dm:test-kid".into(),
                    false
                ),
                (
                    "hi from my laptop".into(),
                    "test-kid".into(),
                    "dm:test-kid".into(),
                    false
                ),
                (
                    "Early, what's for dinner?".into(),
                    "test-kid".into(),
                    GROUP.into(),
                    false
                ),
                // Someone outside the map in a listed group: context, as "~unknown",
                // which no person ID can be.
                (
                    "Early, who are you?".into(),
                    "~unknown".into(),
                    GROUP.into(),
                    true
                ),
                // A DM from outside the map is dropped.
            ]
        );
        assert_eq!(cur.rowid, 5);
    }

    #[tokio::test]
    async fn config_peers_are_never_read_with_a_people_map() {
        let dir = tempfile::tempdir().unwrap();
        let cmd = script(dir.path(), "good", GOOD, "", 0);
        let err = BlueBubblesChannel::new(
            "family",
            &config("http://127.0.0.1:1", &cmd),
            Arc::new(|| vec![STRANGER.to_string()]),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(
            err.contains("takes its people from that map alone"),
            "{err}"
        );
        assert!(!err.contains("555"), "{err}");
    }

    #[tokio::test]
    async fn nothing_is_polled_until_the_map_loads_and_then_nothing_is_lost() {
        let dir = tempfile::tempdir().unwrap();
        let (_, cmd) = switchable(dir.path());
        let fail = script(dir.path(), "fail", "", "", 1);
        let good = script(dir.path(), "good", GOOD, "", 0);
        use_script(&cmd, &fail);
        let s = server(vec![row(
            7,
            &dm(KID_PHONE),
            KID_PHONE,
            "sent while Vault was down",
        )])
        .await;
        let workspace = tempfile::tempdir().unwrap();
        // A resumed channel, so what it missed is answered rather than taken as history.
        cursor::save(
            &cursor::cursor_path(workspace.path(), "family"),
            &cursor::Cursor {
                rowid: 6,
                recent: Vec::new(),
            },
        )
        .unwrap();
        let ch = channel(&s.uri(), cmd.to_str().unwrap())
            .with_workspace_dir(workspace.path().to_path_buf());
        let (tx, mut rx) = mpsc::channel(10);
        let listening = {
            let ch = ch.clone();
            async move { ch.listen(tx).await }
        };
        let listener = zeroclaw_spawn::spawn!(listening);
        tokio::time::sleep(Duration::from_millis(800)).await;
        let windows = s
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| String::from_utf8_lossy(&r.body).contains(":lo"))
            .count();
        assert_eq!(windows, 0, "the channel polled with no people map");
        assert!(rx.try_recv().is_err());
        let saved = cursor::load(&cursor::cursor_path(workspace.path(), "family")).unwrap();
        assert_eq!(saved.rowid, 6, "the cursor moved while the map was missing");

        use_script(&cmd, &good);
        let m = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("the message once the map loaded")
            .unwrap();
        assert_eq!(
            (m.content.as_str(), m.sender.as_str()),
            ("sent while Vault was down", "test-kid")
        );
        listener.abort();
    }

    #[tokio::test]
    async fn sends_go_only_to_the_family_and_listed_groups() {
        let dir = tempfile::tempdir().unwrap();
        let cmd = script(dir.path(), "good", GOOD, "", 0);
        let s = server(vec![row(1, &dm(KID_PHONE), KID_PHONE, "hi")]).await;
        // A channel that has never listened, like one built for a cron announcement.
        let ch = channel(&s.uri(), &cmd);
        ch.send(&SendMessage::new("first", "dm:test-kid"))
            .await
            .unwrap();
        ch.send(&SendMessage::new("to the group", GROUP))
            .await
            .unwrap();
        for refused in [
            "dm:nobody",
            KID_PHONE,
            &dm(KID_PHONE),
            OTHER_GROUP,
            "dm:",
            "test-kid",
        ] {
            let err = ch
                .send(&SendMessage::new("no", refused))
                .await
                .unwrap_err()
                .to_string();
            assert!(
                err.starts_with("BlueBubbles refused to send"),
                "{refused}: {err}"
            );
            assert!(!err.contains("555") && !err.contains('@'), "{err}");
        }
        // The kid's preferred handle, then the group; nothing else was sent.
        assert_eq!(sent_to(&s).await, vec![dm(KID_EMAIL), GROUP.to_string()]);

        // Once the kid writes from their phone, replies go back to that chat.
        let (tx, _rx) = mpsc::channel(10);
        let mut cur = cursor::Cursor {
            rowid: 0,
            recent: Vec::new(),
        };
        ch.poll_once(&tx, &mut cur, true).await.unwrap();
        ch.send(&SendMessage::new("reply", "dm:test-kid"))
            .await
            .unwrap();
        assert_eq!(sent_to(&s).await.last().unwrap(), &dm(KID_PHONE));
    }

    #[tokio::test]
    async fn a_chat_whose_handle_left_the_map_falls_back_to_the_preferred_handle() {
        let dir = tempfile::tempdir().unwrap();
        let (_, cmd) = switchable(dir.path());
        use_script(&cmd, &script(dir.path(), "good", GOOD, "", 0));
        let s = server(vec![row(1, &dm(KID_PHONE), KID_PHONE, "hi")]).await;
        let ch = channel(&s.uri(), cmd.to_str().unwrap());
        assert!(ch.people_ready().await);
        let (tx, _rx) = mpsc::channel(10);
        let mut cur = cursor::Cursor {
            rowid: 0,
            recent: Vec::new(),
        };
        ch.poll_once(&tx, &mut cur, true).await.unwrap();
        // The kid's phone is lost and comes off the map; their email stays.
        let without_phone = GOOD.replace(
            r#"["+15550100002", "kid@example.com"]"#,
            r#"["kid@example.com"]"#,
        );
        assert_ne!(without_phone, GOOD);
        use_script(&cmd, &script(dir.path(), "no-phone", &without_phone, "", 0));
        let people = ch.people.as_ref().unwrap();
        assert!(people.reload().await.changed);
        ch.send(&SendMessage::new("reply", "dm:test-kid"))
            .await
            .unwrap();
        assert_eq!(sent_to(&s).await, vec![dm(KID_EMAIL)]);
    }

    #[test]
    fn a_dm_token_is_a_direct_message() {
        let ch = channel("http://127.0.0.1:1", "/bin/false");
        let m = ChannelMessage::new("g", "test-kid", "dm:test-kid", "hi", "bluebubbles", 0);
        assert!(ch.is_direct_message(&m));
        let g = ChannelMessage::new("g", "test-kid", GROUP, "hi", "bluebubbles", 0);
        assert!(!ch.is_direct_message(&g));
    }
}
