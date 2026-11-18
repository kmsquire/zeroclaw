//! The family's people map, held in memory only (kmsquire/llm-workspace#181).
//!
//! The map names each person, their role and their handles. It comes from
//! `people_command`'s stdout and is never written anywhere: not to config, the
//! workspace, a cache or a log. Nothing outside this module and the send path
//! ever sees a handle; everything the channel hands on carries the person ID.
//!
//! Validation matches kmsquire/llm-workspace's `scripts/store-family-handles.py`,
//! which writes the map. `family-handle-vectors.json` beside this file
//! is a byte-for-byte copy of that repo's shared vectors, and the tests run it.
//! No error text here names a handle: problems are worded by person and
//! position, because an ID that fails its check could itself be a handle.

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::LazyLock;
use std::time::Duration;

/// Largest map accepted from the command. The real one is well under 4 KiB.
pub(crate) const MAX_MAP_BYTES: usize = 256 * 1024;

static PERSON_ID: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-z][a-z0-9_-]{0,31}$").expect("static regex"));
static E164: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^\+[1-9][0-9]{7,14}$").expect("static regex"));
static EMAIL: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(
        r"^[a-z0-9](?:[a-z0-9._+-]{0,62}[a-z0-9])?@(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,24}$",
    )
    .expect("static regex")
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Role {
    Owner,
    Adult,
    Child,
}

impl Role {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "owner" => Some(Self::Owner),
            "adult" => Some(Self::Adult),
            "child" => Some(Self::Child),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Adult => "adult",
            Self::Child => "child",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Person {
    pub id: String,
    pub role: Role,
    pub handles: Vec<String>,
    /// The handle Early texts first; set exactly when `handles` isn't empty.
    pub preferred: Option<String>,
}

/// A validated map. `Debug` is deliberately not derived, so a stray `{:?}`
/// can't print a handle.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct People {
    persons: Vec<Person>,
    by_handle: HashMap<String, usize>,
    by_id: HashMap<String, usize>,
    /// The Vault version the map came from, when the command reports one.
    pub vault_version: Option<u64>,
}

impl fmt::Debug for People {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "People({})", self.summary())
    }
}

/// Counts only, for logs and the control socket.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct Summary {
    pub people: usize,
    pub handles: usize,
    pub roles: BTreeMap<&'static str, usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vault_version: Option<u64>,
}

impl fmt::Display for Summary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let roles: Vec<String> = self.roles.iter().map(|(r, n)| format!("{n} {r}")).collect();
        write!(
            f,
            "{} people ({}), {} handles",
            self.people,
            roles.join(", "),
            self.handles
        )?;
        if let Some(v) = self.vault_version {
            write!(f, ", Vault version {v}")?;
        }
        Ok(())
    }
}

impl People {
    /// Parse and validate the command's output. The error never names a
    /// handle.
    pub(crate) fn parse(raw: &[u8]) -> Result<Self, String> {
        let text = std::str::from_utf8(raw).map_err(|_| "the map isn't UTF-8".to_string())?;
        if text.trim().is_empty() {
            return Err("no map (the command printed nothing)".into());
        }
        let StrictValue(doc) = serde_json::from_str(text).map_err(|e| {
            if e.is_data() {
                // Only our own error reaches here: a repeated key.
                "the map has a key twice".to_string()
            } else {
                format!("the map isn't valid JSON (line {})", e.line())
            }
        })?;
        Self::from_value(&doc).map_err(|errs| errs.join("; "))
    }

    fn from_value(doc: &serde_json::Value) -> Result<Self, Vec<String>> {
        let Some(people) = doc
            .get("people")
            .and_then(serde_json::Value::as_object)
            .filter(|p| !p.is_empty())
        else {
            return Err(vec!["the map has no people".into()]);
        };
        let mut errs = Vec::new();
        let mut persons = Vec::new();
        let mut seen: HashMap<String, String> = HashMap::new();
        for (n, (pid, p)) in people.iter().enumerate() {
            let n = n + 1;
            let name = if PERSON_ID.is_match(pid) {
                pid.clone()
            } else {
                errs.push(format!(
                    "person {n}: id isn't lowercase letters, digits, - or _"
                ));
                format!("person {n}")
            };
            let Some(p) = p.as_object() else {
                errs.push(format!("{name}: must be an object with a role and handles"));
                continue;
            };
            let role = p.get("role").and_then(|r| r.as_str()).and_then(Role::parse);
            if role.is_none() {
                errs.push(format!("{name}: bad role"));
            }
            let raw_handles: &[serde_json::Value] = match p.get("handles") {
                Some(serde_json::Value::Array(hs)) => hs,
                _ => {
                    errs.push(format!("{name}: handles must be a list"));
                    &[]
                }
            };
            let mut handles = Vec::new();
            for (i, h) in raw_handles.iter().enumerate() {
                let i = i + 1;
                match h.as_str().filter(|h| is_handle(h)) {
                    None => errs.push(format!("{name}: handle {i} isn't normalized")),
                    Some(h) => {
                        if let Some(other) = seen.get(h) {
                            errs.push(format!("{name}: handle {i} also belongs to {other}"));
                        } else {
                            seen.insert(h.to_string(), name.clone());
                        }
                        handles.push(h.to_string());
                    }
                }
            }
            // `"preferred": null` is the same as no key, as in the store script.
            let preferred_value = p.get("preferred").filter(|v| !v.is_null());
            let preferred = match (raw_handles.is_empty(), preferred_value) {
                (false, None) => {
                    errs.push(format!("{name}: no preferred handle"));
                    None
                }
                (false, Some(pref)) => match pref.as_str() {
                    Some(pref) if raw_handles.iter().any(|h| h.as_str() == Some(pref)) => {
                        Some(pref.to_string())
                    }
                    _ => {
                        errs.push(format!(
                            "{name}: the preferred handle isn't one of their handles"
                        ));
                        None
                    }
                },
                (true, Some(_)) => {
                    errs.push(format!("{name}: a preferred handle but no handles"));
                    None
                }
                (true, None) => None,
            };
            if let Some(role) = role {
                persons.push(Person {
                    id: name,
                    role,
                    handles,
                    preferred,
                });
            }
        }
        let owners = people
            .values()
            .filter(|p| p.get("role").and_then(|r| r.as_str()) == Some("owner"))
            .count();
        if owners != 1 {
            errs.push(format!("there must be exactly one owner (found {owners})"));
        }
        if !errs.is_empty() {
            return Err(errs);
        }
        let mut by_handle = HashMap::new();
        let mut by_id = HashMap::new();
        for (idx, p) in persons.iter().enumerate() {
            by_id.insert(p.id.clone(), idx);
            for h in &p.handles {
                by_handle.insert(h.clone(), idx);
            }
        }
        Ok(Self {
            persons,
            by_handle,
            by_id,
            vault_version: doc.get("vault_version").and_then(serde_json::Value::as_u64),
        })
    }

    /// The person a handle belongs to, as BlueBubbles reports it: tried as
    /// given, then in the stored form ([`canonical_handle`]), so a number
    /// reported without its +1 still finds its person.
    pub(crate) fn by_handle(&self, handle: &str) -> Option<&Person> {
        self.by_handle
            .get(handle)
            .or_else(|| canonical_handle(handle).and_then(|c| self.by_handle.get(&c)))
            .map(|&i| &self.persons[i])
    }

    pub(crate) fn by_id(&self, id: &str) -> Option<&Person> {
        self.by_id.get(id).map(|&i| &self.persons[i])
    }

    pub(crate) fn handle_count(&self) -> usize {
        self.by_handle.len()
    }

    /// Same people, roles and handles, whatever the Vault version.
    pub(crate) fn same_content(&self, other: &Self) -> bool {
        self.persons == other.persons
    }

    pub(crate) fn summary(&self) -> Summary {
        let mut roles = BTreeMap::new();
        for p in &self.persons {
            *roles.entry(p.role.as_str()).or_insert(0) += 1;
        }
        Summary {
            people: self.persons.len(),
            handles: self.handle_count(),
            roles,
            vault_version: self.vault_version,
        }
    }
}

/// A handle in the form the map stores it, or `None` if it can't be one.
/// Mirrors kmsquire/llm-workspace's `store-family-handles.py`
/// `normalize_handle`; the shared vectors' `normalize` cases pin both.
///
/// Emails are lowercased. A phone number may carry spaces, tabs and `().-`;
/// with a leading `+` its digits are kept as they are. A bare 10-digit number
/// is taken as North American (+1), and 11 digits starting with 1 get a `+`.
/// That's true for this family, but a number from elsewhere written without
/// its country code is mapped wrong rather than refused. BlueBubbles 1.9.9
/// reports `+1` and 10 digits, and its `uncanonicalizedId` the bare 10.
pub(crate) fn canonical_handle(raw: &str) -> Option<String> {
    let t = raw.trim();
    if t.contains('@') {
        let email = t.to_lowercase();
        return is_handle(&email).then_some(email);
    }
    let (plus, rest) = match t.strip_prefix('+') {
        Some(rest) => (true, rest),
        None => (false, t),
    };
    if rest.is_empty()
        || !rest
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, ' ' | '\t' | '(' | ')' | '.' | '-'))
    {
        return None;
    }
    let digits: String = rest.chars().filter(char::is_ascii_digit).collect();
    let e164 = if plus {
        format!("+{digits}")
    } else if digits.len() == 10 {
        format!("+1{digits}")
    } else if digits.len() == 11 && digits.starts_with('1') {
        format!("+{digits}")
    } else {
        return None;
    };
    E164.is_match(&e164).then_some(e164)
}

fn is_handle(h: &str) -> bool {
    E164.is_match(h) || (EMAIL.is_match(h) && !h.contains(".."))
}

/// Run `people_command` and return its stdout. Stderr is discarded unread:
/// we don't control what the command prints there. The error names the exit
/// status or the failure, never the output.
pub(crate) async fn run_command(program: &str, timeout: Duration) -> Result<Vec<u8>, String> {
    use tokio::io::AsyncReadExt;
    // Its own process group, so a timeout kills what the command started (the
    // OCI CLI under a shell script) and not only the script. A small, fixed
    // environment rather than ZeroClaw's.
    let mut command = tokio::process::Command::new(program);
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .env_clear()
        .env(
            "PATH",
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        )
        .env("LANG", "C.UTF-8")
        .env("LC_ALL", "C.UTF-8")
        .kill_on_drop(true)
        .process_group(0);
    if let Some(home) = std::env::var_os("HOME") {
        command.env("HOME", home);
    }
    let mut child = command
        .spawn()
        .map_err(|e| format!("people_command could not start ({})", e.kind()))?;
    let pgid = child.id();
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| "people_command has no stdout".to_string())?;
    let run = async {
        let mut out = Vec::new();
        let limit = u64::try_from(MAX_MAP_BYTES + 1).unwrap_or(u64::MAX);
        let mut reader = (&mut stdout).take(limit);
        // Read until EOF or until the command exits, whichever is first. EOF
        // alone isn't enough: anything else holding the pipe's write end (a
        // background process the command left behind, or on macOS a process
        // forked elsewhere before close-on-exec was set) would hold it open.
        // Once the command has exited, what's already buffered is read and
        // the rest isn't waited for.
        let status = loop {
            tokio::select! {
                read = reader.read_buf(&mut out) => match read {
                    Ok(0) => break child.wait().await,
                    Ok(_) if out.len() > MAX_MAP_BYTES => break child.wait().await,
                    Ok(_) => {}
                    Err(e) => {
                        return Err(format!(
                            "reading people_command's output failed ({})",
                            e.kind()
                        ));
                    }
                },
                status = child.wait() => {
                    let drain = reader.read_to_end(&mut out);
                    let _ = tokio::time::timeout(Duration::from_millis(500), drain).await;
                    break status;
                }
            }
        }
        .map_err(|e| format!("people_command could not be waited on ({})", e.kind()))?;
        if out.len() > MAX_MAP_BYTES {
            return Err(format!(
                "people_command printed more than {MAX_MAP_BYTES} bytes"
            ));
        }
        if !status.success() {
            return Err(match status.code() {
                Some(code) => format!("people_command exited with status {code}"),
                None => "people_command was killed by a signal".to_string(),
            });
        }
        Ok(out)
    };
    let result = match tokio::time::timeout(timeout, run).await {
        Ok(result) => result,
        Err(_) => Err(format!("people_command took longer than {timeout:?}")),
    };
    if result.is_err() {
        kill_group(pgid);
    }
    result
}

/// Kill everything in the command's process group. The child leads its own
/// group (`process_group(0)`), so its pid is the group's id.
fn kill_group(pgid: Option<u32>) {
    if let Some(pgid) = pgid.and_then(|p| libc::pid_t::try_from(p).ok()) {
        // SAFETY: killpg takes a group id and a signal number; no memory is
        // shared. A group that has already exited gives ESRCH, which is fine.
        unsafe {
            libc::killpg(pgid, libc::SIGKILL);
        }
    }
}

/// A JSON value that refuses a repeated key in any object, where serde_json
/// would quietly keep the last one.
struct StrictValue(serde_json::Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(StrictVisitor)
    }
}

struct StrictVisitor;

impl<'de> Visitor<'de> for StrictVisitor {
    type Value = StrictValue;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<StrictValue, E> {
        Ok(StrictValue(v.into()))
    }

    fn visit_i64<E>(self, v: i64) -> Result<StrictValue, E> {
        Ok(StrictValue(v.into()))
    }

    fn visit_u64<E>(self, v: u64) -> Result<StrictValue, E> {
        Ok(StrictValue(v.into()))
    }

    fn visit_f64<E>(self, v: f64) -> Result<StrictValue, E> {
        Ok(StrictValue(
            serde_json::Number::from_f64(v).map_or(serde_json::Value::Null, Into::into),
        ))
    }

    fn visit_str<E>(self, v: &str) -> Result<StrictValue, E> {
        Ok(StrictValue(v.into()))
    }

    fn visit_string<E>(self, v: String) -> Result<StrictValue, E> {
        Ok(StrictValue(v.into()))
    }

    fn visit_unit<E>(self) -> Result<StrictValue, E> {
        Ok(StrictValue(serde_json::Value::Null))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<StrictValue, A::Error> {
        let mut out = Vec::new();
        while let Some(StrictValue(v)) = seq.next_element()? {
            out.push(v);
        }
        Ok(StrictValue(out.into()))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<StrictValue, A::Error> {
        let mut out = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            let StrictValue(v) = map.next_value()?;
            if out.contains_key(&key) {
                return Err(de::Error::custom("a key appears twice"));
            }
            out.insert(key, v);
        }
        Ok(StrictValue(out.into()))
    }
}
