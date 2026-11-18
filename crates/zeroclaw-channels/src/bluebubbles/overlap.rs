//! Startup check that no two aliases on the same BlueBubbles server admit the
//! same direct-message sender.
//!
//! Each alias polls the whole server and admits only its own peers, and an
//! alias is bound to one agent. If two aliases admit one sender, that sender's
//! direct messages reach two agents. No single alias can see that, so the
//! check runs over every alias at once.

use super::inbound::normalize_handle;

/// One alias as the check sees it.
pub(crate) struct AliasPeers<'a> {
    pub alias: &'a str,
    pub server_url: &'a str,
    pub peers: Vec<String>,
    pub group_chat_guids: &'a [String],
}

/// A conflict: two aliases on one server that would both answer the same
/// direct-message sender, or the same group chat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Overlap {
    pub first: String,
    pub second: String,
    /// The shared direct-message handle (`*` for a wildcard on both sides),
    /// or the shared group chat GUID.
    pub what: String,
}

fn admits(peers: &[String], handle: &str) -> bool {
    crate::allowlist::is_user_allowed_by(peers, handle, |peer, user| {
        normalize_handle(peer) == normalize_handle(user)
    })
}

fn grants(peers: &[String]) -> impl Iterator<Item = &str> {
    peers
        .iter()
        .map(|p| p.trim())
        .filter(|p| !p.is_empty() && !p.starts_with('!') && *p != "*")
}

fn has_wildcard(peers: &[String]) -> bool {
    peers.iter().any(|p| p.trim() == "*")
}

fn same_server(a: &str, b: &str) -> bool {
    a.trim()
        .trim_end_matches('/')
        .eq_ignore_ascii_case(b.trim().trim_end_matches('/'))
}

/// Every pair of aliases on the same server that would both admit a sender.
pub(crate) fn find_overlaps(aliases: &[AliasPeers<'_>]) -> Vec<Overlap> {
    let mut found = Vec::new();
    for (i, a) in aliases.iter().enumerate() {
        for b in &aliases[i + 1..] {
            if !same_server(a.server_url, b.server_url) {
                continue;
            }
            if has_wildcard(&a.peers) && has_wildcard(&b.peers) {
                found.push(Overlap {
                    first: a.alias.to_string(),
                    second: b.alias.to_string(),
                    what: "*".to_string(),
                });
            }
            for guid in a.group_chat_guids {
                if b.group_chat_guids.iter().any(|g| g == guid) {
                    found.push(Overlap {
                        first: a.alias.to_string(),
                        second: b.alias.to_string(),
                        what: guid.clone(),
                    });
                }
            }
            if has_wildcard(&a.peers) && has_wildcard(&b.peers) {
                continue;
            }
            let candidates = grants(&a.peers).chain(grants(&b.peers));
            let mut seen = std::collections::HashSet::new();
            for handle in candidates {
                let handle = normalize_handle(handle);
                if seen.insert(handle.clone())
                    && admits(&a.peers, &handle)
                    && admits(&b.peers, &handle)
                {
                    found.push(Overlap {
                        first: a.alias.to_string(),
                        second: b.alias.to_string(),
                        what: handle,
                    });
                }
            }
        }
    }
    found
}
