//! Keeping a private channel's message text out of the log
//! (kmsquire/llm-workspace#181).
//!
//! The orchestrator knows which channel a turn came from; the runtime, which
//! logs the model's reply, doesn't. So the orchestrator runs a private
//! channel's turn inside [`with_private_content`], and every record that would
//! carry message or reply text asks [`loggable_text`] for it instead. The flag
//! is task-local: it covers the turn's own task, not tasks it spawns.

use std::future::Future;

tokio::task_local! {
    static PRIVATE: bool;
}

/// Run `future` with message text kept out of the log when `private` is true.
pub async fn with_private_content<F: Future>(private: bool, future: F) -> F::Output {
    PRIVATE.scope(private, future).await
}

/// Whether the current task's message text is private.
pub fn content_is_private() -> bool {
    PRIVATE.try_with(|private| *private).unwrap_or(false)
}

/// `text` for a log record, or `null` inside a private channel's turn. Log the
/// length beside it if it helps; the length says nothing about the content.
pub fn loggable_text(text: &str) -> serde_json::Value {
    if content_is_private() {
        serde_json::Value::Null
    } else {
        serde_json::Value::String(text.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn text_is_null_only_inside_a_private_scope() {
        assert_eq!(loggable_text("hi Mom"), serde_json::json!("hi Mom"));
        let inside = with_private_content(true, async { loggable_text("hi Mom") }).await;
        assert_eq!(inside, serde_json::Value::Null);
        let public = with_private_content(false, async { loggable_text("hi Mom") }).await;
        assert_eq!(public, serde_json::json!("hi Mom"));
        assert!(!content_is_private(), "the flag ends with its scope");
    }
}
