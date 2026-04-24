//! Notification and share-link tests.

use gpt2api_rs::notifications::{is_valid_notification_email, render_image_done_email};

#[test]
fn notification_email_validation_is_syntax_only() {
    assert!(is_valid_notification_email("user@example.com"));
    assert!(is_valid_notification_email("user.name+tag@example.co"));
    assert!(!is_valid_notification_email(""));
    assert!(!is_valid_notification_email("missing-at.example.com"));
    assert!(!is_valid_notification_email("user@"));
}

#[test]
fn image_done_email_contains_prompt_and_signed_link_without_key_secret() {
    let rendered = render_image_done_email(
        "Lake session",
        "draw a lake",
        "gpt-image-1",
        1,
        "https://example.com/gpt2api/share/token-123",
    );
    assert!(rendered.subject.contains("Lake session"));
    assert!(rendered.text_body.contains("draw a lake"));
    assert!(rendered.text_body.contains("https://example.com/gpt2api/share/token-123"));
    assert!(!rendered.text_body.contains("sk-"));
}
