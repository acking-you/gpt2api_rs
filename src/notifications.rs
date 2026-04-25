//! Optional email notifications for completed image tasks.

use anyhow::{Context, Result};
use lettre::{
    message::Mailbox, transport::smtp::authentication::Credentials, AsyncSmtpTransport,
    AsyncTransport, Message, Tokio1Executor,
};

use crate::config::SmtpConfig;

/// Rendered email content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedEmail {
    /// Email subject.
    pub subject: String,
    /// Plain text body.
    pub text_body: String,
}

/// Email delivery outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationOutcome {
    /// SMTP configuration or key settings disabled delivery.
    Skipped,
    /// Message was accepted by the SMTP transport.
    Sent,
}

/// Performs syntax-only email validation.
#[must_use]
pub fn is_valid_notification_email(email: &str) -> bool {
    let value = email.trim();
    let Some((local, domain)) = value.split_once('@') else {
        return false;
    };
    !local.is_empty() && domain.contains('.') && !domain.starts_with('.') && !domain.ends_with('.')
}

/// Renders the image completion email.
#[must_use]
pub fn render_image_done_email(
    session_title: &str,
    prompt: &str,
    model: &str,
    image_count: usize,
    signed_link: &str,
) -> RenderedEmail {
    RenderedEmail {
        subject: format!("GPT2API image ready: {session_title}"),
        text_body: format!(
            "Your image generation is complete.\n\nSession: {session_title}\nModel: {model}\nImages: {image_count}\nPrompt:\n{prompt}\n\nView result:\n{signed_link}\n"
        ),
    }
}

/// Sends a rendered email when SMTP config is complete.
pub async fn send_rendered_email(
    config: &SmtpConfig,
    recipient: &str,
    rendered: &RenderedEmail,
) -> Result<NotificationOutcome> {
    if !config.is_complete() || !is_valid_notification_email(recipient) {
        return Ok(NotificationOutcome::Skipped);
    }
    let host = config.host.as_deref().expect("checked complete");
    let username = config.username.as_deref().expect("checked complete");
    let password = config.password.as_deref().expect("checked complete");
    let from = config.from.as_deref().expect("checked complete");
    let email = Message::builder()
        .from(from.parse::<Mailbox>().context("invalid SMTP from address")?)
        .to(recipient.parse::<Mailbox>().context("invalid notification recipient")?)
        .subject(&rendered.subject)
        .body(rendered.text_body.clone())
        .context("build notification email")?;
    let builder = if config.port == 465 {
        AsyncSmtpTransport::<Tokio1Executor>::relay(host).context("build SMTP relay")?
    } else {
        AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)
            .context("build SMTP STARTTLS relay")?
    };
    let mailer = builder
        .port(config.port)
        .credentials(Credentials::new(username.to_string(), password.to_string()))
        .build();
    mailer.send(email).await.context("send notification email")?;
    Ok(NotificationOutcome::Sent)
}
