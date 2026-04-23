//! ChatGPT Web transport helpers for image generation and account refresh.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{FixedOffset, Utc};
use primp::{
    header::{HeaderMap, HeaderName, HeaderValue, COOKIE},
    Client, Impersonate, ImpersonateOS, Proxy,
};
use serde_json::{json, Value};
use sha3::{Digest, Sha3_512};
use uuid::Uuid;

use crate::{
    models::{AccountMetadata, AccountRecord, BrowserProfile},
    service::{ImageEditInput, ResolvedAccountProxy},
};

const DEFAULT_BASE_URL: &str = "https://chatgpt.com";
const DEFAULT_PROXY_URL: &str = "http://127.0.0.1:11118";
const DEFAULT_USER_AGENT: &str = concat!(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ",
    "AppleWebKit/537.36 (KHTML, like Gecko) ",
    "Chrome/131.0.0.0 Safari/537.36"
);
const CLIENT_BUILD_NUMBER: &str = "5955942";
const CLIENT_VERSION: &str = "prod-be885abbfcfe7b1f511e88b3003d9ee44757fbad";
const IMAGE_REQUIREMENT_FEATURE: &str = "image_gen";

static RANDOM_COUNTER: AtomicU64 = AtomicU64::new(1);

const NAV_KEYS: &[&str] = &[
    "webdriver-false",
    "vendor-Google Inc.",
    "cookieEnabled-true",
    "pdfViewerEnabled-true",
    "hardwareConcurrency-32",
    "language-zh-CN",
    "mimeTypes-[object MimeTypeArray]",
    "userAgentData-[object NavigatorUAData]",
];

const DOCUMENT_KEYS: &[&str] = &["_reactListening", "location"];

const WINDOW_KEYS: &[&str] = &[
    "window",
    "self",
    "document",
    "location",
    "history",
    "navigator",
    "screen",
    "innerWidth",
    "innerHeight",
    "devicePixelRatio",
    "chrome",
    "statusbar",
    "status",
];

/// One downloaded upstream image payload ready for downstream JSON shaping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedImageItem {
    /// Base64-encoded PNG/JPEG payload as returned to downstream callers.
    pub b64_json: String,
    /// Prompt text the upstream associated with the generated image.
    pub revised_prompt: String,
}

/// Successful upstream image generation result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatgptImageResult {
    /// Unix timestamp in seconds.
    pub created: i64,
    /// Downloaded image payloads.
    pub data: Vec<GeneratedImageItem>,
    /// Upstream model actually sent to ChatGPT Web.
    pub resolved_model: String,
}

/// Successful upstream text completion result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatgptTextResult {
    /// Unix timestamp in seconds.
    pub created: i64,
    /// Final assistant text.
    pub text: String,
    /// Upstream model actually sent to ChatGPT Web.
    pub resolved_model: String,
}

/// Streaming upstream text completion response.
#[derive(Debug)]
pub struct ChatgptTextStream {
    /// Unix timestamp in seconds.
    pub created: i64,
    /// Open ChatGPT SSE response body.
    pub response: primp::Response,
    /// Upstream model actually sent to ChatGPT Web.
    pub resolved_model: String,
}

/// Session-derived credential refresh result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RefreshedSessionCredentials {
    /// Fresh upstream bearer token returned by the session endpoint.
    pub access_token: String,
    /// Rotated session cookie if the upstream sent one back.
    pub session_token: Option<String>,
}

/// ChatGPT Web transport root configuration.
#[derive(Debug, Clone)]
pub struct ChatgptUpstreamClient {
    base_url: String,
    proxy_url: Option<String>,
}

#[derive(Debug, Clone)]
struct BootstrapState {
    device_id: String,
    scripts: Vec<String>,
    build_id: Option<String>,
}

#[derive(Clone, Copy)]
struct UpstreamRequestContext<'a> {
    client: &'a Client,
    profile: &'a BrowserProfile,
    access_token: &'a str,
    device_id: &'a str,
}

#[derive(Clone, Copy)]
struct UploadPayload<'a> {
    image_data: &'a [u8],
    file_name: &'a str,
    mime_type: &'a str,
}

#[derive(Clone, Copy)]
struct EditConversationPayload<'a> {
    prompt: &'a str,
    model: &'a str,
    file_id: &'a str,
    image_size: usize,
    image_width: u32,
    image_height: u32,
    file_name: &'a str,
    mime_type: &'a str,
}

impl Default for ChatgptUpstreamClient {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            proxy_url: Some(DEFAULT_PROXY_URL.to_string()),
        }
    }
}

impl ChatgptUpstreamClient {
    /// Builds a ChatGPT Web transport rooted at the provided base URL and proxy URL.
    #[must_use]
    pub fn new(base_url: impl Into<String>, proxy_url: Option<String>) -> Self {
        Self { base_url: base_url.into(), proxy_url }
    }

    /// Returns the service-wide default upstream proxy URL used by `inherit`.
    #[must_use]
    pub(crate) fn default_proxy_url(&self) -> Option<String> {
        self.proxy_url.clone()
    }

    /// Returns the configured upstream base URL.
    #[must_use]
    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Refreshes one account access token from the persisted ChatGPT session cookie.
    pub(crate) async fn refresh_session_access_token(
        &self,
        account: &AccountRecord,
        resolved_proxy: &ResolvedAccountProxy,
    ) -> Result<RefreshedSessionCredentials> {
        let profile = account.browser_profile();
        let session_token = profile
            .session_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("account does not have a session_token"))?;
        let client = self.build_client(&profile, resolved_proxy)?;
        let url = format!("{}/api/auth/session", self.base_url.trim_end_matches('/'));
        let response = self
            .apply_cookie(
                client
                    .get(&url)
                    .headers(self.base_headers(&profile, None))
                    .header("accept", "application/json"),
                &profile,
            )
            .send()
            .await
            .context("request /api/auth/session failed")?;
        let status = response.status();
        let rotated_session_token = extract_session_token_cookie(&response);
        let body = response.text().await.context("read /api/auth/session body failed")?;
        if !status.is_success() {
            bail!("/api/auth/session failed: HTTP {status}");
        }
        let value: Value = serde_json::from_str(&body).context("invalid /api/auth/session JSON")?;
        let access_token = value
            .get("accessToken")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("/api/auth/session missing accessToken"))?
            .to_string();
        let session_token = rotated_session_token
            .or_else(|| Some(session_token.to_string()))
            .filter(|value| !value.trim().is_empty());
        Ok(RefreshedSessionCredentials { access_token, session_token })
    }

    /// Fetches account metadata from `/backend-api/me` and `/backend-api/conversation/init`.
    pub(crate) async fn fetch_account_metadata(
        &self,
        account: &AccountRecord,
        resolved_proxy: &ResolvedAccountProxy,
    ) -> Result<AccountMetadata> {
        let profile = account.browser_profile();
        let client = self.build_client(&profile, resolved_proxy)?;
        let me_url = format!("{}/backend-api/me", self.base_url.trim_end_matches('/'));
        let init_url =
            format!("{}/backend-api/conversation/init", self.base_url.trim_end_matches('/'));

        let me = self
            .apply_cookie(
                client
                    .get(&me_url)
                    .bearer_auth(&account.access_token)
                    .headers(self.base_headers(&profile, None)),
                &profile,
            )
            .header("x-openai-target-path", "/backend-api/me")
            .header("x-openai-target-route", "/backend-api/me")
            .send()
            .await
            .context("request /backend-api/me failed")?;
        let me_status = me.status();
        let me_body = me.text().await.context("read /backend-api/me body failed")?;
        if !me_status.is_success() {
            bail!("/backend-api/me failed: HTTP {me_status}");
        }
        let me_json: Value =
            serde_json::from_str(&me_body).context("invalid /backend-api/me JSON")?;

        let init = self
            .apply_cookie(
                client
                    .post(&init_url)
                    .bearer_auth(&account.access_token)
                    .headers(self.base_headers(&profile, None))
                    .json(&json!({
                        "gizmo_id": Value::Null,
                        "requested_default_model": Value::Null,
                        "conversation_id": Value::Null,
                        "timezone_offset_min": -480,
                    })),
                &profile,
            )
            .send()
            .await
            .context("request /backend-api/conversation/init failed")?;
        let init_status = init.status();
        let init_body =
            init.text().await.context("read /backend-api/conversation/init body failed")?;
        if !init_status.is_success() {
            bail!("/backend-api/conversation/init failed: HTTP {init_status}");
        }
        let init_json: Value = serde_json::from_str(&init_body)
            .context("invalid /backend-api/conversation/init JSON")?;

        let image_limit = find_image_limit(&init_json);

        Ok(AccountMetadata {
            email: me_json.get("email").and_then(Value::as_str).map(ToString::to_string),
            user_id: me_json.get("id").and_then(Value::as_str).map(ToString::to_string),
            plan_type: extract_plan_type(&account.access_token, &me_json, &init_json),
            default_model_slug: init_json
                .get("default_model_slug")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            quota_remaining: image_limit
                .and_then(|value| value.get("remaining"))
                .and_then(Value::as_i64)
                .unwrap_or(0),
            quota_known: image_limit.is_some(),
            restore_at: image_limit
                .and_then(|value| value.get("reset_after"))
                .and_then(Value::as_str)
                .map(ToString::to_string),
        })
    }

    /// Executes one text-to-image request against ChatGPT Web.
    pub(crate) async fn generate_image(
        &self,
        account: &AccountRecord,
        resolved_proxy: &ResolvedAccountProxy,
        prompt: &str,
        requested_model: &str,
    ) -> Result<ChatgptImageResult> {
        self.generate_image_inner(account, resolved_proxy, prompt, requested_model, None).await
    }

    /// Executes one image-edit request against ChatGPT Web.
    pub(crate) async fn edit_image(
        &self,
        account: &AccountRecord,
        resolved_proxy: &ResolvedAccountProxy,
        prompt: &str,
        requested_model: &str,
        image: &ImageEditInput,
    ) -> Result<ChatgptImageResult> {
        self.generate_image_inner(
            account,
            resolved_proxy,
            prompt,
            requested_model,
            Some((image.image_data.as_slice(), image.file_name.as_str(), image.mime_type.as_str())),
        )
        .await
    }

    /// Executes one text completion request against ChatGPT Web and buffers the full SSE body.
    pub(crate) async fn complete_text(
        &self,
        account: &AccountRecord,
        resolved_proxy: &ResolvedAccountProxy,
        prompt: &str,
        requested_model: &str,
    ) -> Result<ChatgptTextResult> {
        let stream =
            self.start_text_stream(account, resolved_proxy, prompt, requested_model).await?;
        let body = stream.response.text().await.context("conversation body read failed")?;
        let parsed = parse_text_sse_payload(&body);
        let text = parsed.text.trim().to_string();
        if text.is_empty() {
            bail!("empty chat completion");
        }
        Ok(ChatgptTextResult {
            created: stream.created,
            text,
            resolved_model: stream.resolved_model,
        })
    }

    /// Opens one text completion stream against ChatGPT Web and returns the raw SSE response.
    pub(crate) async fn start_text_stream(
        &self,
        account: &AccountRecord,
        resolved_proxy: &ResolvedAccountProxy,
        prompt: &str,
        requested_model: &str,
    ) -> Result<ChatgptTextStream> {
        let prompt = prompt.trim();
        if prompt.is_empty() {
            bail!("prompt is required");
        }
        let profile = account.browser_profile();
        let client = self.build_client(&profile, resolved_proxy)?;
        let resolved_model = resolve_text_model(account, requested_model);
        let bootstrap = self.bootstrap(&client, &profile).await?;
        let request = UpstreamRequestContext {
            client: &client,
            profile: &profile,
            access_token: &account.access_token,
            device_id: &bootstrap.device_id,
        };
        let chat_requirements = self.chat_requirements(request, &bootstrap).await?;
        let proof_token = match chat_requirements
            .get("proofofwork")
            .and_then(Value::as_object)
            .filter(|value| value.get("required").and_then(Value::as_bool).unwrap_or(false))
        {
            Some(pow) => Some(generate_proof_token(
                pow.get("seed").and_then(Value::as_str).unwrap_or_default(),
                pow.get("difficulty").and_then(Value::as_str).unwrap_or_default(),
                &build_pow_config(
                    profile.user_agent.as_deref().unwrap_or(DEFAULT_USER_AGENT),
                    &bootstrap.scripts,
                    bootstrap.build_id.as_deref(),
                ),
            )),
            None => None,
        };
        let response = self
            .send_text_conversation_response(
                request,
                chat_requirements.get("token").and_then(Value::as_str).unwrap_or_default(),
                proof_token.as_deref(),
                prompt,
                &resolved_model,
            )
            .await?;
        Ok(ChatgptTextStream { created: unix_timestamp_secs(), response, resolved_model })
    }

    fn build_client(
        &self,
        profile: &BrowserProfile,
        resolved_proxy: &ResolvedAccountProxy,
    ) -> Result<Client> {
        let mut builder = Client::builder()
            .impersonate(resolve_impersonate(profile))
            .impersonate_os(ImpersonateOS::Windows)
            .cookie_store(true)
            .redirect(primp::redirect::Policy::none())
            .timeout(Duration::from_secs(180))
            .connect_timeout(Duration::from_secs(30))
            .user_agent(profile.user_agent.as_deref().unwrap_or(DEFAULT_USER_AGENT));
        if let Some(proxy_url) = render_proxy_url(
            resolved_proxy.proxy_url.as_deref(),
            resolved_proxy.proxy_username.as_deref(),
            resolved_proxy.proxy_password.as_deref(),
        )? {
            builder = builder.proxy(Proxy::all(proxy_url).context("invalid upstream proxy URL")?);
        }
        builder.build().context("build upstream client failed")
    }

    fn base_headers(&self, profile: &BrowserProfile, device_id: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("accept-language", HeaderValue::from_static("zh-CN,zh;q=0.9,en;q=0.8"));
        headers.insert(
            "origin",
            HeaderValue::from_str(self.base_url.trim_end_matches('/')).expect("origin header"),
        );
        headers.insert(
            "referer",
            HeaderValue::from_str(&format!("{}/", self.base_url.trim_end_matches('/')))
                .expect("referer header"),
        );
        headers.insert("accept", HeaderValue::from_static("*/*"));
        headers.insert("sec-fetch-dest", HeaderValue::from_static("empty"));
        headers.insert("sec-fetch-mode", HeaderValue::from_static("cors"));
        headers.insert("sec-fetch-site", HeaderValue::from_static("same-origin"));
        headers.insert(
            "sec-ch-ua",
            HeaderValue::from_str(profile.sec_ch_ua.as_deref().unwrap_or(
                "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            ))
            .expect("sec-ch-ua header"),
        );
        headers.insert(
            "sec-ch-ua-mobile",
            HeaderValue::from_str(profile.sec_ch_ua_mobile.as_deref().unwrap_or("?0"))
                .expect("sec-ch-ua-mobile header"),
        );
        headers.insert(
            "sec-ch-ua-platform",
            HeaderValue::from_str(profile.sec_ch_ua_platform.as_deref().unwrap_or("\"Windows\""))
                .expect("sec-ch-ua-platform header"),
        );
        if let Some(value) =
            device_id.or(profile.oai_device_id.as_deref()).filter(|value| !value.trim().is_empty())
        {
            headers.insert(
                "oai-device-id",
                HeaderValue::from_str(value).expect("oai-device-id header"),
            );
        }
        headers
    }

    fn apply_cookie(
        &self,
        builder: primp::RequestBuilder,
        profile: &BrowserProfile,
    ) -> primp::RequestBuilder {
        match profile.session_token.as_deref().filter(|value| !value.trim().is_empty()) {
            Some(session_token) => {
                builder.header(COOKIE, format!("__Secure-next-auth.session-token={session_token}"))
            }
            None => builder,
        }
    }

    async fn generate_image_inner(
        &self,
        account: &AccountRecord,
        resolved_proxy: &ResolvedAccountProxy,
        prompt: &str,
        requested_model: &str,
        edit_input: Option<(&[u8], &str, &str)>,
    ) -> Result<ChatgptImageResult> {
        let prompt = prompt.trim();
        if prompt.is_empty() {
            bail!("prompt is required");
        }
        let profile = account.browser_profile();
        let client = self.build_client(&profile, resolved_proxy)?;
        let resolved_model = resolve_upstream_model(account, requested_model);
        let bootstrap = self.bootstrap(&client, &profile).await?;
        let request = UpstreamRequestContext {
            client: &client,
            profile: &profile,
            access_token: &account.access_token,
            device_id: &bootstrap.device_id,
        };
        let chat_requirements = self.chat_requirements(request, &bootstrap).await?;
        let proof_token = match chat_requirements
            .get("proofofwork")
            .and_then(Value::as_object)
            .filter(|value| value.get("required").and_then(Value::as_bool).unwrap_or(false))
        {
            Some(pow) => Some(generate_proof_token(
                pow.get("seed").and_then(Value::as_str).unwrap_or_default(),
                pow.get("difficulty").and_then(Value::as_str).unwrap_or_default(),
                &build_pow_config(
                    profile.user_agent.as_deref().unwrap_or(DEFAULT_USER_AGENT),
                    &bootstrap.scripts,
                    bootstrap.build_id.as_deref(),
                ),
            )),
            None => None,
        };
        let conversation = match edit_input {
            Some((image_data, file_name, mime_type)) => {
                let file_id = self
                    .upload_image(request, UploadPayload { image_data, file_name, mime_type })
                    .await?;
                let (width, height) = detect_image_dimensions(image_data);
                self.send_edit_conversation(
                    request,
                    chat_requirements.get("token").and_then(Value::as_str).unwrap_or_default(),
                    proof_token.as_deref(),
                    EditConversationPayload {
                        prompt,
                        model: &resolved_model,
                        file_id: &file_id,
                        image_size: image_data.len(),
                        image_width: width,
                        image_height: height,
                        file_name,
                        mime_type,
                    },
                )
                .await?
            }
            None => {
                self.send_conversation(
                    request,
                    chat_requirements.get("token").and_then(Value::as_str).unwrap_or_default(),
                    proof_token.as_deref(),
                    prompt,
                    &resolved_model,
                )
                .await?
            }
        };

        let parsed = parse_sse_payload(&conversation);
        let mut file_ids = parsed.file_ids;
        if !parsed.conversation_id.is_empty() && file_ids.is_empty() {
            file_ids = self.poll_image_ids(request, &parsed.conversation_id).await?;
        }
        if file_ids.is_empty() {
            if !parsed.text.trim().is_empty() {
                bail!("{}", parsed.text.trim());
            }
            bail!("no image returned from upstream");
        }
        let first_file_id = file_ids.first().expect("checked non-empty file ids");
        let download_url =
            self.fetch_download_url(request, &parsed.conversation_id, first_file_id).await?;
        if download_url.is_empty() {
            bail!("failed to get download url");
        }
        let image_bytes = self.download_image(&client, &download_url).await?;
        Ok(ChatgptImageResult {
            created: unix_timestamp_secs(),
            data: vec![GeneratedImageItem {
                b64_json: BASE64.encode(image_bytes),
                revised_prompt: prompt.to_string(),
            }],
            resolved_model,
        })
    }

    async fn bootstrap(&self, client: &Client, profile: &BrowserProfile) -> Result<BootstrapState> {
        let url = format!("{}/", self.base_url.trim_end_matches('/'));
        let response = self
            .apply_cookie(client.get(&url).headers(self.base_headers(profile, None)), profile)
            .send()
            .await
            .context("bootstrap request failed")?;
        let body = response.text().await.context("bootstrap body read failed")?;
        let scripts = extract_script_sources(&body);
        let build_id = extract_data_build(&body);
        let device_id = profile
            .oai_device_id
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        Ok(BootstrapState { device_id, scripts, build_id })
    }

    async fn chat_requirements(
        &self,
        request: UpstreamRequestContext<'_>,
        bootstrap: &BootstrapState,
    ) -> Result<Value> {
        let url = format!(
            "{}/backend-api/sentinel/chat-requirements",
            self.base_url.trim_end_matches('/')
        );
        let config = build_pow_config(
            request.profile.user_agent.as_deref().unwrap_or(DEFAULT_USER_AGENT),
            &bootstrap.scripts,
            bootstrap.build_id.as_deref(),
        );
        let response = self
            .apply_cookie(
                request
                    .client
                    .post(&url)
                    .bearer_auth(request.access_token)
                    .headers(self.base_headers(request.profile, Some(request.device_id)))
                    .header("content-type", "application/json")
                    .json(&json!({ "p": generate_requirements_token(&config) })),
                request.profile,
            )
            .send()
            .await
            .context("chat-requirements request failed")?;
        let status = response.status();
        let body = response.text().await.context("chat-requirements body read failed")?;
        if !status.is_success() {
            bail!(
                "{}",
                truncate_message(
                    &body,
                    format!("chat-requirements failed: HTTP {status}").as_str()
                )
            );
        }
        serde_json::from_str(&body).context("invalid chat-requirements JSON")
    }

    async fn upload_image(
        &self,
        request: UpstreamRequestContext<'_>,
        payload: UploadPayload<'_>,
    ) -> Result<String> {
        let url = format!("{}/backend-api/files", self.base_url.trim_end_matches('/'));
        let response = self
            .apply_cookie(
                request
                    .client
                    .post(&url)
                    .bearer_auth(request.access_token)
                    .headers(self.base_headers(request.profile, Some(request.device_id)))
                    .header("content-type", "application/json")
                    .json(&json!({
                        "file_name": payload.file_name,
                        "file_size": payload.image_data.len(),
                        "use_case": "multimodal",
                        "timezone_offset_min": -480,
                        "reset_rate_limits": false,
                    })),
                request.profile,
            )
            .send()
            .await
            .context("file upload init request failed")?;
        let status = response.status();
        let body = response.text().await.context("file upload init body read failed")?;
        if !status.is_success() {
            bail!("file upload init failed: {status} {}", truncate_message(&body, ""));
        }
        let response_json: Value =
            serde_json::from_str(&body).context("invalid file upload init JSON")?;
        let upload_url =
            response_json.get("upload_url").and_then(Value::as_str).unwrap_or_default();
        let file_id = response_json.get("file_id").and_then(Value::as_str).unwrap_or_default();
        if upload_url.is_empty() || file_id.is_empty() {
            bail!("file upload init returned no upload_url or file_id");
        }

        let put_response = request
            .client
            .put(upload_url)
            .header("Content-Type", payload.mime_type)
            .header("x-ms-blob-type", "BlockBlob")
            .header("x-ms-version", "2020-04-08")
            .body(payload.image_data.to_vec())
            .send()
            .await
            .context("file upload PUT request failed")?;
        if !put_response.status().is_success() {
            bail!("file upload PUT failed: {}", put_response.status());
        }

        let process_url = format!(
            "{}/backend-api/files/process_upload_stream",
            self.base_url.trim_end_matches('/')
        );
        let process_response = self
            .apply_cookie(
                request
                    .client
                    .post(&process_url)
                    .bearer_auth(request.access_token)
                    .headers(self.base_headers(request.profile, Some(request.device_id)))
                    .header("content-type", "application/json")
                    .json(&json!({
                        "file_id": file_id,
                        "use_case": "multimodal",
                        "index_for_retrieval": false,
                        "file_name": payload.file_name,
                    })),
                request.profile,
            )
            .send()
            .await
            .context("process upload request failed")?;
        if !process_response.status().is_success() {
            bail!("file process failed: {}", process_response.status());
        }

        Ok(file_id.to_string())
    }

    async fn send_conversation(
        &self,
        request: UpstreamRequestContext<'_>,
        chat_token: &str,
        proof_token: Option<&str>,
        prompt: &str,
        model: &str,
    ) -> Result<String> {
        let url = format!("{}/backend-api/conversation", self.base_url.trim_end_matches('/'));
        let mut headers = self.base_headers(request.profile, Some(request.device_id));
        headers.insert("accept", HeaderValue::from_static("text/event-stream"));
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("oai-language", HeaderValue::from_static("zh-CN"));
        headers.insert(
            HeaderName::from_static("oai-client-build-number"),
            HeaderValue::from_static(CLIENT_BUILD_NUMBER),
        );
        headers.insert(
            HeaderName::from_static("oai-client-version"),
            HeaderValue::from_static(CLIENT_VERSION),
        );
        headers.insert(
            HeaderName::from_static("openai-sentinel-chat-requirements-token"),
            HeaderValue::from_str(chat_token).context("invalid chat requirements token header")?,
        );
        if let Some(proof_token) = proof_token.filter(|value| !value.is_empty()) {
            headers.insert(
                HeaderName::from_static("openai-sentinel-proof-token"),
                HeaderValue::from_str(proof_token).context("invalid proof token header")?,
            );
        }
        let response = self
            .apply_cookie(
                request.client.post(&url).bearer_auth(request.access_token).headers(headers).json(
                    &json!({
                        "action": "next",
                        "messages": [{
                            "id": Uuid::new_v4().to_string(),
                            "author": {"role": "user"},
                            "content": {"content_type": "text", "parts": [prompt]},
                            "metadata": {"attachments": []},
                        }],
                        "parent_message_id": Uuid::new_v4().to_string(),
                        "model": model,
                        "history_and_training_disabled": false,
                        "timezone_offset_min": -480,
                        "timezone": "America/Los_Angeles",
                        "conversation_mode": {"kind": "primary_assistant"},
                        "conversation_origin": Value::Null,
                        "force_paragen": false,
                        "force_paragen_model_slug": "",
                        "force_rate_limit": false,
                        "force_use_sse": true,
                        "paragen_cot_summary_display_override": "allow",
                        "paragen_stream_type_override": Value::Null,
                        "reset_rate_limits": false,
                        "suggestions": [],
                        "supported_encodings": [],
                        "system_hints": ["picture_v2"],
                        "variant_purpose": "comparison_implicit",
                        "websocket_request_id": Uuid::new_v4().to_string(),
                        "client_contextual_info": build_client_contextual_info(),
                    }),
                ),
                request.profile,
            )
            .send()
            .await
            .context("conversation request failed")?;
        let status = response.status();
        let body = response.text().await.context("conversation body read failed")?;
        if !status.is_success() {
            bail!(
                "{}",
                truncate_message(&body, format!("conversation failed: HTTP {status}").as_str())
            );
        }
        Ok(body)
    }

    async fn send_text_conversation_response(
        &self,
        request: UpstreamRequestContext<'_>,
        chat_token: &str,
        proof_token: Option<&str>,
        prompt: &str,
        model: &str,
    ) -> Result<primp::Response> {
        let url = format!("{}/backend-api/conversation", self.base_url.trim_end_matches('/'));
        let mut headers = self.base_headers(request.profile, Some(request.device_id));
        headers.insert("accept", HeaderValue::from_static("text/event-stream"));
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("oai-language", HeaderValue::from_static("zh-CN"));
        headers.insert(
            HeaderName::from_static("oai-client-build-number"),
            HeaderValue::from_static(CLIENT_BUILD_NUMBER),
        );
        headers.insert(
            HeaderName::from_static("oai-client-version"),
            HeaderValue::from_static(CLIENT_VERSION),
        );
        headers.insert(
            HeaderName::from_static("openai-sentinel-chat-requirements-token"),
            HeaderValue::from_str(chat_token).context("invalid chat requirements token header")?,
        );
        if let Some(proof_token) = proof_token.filter(|value| !value.is_empty()) {
            headers.insert(
                HeaderName::from_static("openai-sentinel-proof-token"),
                HeaderValue::from_str(proof_token).context("invalid proof token header")?,
            );
        }
        let response = self
            .apply_cookie(
                request.client.post(&url).bearer_auth(request.access_token).headers(headers).json(
                    &json!({
                        "action": "next",
                        "messages": [{
                            "id": Uuid::new_v4().to_string(),
                            "author": {"role": "user"},
                            "content": {"content_type": "text", "parts": [prompt]},
                            "metadata": {"attachments": []},
                        }],
                        "parent_message_id": Uuid::new_v4().to_string(),
                        "model": model,
                        "history_and_training_disabled": false,
                        "timezone_offset_min": -480,
                        "timezone": "America/Los_Angeles",
                        "conversation_mode": {"kind": "primary_assistant"},
                        "conversation_origin": Value::Null,
                        "force_paragen": false,
                        "force_paragen_model_slug": "",
                        "force_rate_limit": false,
                        "force_use_sse": true,
                        "paragen_cot_summary_display_override": "allow",
                        "paragen_stream_type_override": Value::Null,
                        "reset_rate_limits": false,
                        "suggestions": [],
                        "supported_encodings": [],
                        "system_hints": [],
                        "variant_purpose": "none",
                        "websocket_request_id": Uuid::new_v4().to_string(),
                        "client_contextual_info": build_client_contextual_info(),
                    }),
                ),
                request.profile,
            )
            .send()
            .await
            .context("text conversation request failed")?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let body = response.text().await.context("text conversation body read failed")?;
        bail!(
            "{}",
            truncate_message(&body, format!("conversation failed: HTTP {status}").as_str())
        );
    }

    async fn send_edit_conversation(
        &self,
        request: UpstreamRequestContext<'_>,
        chat_token: &str,
        proof_token: Option<&str>,
        payload: EditConversationPayload<'_>,
    ) -> Result<String> {
        let url = format!("{}/backend-api/conversation", self.base_url.trim_end_matches('/'));
        let mut headers = self.base_headers(request.profile, Some(request.device_id));
        headers.insert("accept", HeaderValue::from_static("text/event-stream"));
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("oai-language", HeaderValue::from_static("zh-CN"));
        headers.insert(
            HeaderName::from_static("oai-client-build-number"),
            HeaderValue::from_static(CLIENT_BUILD_NUMBER),
        );
        headers.insert(
            HeaderName::from_static("oai-client-version"),
            HeaderValue::from_static(CLIENT_VERSION),
        );
        headers.insert(
            HeaderName::from_static("openai-sentinel-chat-requirements-token"),
            HeaderValue::from_str(chat_token).context("invalid chat requirements token header")?,
        );
        if let Some(proof_token) = proof_token.filter(|value| !value.is_empty()) {
            headers.insert(
                HeaderName::from_static("openai-sentinel-proof-token"),
                HeaderValue::from_str(proof_token).context("invalid proof token header")?,
            );
        }
        let response = self
            .apply_cookie(
                request.client.post(&url).bearer_auth(request.access_token).headers(headers).json(
                    &json!({
                        "action": "next",
                        "messages": [{
                            "id": Uuid::new_v4().to_string(),
                            "author": {"role": "user"},
                            "content": {
                                "content_type": "multimodal_text",
                                "parts": [
                                    {
                                        "content_type": "image_asset_pointer",
                                        "asset_pointer": format!("sediment://{}", payload.file_id),
                                        "size_bytes": payload.image_size,
                                        "width": payload.image_width,
                                        "height": payload.image_height,
                                    },
                                    payload.prompt,
                                ],
                            },
                            "metadata": {
                                "attachments": [{
                                    "id": payload.file_id,
                                    "size": payload.image_size,
                                    "name": payload.file_name,
                                    "mime_type": payload.mime_type,
                                    "width": payload.image_width,
                                    "height": payload.image_height,
                                    "source": "local",
                                    "is_big_paste": false,
                                }],
                            },
                        }],
                        "parent_message_id": Uuid::new_v4().to_string(),
                        "model": payload.model,
                        "history_and_training_disabled": false,
                        "timezone_offset_min": -480,
                        "timezone": "America/Los_Angeles",
                        "conversation_mode": {"kind": "primary_assistant"},
                        "force_paragen": false,
                        "force_paragen_model_slug": "",
                        "force_rate_limit": false,
                        "force_use_sse": true,
                        "paragen_cot_summary_display_override": "allow",
                        "reset_rate_limits": false,
                        "suggestions": [],
                        "supported_encodings": [],
                        "system_hints": ["picture_v2"],
                        "variant_purpose": "comparison_implicit",
                        "websocket_request_id": Uuid::new_v4().to_string(),
                        "client_contextual_info": build_client_contextual_info(),
                    }),
                ),
                request.profile,
            )
            .send()
            .await
            .context("edit conversation request failed")?;
        let status = response.status();
        let body = response.text().await.context("edit conversation body read failed")?;
        if !status.is_success() {
            bail!(
                "{}",
                truncate_message(&body, format!("conversation failed: HTTP {status}").as_str())
            );
        }
        Ok(body)
    }

    async fn poll_image_ids(
        &self,
        request: UpstreamRequestContext<'_>,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        let url = format!(
            "{}/backend-api/conversation/{conversation_id}",
            self.base_url.trim_end_matches('/')
        );
        let deadline = Instant::now() + Duration::from_secs(45);
        while Instant::now() < deadline {
            let response = self
                .apply_cookie(
                    request
                        .client
                        .get(&url)
                        .bearer_auth(request.access_token)
                        .headers(self.base_headers(request.profile, Some(request.device_id))),
                    request.profile,
                )
                .send()
                .await
                .context("conversation poll request failed")?;
            if response.status().is_success() {
                let body = response.text().await.context("conversation poll body read failed")?;
                if let Ok(value) = serde_json::from_str::<Value>(&body) {
                    let file_ids = extract_image_ids(value.get("mapping").unwrap_or(&Value::Null));
                    if !file_ids.is_empty() {
                        return Ok(file_ids);
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        Ok(Vec::new())
    }

    async fn fetch_download_url(
        &self,
        request: UpstreamRequestContext<'_>,
        conversation_id: &str,
        file_id: &str,
    ) -> Result<String> {
        let (endpoint, raw_id) = match file_id.strip_prefix("sed:") {
            Some(raw) => (
                format!(
                    "{}/backend-api/conversation/{conversation_id}/attachment/{raw}/download",
                    self.base_url.trim_end_matches('/')
                ),
                raw.to_string(),
            ),
            None => (
                format!(
                    "{}/backend-api/files/{file_id}/download",
                    self.base_url.trim_end_matches('/')
                ),
                file_id.to_string(),
            ),
        };
        let response = self
            .apply_cookie(
                request
                    .client
                    .get(&endpoint)
                    .bearer_auth(request.access_token)
                    .headers(self.base_headers(request.profile, Some(request.device_id))),
                request.profile,
            )
            .send()
            .await
            .with_context(|| format!("download-url request failed for {raw_id}"))?;
        if !response.status().is_success() {
            return Ok(String::new());
        }
        let body = response.text().await.context("download-url body read failed")?;
        let value: Value = serde_json::from_str(&body).context("invalid download-url JSON")?;
        Ok(value.get("download_url").and_then(Value::as_str).unwrap_or_default().to_string())
    }

    async fn download_image(&self, client: &Client, download_url: &str) -> Result<Vec<u8>> {
        let response =
            client.get(download_url).send().await.context("image download request failed")?;
        if !response.status().is_success() {
            bail!("download image failed");
        }
        let bytes = response.bytes().await.context("download image body read failed")?;
        if bytes.is_empty() {
            bail!("download image failed");
        }
        Ok(bytes.to_vec())
    }
}

/// Returns whether the upstream error message indicates a revoked or invalid access token.
#[must_use]
pub fn is_token_invalid_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    text.contains("token_invalidated")
        || text.contains("token_revoked")
        || text.contains("authentication token has been invalidated")
        || text.contains("invalidated oauth token")
        || text.contains("/backend-api/me failed: http 401")
}

/// Returns the JWT expiry timestamp embedded in one ChatGPT access token.
#[must_use]
pub(crate) fn access_token_expires_at(access_token: &str) -> Option<i64> {
    decode_jwt_payload(access_token).and_then(|payload| {
        payload.get("exp").and_then(Value::as_i64).or_else(|| {
            payload.get("exp").and_then(Value::as_u64).and_then(|value| i64::try_from(value).ok())
        })
    })
}

/// Parsed highlights extracted from a ChatGPT conversation SSE stream.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ParsedConversationSse {
    /// Conversation id observed in the stream.
    pub conversation_id: String,
    /// File ids emitted by the image generation tool call.
    pub file_ids: Vec<String>,
    /// Aggregated assistant text parts observed in the stream.
    pub text: String,
}

fn render_proxy_url(
    proxy_url: Option<&str>,
    proxy_username: Option<&str>,
    proxy_password: Option<&str>,
) -> Result<Option<String>> {
    let Some(proxy_url) = proxy_url.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let mut url = reqwest::Url::parse(proxy_url).context("invalid upstream proxy URL")?;
    if let Some(username) = proxy_username.map(str::trim).filter(|value| !value.is_empty()) {
        url.set_username(username).map_err(|_| anyhow!("invalid proxy username"))?;
    }
    if let Some(password) = proxy_password.map(str::trim).filter(|value| !value.is_empty()) {
        url.set_password(Some(password)).map_err(|_| anyhow!("invalid proxy password"))?;
    }
    Ok(Some(url.to_string()))
}

fn resolve_impersonate(profile: &BrowserProfile) -> Impersonate {
    match profile
        .impersonate_browser
        .as_deref()
        .unwrap_or("edge")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "chrome" => Impersonate::Chrome,
        "firefox" => Impersonate::Firefox,
        "safari" => Impersonate::Safari,
        _ => Impersonate::Edge,
    }
}

fn find_image_limit(init: &Value) -> Option<&Value> {
    init.get("limits_progress").and_then(Value::as_array).and_then(|items| {
        items.iter().find(|item| {
            item.get("feature_name").and_then(Value::as_str) == Some(IMAGE_REQUIREMENT_FEATURE)
        })
    })
}

fn extract_session_token_cookie(response: &primp::Response) -> Option<String> {
    response
        .cookies()
        .find(|cookie| {
            matches!(cookie.name(), "__Secure-next-auth.session-token" | "next-auth.session-token")
        })
        .map(|cookie| cookie.value().to_string())
}

fn extract_plan_type(access_token: &str, me: &Value, init: &Value) -> Option<String> {
    let token_payload = decode_jwt_payload(access_token);
    for payload in [token_payload.as_ref(), Some(me), Some(init)].into_iter().flatten() {
        if let Some(plan) = find_plan_type(payload) {
            return Some(plan);
        }
    }
    None
}

fn decode_jwt_payload(access_token: &str) -> Option<Value> {
    let mut parts = access_token.split('.');
    let _header = parts.next()?;
    let payload = parts.next()?;
    let mut padded = payload.to_string();
    while padded.len() % 4 != 0 {
        padded.push('=');
    }
    let decoded = base64::engine::general_purpose::URL_SAFE.decode(padded.as_bytes()).ok()?;
    serde_json::from_slice(&decoded).ok()
}

fn find_plan_type(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => {
            if let Some(plan) = map
                .get("https://api.openai.com/auth")
                .and_then(Value::as_object)
                .and_then(|auth| auth.get("chatgpt_plan_type"))
                .and_then(Value::as_str)
                .and_then(normalize_plan_type)
            {
                return Some(plan);
            }
            for (key, item) in map {
                if key.to_ascii_lowercase().contains("plan")
                    || key.to_ascii_lowercase().contains("type")
                    || key.to_ascii_lowercase().contains("subscription")
                    || key.to_ascii_lowercase().contains("tier")
                {
                    if let Some(plan) = item.as_str().and_then(normalize_plan_type) {
                        return Some(plan);
                    }
                }
                if let Some(plan) = find_plan_type(item) {
                    return Some(plan);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(find_plan_type),
        Value::String(text) => normalize_plan_type(text),
        _ => None,
    }
}

fn normalize_plan_type(value: &str) -> Option<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "free" => Some("free".to_string()),
        "plus" | "personal" => Some("plus".to_string()),
        "team" | "business" | "enterprise" => Some("team".to_string()),
        "pro" => Some("pro".to_string()),
        _ => None,
    }
}

fn build_pow_config(user_agent: &str, scripts: &[String], build_id: Option<&str>) -> Vec<Value> {
    let eastern = FixedOffset::west_opt(5 * 3600).expect("fixed timezone");
    let now = Utc::now().with_timezone(&eastern);
    let request_id = Uuid::new_v4().to_string();
    vec![
        json!(3000),
        json!(now.format("%a %b %d %Y %H:%M:%S GMT-0500 (Eastern Standard Time)").to_string()),
        json!(4294705152_u64),
        json!(0),
        json!(user_agent),
        json!(pick_from(scripts, "https://chatgpt.com/backend-api/sentinel/sdk.js")),
        json!(build_id.unwrap_or_default()),
        json!("en-US"),
        json!("en-US,es-US,en,es"),
        json!(0),
        json!(pick_from_ref(NAV_KEYS)),
        json!(pick_from_ref(DOCUMENT_KEYS)),
        json!(pick_from_ref(WINDOW_KEYS)),
        json!(unix_timestamp_millis() as f64),
        json!(request_id),
        json!(""),
        json!(32),
        json!(unix_timestamp_millis() as f64),
    ]
}

fn generate_requirements_token(config: &[Value]) -> String {
    format!("gAAAAAC{}", generate_answer(&format!("0.{}", random_number()), "0fffff", config))
}

fn generate_proof_token(seed: &str, difficulty: &str, config: &[Value]) -> String {
    format!("gAAAAAB{}", generate_answer(seed, difficulty, config))
}

fn generate_answer(seed: &str, difficulty: &str, config: &[Value]) -> String {
    let part1 = {
        let json = serde_json::to_string(&config[..3]).expect("config serialization");
        format!("{},", json.trim_end_matches(']'))
    };
    let part2 = {
        let json = serde_json::to_string(&config[4..9]).expect("config serialization");
        format!(",{},", &json[1..json.len() - 1])
    };
    let part3 = {
        let json = serde_json::to_string(&config[10..]).expect("config serialization");
        format!(",{}", &json[1..])
    };
    let target = hex_decode(difficulty);
    for i in 0..500_000_u32 {
        let mut bytes = Vec::with_capacity(part1.len() + part2.len() + part3.len() + 32);
        bytes.extend_from_slice(part1.as_bytes());
        bytes.extend_from_slice(i.to_string().as_bytes());
        bytes.extend_from_slice(part2.as_bytes());
        bytes.extend_from_slice((i >> 1).to_string().as_bytes());
        bytes.extend_from_slice(part3.as_bytes());
        let encoded = BASE64.encode(bytes);
        let digest = Sha3_512::digest(
            seed.as_bytes().iter().chain(encoded.as_bytes()).copied().collect::<Vec<_>>(),
        );
        if digest[..target.len()] <= target[..] {
            return encoded;
        }
    }
    format!("wQ8Lk5FbGpA2NcR9dShT6gYjU7VxZ4D{}", BASE64.encode(format!("\"{seed}\"")))
}

fn hex_decode(raw: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(raw.len() / 2);
    let mut chars = raw.chars();
    while let (Some(left), Some(right)) = (chars.next(), chars.next()) {
        let value = left.to_digit(16).unwrap_or(0) * 16 + right.to_digit(16).unwrap_or(0);
        bytes.push(value as u8);
    }
    bytes
}

fn build_client_contextual_info() -> Value {
    let seed = RANDOM_COUNTER.fetch_add(1, Ordering::Relaxed);
    let page_height = 500 + (seed % 500) as i64;
    let page_width = 1000 + (seed % 1000) as i64;
    let screen_height = 800 + (seed % 400) as i64;
    let screen_width = 1200 + (seed % 1000) as i64;
    json!({
        "is_dark_mode": false,
        "time_since_loaded": 50 + (seed % 450) as i64,
        "page_height": page_height,
        "page_width": page_width,
        "pixel_ratio": 1.2,
        "screen_height": screen_height,
        "screen_width": screen_width,
    })
}

fn parse_sse_payload(raw: &str) -> ParsedConversationSse {
    let mut parsed = ParsedConversationSse::default();
    for line in raw.lines() {
        let line = line.trim();
        if !line.starts_with("data:") {
            continue;
        }
        let payload = line.trim_start_matches("data:").trim();
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        extract_inline_file_ids(payload, &mut parsed.file_ids);
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            continue;
        };
        if parsed.conversation_id.is_empty() {
            parsed.conversation_id = value
                .get("conversation_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
        }
        if let Some(data) = value.get("v").and_then(Value::as_object) {
            if parsed.conversation_id.is_empty() {
                parsed.conversation_id = data
                    .get("conversation_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
            }
        }
        if let Some(message) = value.get("message").and_then(Value::as_object) {
            if let Some(content) = message.get("content").and_then(Value::as_object) {
                if content.get("content_type").and_then(Value::as_str) == Some("text") {
                    if let Some(text) = content
                        .get("parts")
                        .and_then(Value::as_array)
                        .and_then(|parts| parts.first())
                        .and_then(Value::as_str)
                    {
                        parsed.text.push_str(text);
                    }
                }
            }
        }
    }
    parsed
}

fn parse_text_sse_payload(raw: &str) -> ParsedConversationSse {
    let mut parsed = ParsedConversationSse::default();
    for line in raw.lines() {
        let line = line.trim();
        if !line.starts_with("data:") {
            continue;
        }
        let payload = line.trim_start_matches("data:").trim();
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            continue;
        };
        if parsed.conversation_id.is_empty() {
            parsed.conversation_id =
                extract_conversation_id(&value).unwrap_or_default().to_string();
        }
        if let Some(text) = extract_assistant_text_snapshot(&value) {
            parsed.text = text;
        }
    }
    parsed
}

/// Parses the conversation SSE response and extracts file-service ids.
#[must_use]
pub fn parse_conversation_sse(raw: &str) -> ParsedConversationSse {
    parse_sse_payload(raw)
}

fn extract_inline_file_ids(payload: &str, file_ids: &mut Vec<String>) {
    for (prefix, stored_prefix) in [("file-service://", ""), ("sediment://", "sed:")] {
        let mut start = 0;
        while let Some(index) = payload[start..].find(prefix) {
            let begin = start + index + prefix.len();
            let tail = &payload[begin..];
            let file_id: String = tail
                .chars()
                .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
                .collect();
            if !file_id.is_empty() {
                let stored = format!("{stored_prefix}{file_id}");
                if !file_ids.contains(&stored) {
                    file_ids.push(stored);
                }
            }
            start = begin;
        }
    }
}

fn extract_conversation_id(value: &Value) -> Option<&str> {
    value.get("conversation_id").and_then(Value::as_str).or_else(|| {
        value.get("v").and_then(|inner| inner.get("conversation_id")).and_then(Value::as_str)
    })
}

fn extract_assistant_text_snapshot(value: &Value) -> Option<String> {
    for message in [value.get("message"), value.get("v").and_then(|inner| inner.get("message"))] {
        let Some(message) = message.and_then(Value::as_object) else {
            continue;
        };
        if message
            .get("author")
            .and_then(Value::as_object)
            .and_then(|author| author.get("role"))
            .and_then(Value::as_str)
            != Some("assistant")
        {
            continue;
        }
        let Some(content) = message.get("content").and_then(Value::as_object) else {
            continue;
        };
        match content.get("content_type").and_then(Value::as_str).unwrap_or_default() {
            "text" | "multimodal_text" => {
                let text = content
                    .get("parts")
                    .and_then(Value::as_array)
                    .map(|parts| {
                        parts
                            .iter()
                            .filter_map(|part| match part {
                                Value::String(text) => Some(text.trim().to_string()),
                                Value::Object(object) => object
                                    .get("text")
                                    .or_else(|| object.get("content"))
                                    .and_then(Value::as_str)
                                    .map(|text| text.trim().to_string()),
                                _ => None,
                            })
                            .filter(|text| !text.is_empty())
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();
                if !text.is_empty() {
                    return Some(text);
                }
            }
            _ => {}
        }
    }
    None
}

fn extract_image_ids(mapping: &Value) -> Vec<String> {
    let mut file_ids = Vec::new();
    let Some(entries) = mapping.as_object() else {
        return file_ids;
    };
    for node in entries.values() {
        let Some(message) = node.get("message").and_then(Value::as_object) else {
            continue;
        };
        if message
            .get("author")
            .and_then(Value::as_object)
            .and_then(|author| author.get("role"))
            .and_then(Value::as_str)
            != Some("tool")
        {
            continue;
        }
        if message
            .get("metadata")
            .and_then(Value::as_object)
            .and_then(|metadata| metadata.get("async_task_type"))
            .and_then(Value::as_str)
            != Some(IMAGE_REQUIREMENT_FEATURE)
        {
            continue;
        }
        if message
            .get("content")
            .and_then(Value::as_object)
            .and_then(|content| content.get("content_type"))
            .and_then(Value::as_str)
            != Some("multimodal_text")
        {
            continue;
        }
        if let Some(parts) = message
            .get("content")
            .and_then(Value::as_object)
            .and_then(|content| content.get("parts"))
            .and_then(Value::as_array)
        {
            for part in parts {
                let Some(pointer) = part.get("asset_pointer").and_then(Value::as_str) else {
                    continue;
                };
                let candidate = if let Some(raw) = pointer.strip_prefix("file-service://") {
                    raw.to_string()
                } else if let Some(raw) = pointer.strip_prefix("sediment://") {
                    format!("sed:{raw}")
                } else {
                    continue;
                };
                if !file_ids.contains(&candidate) {
                    file_ids.push(candidate);
                }
            }
        }
    }
    file_ids
}

fn detect_image_dimensions(image_data: &[u8]) -> (u32, u32) {
    if image_data.starts_with(b"\x89PNG\r\n\x1a\n") && image_data.len() >= 24 {
        let width =
            u32::from_be_bytes([image_data[16], image_data[17], image_data[18], image_data[19]]);
        let height =
            u32::from_be_bytes([image_data[20], image_data[21], image_data[22], image_data[23]]);
        return (width, height);
    }
    if image_data.starts_with(&[0xFF, 0xD8]) {
        let mut cursor = 2;
        while cursor + 9 < image_data.len() {
            if image_data[cursor] != 0xFF {
                break;
            }
            let marker = image_data[cursor + 1];
            if matches!(marker, 0xC0..=0xC2) {
                let height =
                    u16::from_be_bytes([image_data[cursor + 5], image_data[cursor + 6]]) as u32;
                let width =
                    u16::from_be_bytes([image_data[cursor + 7], image_data[cursor + 8]]) as u32;
                return (width, height);
            }
            if cursor + 4 >= image_data.len() {
                break;
            }
            let segment_len =
                u16::from_be_bytes([image_data[cursor + 2], image_data[cursor + 3]]) as usize;
            if segment_len < 2 {
                break;
            }
            cursor += 2 + segment_len;
        }
    }
    (1024, 1024)
}

fn resolve_upstream_model(account: &AccountRecord, requested_model: &str) -> String {
    let requested_model = requested_model.trim();
    if requested_model.is_empty() || requested_model == "gpt-image-1" {
        return "auto".to_string();
    }
    if requested_model == "gpt-image-2" {
        let free = account.plan_type.as_deref().unwrap_or("free").eq_ignore_ascii_case("free");
        return if free { "auto".to_string() } else { "gpt-5-3".to_string() };
    }
    requested_model.to_string()
}

fn resolve_text_model(account: &AccountRecord, requested_model: &str) -> String {
    let requested_model = requested_model.trim();
    if requested_model.is_empty() || requested_model.eq_ignore_ascii_case("auto") {
        return account
            .default_model_slug
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "auto".to_string());
    }
    requested_model.to_string()
}

fn truncate_message(body: &str, fallback: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return fallback.to_string();
    }
    trimmed.chars().take(400).collect()
}

fn extract_script_sources(body: &str) -> Vec<String> {
    let mut scripts = Vec::new();
    let mut search = body;
    while let Some(index) = search.find("src=\"") {
        let tail = &search[index + 5..];
        let Some(end) = tail.find('"') else {
            break;
        };
        let value = &tail[..end];
        if !value.trim().is_empty() {
            scripts.push(value.to_string());
        }
        search = &tail[end + 1..];
    }
    if scripts.is_empty() {
        scripts.push("https://chatgpt.com/backend-api/sentinel/sdk.js".to_string());
    }
    scripts
}

fn extract_data_build(body: &str) -> Option<String> {
    let marker = "data-build=\"";
    let index = body.find(marker)?;
    let tail = &body[index + marker.len()..];
    let end = tail.find('"')?;
    Some(tail[..end].to_string())
}

fn pick_from(items: &[String], fallback: &str) -> String {
    if items.is_empty() {
        return fallback.to_string();
    }
    items[(RANDOM_COUNTER.fetch_add(1, Ordering::Relaxed) as usize) % items.len()].clone()
}

fn pick_from_ref(items: &[&str]) -> String {
    items[(RANDOM_COUNTER.fetch_add(1, Ordering::Relaxed) as usize) % items.len()].to_string()
}

fn random_number() -> u64 {
    RANDOM_COUNTER.fetch_add(17, Ordering::Relaxed)
}

fn unix_timestamp_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after unix epoch")
        .as_secs() as i64
}

fn unix_timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after unix epoch")
        .as_millis()
}
