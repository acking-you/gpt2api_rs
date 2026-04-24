//! Filesystem-backed generated image artifact storage.

use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::{models::ImageArtifactRecord, upstream::chatgpt::GeneratedImageItem};

/// Filesystem artifact store rooted at the service storage directory.
#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root: PathBuf,
    images_dir: PathBuf,
}

impl ArtifactStore {
    /// Creates a new artifact store.
    #[must_use]
    pub fn new(root: PathBuf, images_dir: PathBuf) -> Self {
        Self { root, images_dir }
    }

    /// Writes one generated image and returns its metadata.
    pub async fn write_generated_image(
        &self,
        task_id: &str,
        session_id: &str,
        message_id: &str,
        key_id: &str,
        item: &GeneratedImageItem,
        index: usize,
    ) -> Result<ImageArtifactRecord> {
        let bytes = BASE64.decode(item.b64_json.as_bytes()).context("invalid image base64")?;
        let image_id = format!("img_{}", Uuid::new_v4().simple());
        let file_name =
            if index == 0 { format!("{image_id}.png") } else { format!("{image_id}_{index}.png") };
        let relative_path = PathBuf::from("artifacts")
            .join("images")
            .join(key_id)
            .join(session_id)
            .join(message_id)
            .join(&file_name);
        let full_dir = self.images_dir.join(key_id).join(session_id).join(message_id);
        tokio::fs::create_dir_all(&full_dir).await?;
        let full_path = full_dir.join(file_name);
        let mut file = tokio::fs::File::create(&full_path).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;

        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let (width, height) = detect_png_or_jpeg_dimensions(&bytes);
        Ok(ImageArtifactRecord {
            id: image_id,
            task_id: task_id.to_string(),
            session_id: session_id.to_string(),
            message_id: message_id.to_string(),
            key_id: key_id.to_string(),
            relative_path: relative_path.to_string_lossy().replace('\\', "/"),
            mime_type: detect_mime_type(&bytes).to_string(),
            sha256: format!("{:x}", hasher.finalize()),
            size_bytes: bytes.len() as i64,
            width,
            height,
            revised_prompt: Some(item.revised_prompt.clone()),
            created_at: unix_timestamp_secs(),
        })
    }

    /// Reads an artifact by metadata relative path.
    pub async fn read_artifact(&self, artifact: &ImageArtifactRecord) -> Result<Vec<u8>> {
        let path = self.root.join(&artifact.relative_path);
        let canonical_root = self.root.canonicalize().context("canonical root")?;
        let canonical_path = path.canonicalize().context("canonical artifact path")?;
        ensure!(canonical_path.starts_with(canonical_root), "artifact path escaped root");
        Ok(tokio::fs::read(canonical_path).await?)
    }
}

fn detect_mime_type(image_data: &[u8]) -> &'static str {
    if image_data.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if image_data.starts_with(&[0xFF, 0xD8]) {
        "image/jpeg"
    } else {
        "application/octet-stream"
    }
}

fn detect_png_or_jpeg_dimensions(image_data: &[u8]) -> (Option<i64>, Option<i64>) {
    if image_data.starts_with(b"\x89PNG\r\n\x1a\n") && image_data.len() >= 24 {
        let width =
            u32::from_be_bytes([image_data[16], image_data[17], image_data[18], image_data[19]]);
        let height =
            u32::from_be_bytes([image_data[20], image_data[21], image_data[22], image_data[23]]);
        return (Some(i64::from(width)), Some(i64::from(height)));
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
                    u16::from_be_bytes([image_data[cursor + 5], image_data[cursor + 6]]) as i64;
                let width =
                    u16::from_be_bytes([image_data[cursor + 7], image_data[cursor + 8]]) as i64;
                return (Some(width), Some(height));
            }
            if cursor + 4 >= image_data.len() {
                break;
            }
            let length =
                u16::from_be_bytes([image_data[cursor + 2], image_data[cursor + 3]]) as usize;
            if length < 2 {
                break;
            }
            cursor += 2 + length;
        }
    }
    (None, None)
}

fn unix_timestamp_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock before unix epoch").as_secs()
        as i64
}
