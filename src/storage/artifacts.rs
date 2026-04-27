//! Filesystem-backed generated image artifact storage.

use std::{
    io::ErrorKind,
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use image::{codecs::jpeg::JpegEncoder, imageops::FilterType};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::{models::ImageArtifactRecord, upstream::chatgpt::GeneratedImageItem};

const THUMBNAIL_MAX_EDGE: u32 = 512;
const THUMBNAIL_JPEG_QUALITY: u8 = 82;

/// Filesystem artifact store rooted at the service storage directory.
#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root: PathBuf,
    images_dir: PathBuf,
    thumbnails_dir: PathBuf,
}

impl ArtifactStore {
    /// Creates a new artifact store.
    #[must_use]
    pub fn new(root: PathBuf, images_dir: PathBuf) -> Self {
        let thumbnails_dir = root.join("artifacts").join("thumbnails");
        Self { root, images_dir, thumbnails_dir }
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

    /// Reads an existing JPEG thumbnail or creates and caches one from the original artifact.
    pub async fn read_or_create_thumbnail(
        &self,
        artifact: &ImageArtifactRecord,
    ) -> Result<Vec<u8>> {
        let relative_path = thumbnail_relative_path(artifact);
        let full_path = self.root.join(relative_path);
        match tokio::fs::read(&full_path).await {
            Ok(bytes) if !bytes.is_empty() => return Ok(bytes),
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("read image thumbnail"),
        }

        let original = self.read_artifact(artifact).await?;
        let bytes = tokio::task::spawn_blocking(move || create_jpeg_thumbnail(&original)).await??;
        let parent = full_path.parent().context("thumbnail path has no parent")?;
        tokio::fs::create_dir_all(parent).await?;
        let temp_path = parent.join(format!(".thumb-{}.tmp", Uuid::new_v4().simple()));
        let mut file = tokio::fs::File::create(&temp_path).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        drop(file);
        tokio::fs::rename(&temp_path, &full_path).await?;
        Ok(bytes)
    }

    /// Deletes artifact files and then prunes empty artifact directories.
    pub async fn delete_artifacts(&self, artifacts: &[ImageArtifactRecord]) -> Result<()> {
        if artifacts.is_empty() {
            return Ok(());
        }
        let root = self.root.clone();
        let images_dir = self.images_dir.clone();
        let thumbnails_dir = self.thumbnails_dir.clone();
        let artifacts = artifacts.to_vec();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let canonical_root = root.canonicalize().context("canonical artifact root")?;
            let canonical_images_dir =
                images_dir.canonicalize().context("canonical image artifact directory")?;
            let canonical_thumbnails_dir =
                thumbnails_dir.canonicalize().context("canonical thumbnail artifact directory")?;
            ensure!(
                canonical_images_dir.starts_with(&canonical_root),
                "image artifact directory escaped root"
            );
            ensure!(
                canonical_thumbnails_dir.starts_with(&canonical_root),
                "thumbnail artifact directory escaped root"
            );
            for artifact in &artifacts {
                let relative_path = safe_relative_artifact_path(&artifact.relative_path)?;
                let full_path = root.join(relative_path);
                delete_stored_file(&canonical_root, &canonical_images_dir, &full_path)
                    .with_context(|| format!("delete artifact {}", artifact.relative_path))?;
                let thumbnail_path = root.join(thumbnail_relative_path(artifact));
                delete_stored_file(&canonical_root, &canonical_thumbnails_dir, &thumbnail_path)
                    .with_context(|| format!("delete artifact thumbnail {}", artifact.id))?;
            }
            Ok(())
        })
        .await??;
        Ok(())
    }
}

fn thumbnail_relative_path(artifact: &ImageArtifactRecord) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(artifact.id.as_bytes());
    hasher.update(b"\0");
    hasher.update(artifact.relative_path.as_bytes());
    hasher.update(b"\0");
    hasher.update(artifact.sha256.as_bytes());
    let file_name = format!("{:x}.jpg", hasher.finalize());
    PathBuf::from("artifacts")
        .join("thumbnails")
        .join(safe_path_component(&artifact.key_id))
        .join(safe_path_component(&artifact.session_id))
        .join(safe_path_component(&artifact.message_id))
        .join(file_name)
}

fn safe_path_component(value: &str) -> String {
    let component = value
        .chars()
        .take(96)
        .map(|ch| if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') { ch } else { '_' })
        .collect::<String>();
    if component.is_empty() {
        "item".to_string()
    } else {
        component
    }
}

fn safe_relative_artifact_path(value: &str) -> Result<PathBuf> {
    let path = Path::new(value);
    ensure!(!path.is_absolute(), "artifact path must be relative");
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("artifact path escaped root")
            }
        }
    }
    ensure!(!normalized.as_os_str().is_empty(), "artifact path is empty");
    Ok(normalized)
}

fn delete_stored_file(root: &Path, base_dir: &Path, full_path: &Path) -> Result<()> {
    let parent = full_path.parent().context("artifact path has no parent")?;
    let canonical_parent = match parent.canonicalize() {
        Ok(path) => path,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("canonical artifact parent"),
    };
    ensure!(canonical_parent.starts_with(root), "artifact parent escaped root");
    ensure!(canonical_parent.starts_with(base_dir), "artifact parent escaped storage directory");
    match std::fs::remove_file(full_path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("remove artifact file"),
    }
    prune_empty_dirs(parent, base_dir)
}

fn create_jpeg_thumbnail(image_data: &[u8]) -> Result<Vec<u8>> {
    let source = image::load_from_memory(image_data).context("decode image thumbnail source")?;
    let thumbnail = if source.width() > THUMBNAIL_MAX_EDGE || source.height() > THUMBNAIL_MAX_EDGE {
        source.resize(THUMBNAIL_MAX_EDGE, THUMBNAIL_MAX_EDGE, FilterType::Triangle)
    } else {
        source
    };
    let rgba = thumbnail.to_rgba8();
    let (width, height) = rgba.dimensions();
    let mut rgb = image::RgbImage::new(width, height);
    for (x, y, pixel) in rgba.enumerate_pixels() {
        let alpha = u16::from(pixel[3]);
        let inverse_alpha = 255_u16 - alpha;
        let red = blend_over_white(pixel[0], alpha, inverse_alpha);
        let green = blend_over_white(pixel[1], alpha, inverse_alpha);
        let blue = blend_over_white(pixel[2], alpha, inverse_alpha);
        rgb.put_pixel(x, y, image::Rgb([red, green, blue]));
    }
    let mut output = Vec::new();
    let mut encoder = JpegEncoder::new_with_quality(&mut output, THUMBNAIL_JPEG_QUALITY);
    encoder
        .encode(rgb.as_raw(), width, height, image::ColorType::Rgb8.into())
        .context("encode image thumbnail")?;
    Ok(output)
}

fn blend_over_white(channel: u8, alpha: u16, inverse_alpha: u16) -> u8 {
    ((u16::from(channel) * alpha + 255_u16 * inverse_alpha + 127_u16) / 255_u16) as u8
}

fn prune_empty_dirs(start: &Path, stop: &Path) -> Result<()> {
    let mut current = start.to_path_buf();
    loop {
        let canonical = match current.canonicalize() {
            Ok(path) => path,
            Err(error) if error.kind() == ErrorKind::NotFound => break,
            Err(error) => return Err(error).context("canonical artifact cleanup directory"),
        };
        if canonical == stop || !canonical.starts_with(stop) {
            break;
        }
        match std::fs::remove_dir(&current) {
            Ok(()) => {}
            Err(error)
                if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::DirectoryNotEmpty) =>
            {
                break;
            }
            Err(error) => return Err(error).context("remove empty artifact directory"),
        }
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent.to_path_buf();
    }
    Ok(())
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
