//! Session-scoped immutable image objects. Only upload authorization mints references;
//! resolving a reference never reads another Session's object directory.
use super::{Manager, now, safe};
use crate::auth::Principal;
use async_trait::async_trait;
use microcrab_protocol::application::WireError;
use microcrab_session::{
    Error,
    admission::image::{AttachmentResolver, MAX_IMAGE_BYTES, ResolvedAttachment, validate_image},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

pub(super) struct SessionAttachments {
    root: PathBuf,
}
impl SessionAttachments {
    pub(super) fn new(data_dir: &Path, session_id: &str) -> Arc<Self> {
        Arc::new(Self {
            root: data_dir
                .join("sessions")
                .join(session_id)
                .join("attachments"),
        })
    }
    async fn save(&self, media_type: &str, data: Vec<u8>) -> Result<Value, Error> {
        let metadata = validate_image(media_type, &data)?;
        let extension =
            extension(media_type).ok_or_else(|| Error::config("unsupported image media type"))?;
        let digest = format!("{:x}", Sha256::digest(&data));
        let attachment_id = format!("sha256:{digest}.{extension}");
        tokio::fs::create_dir_all(&self.root).await?;
        let target = self.root.join(format!("{digest}.{extension}"));
        // A UUID staging file isolates concurrent identical uploads. Publication
        // uses a hard link so an existing immutable object is never overwritten.
        let staging = self.root.join(format!(".upload-{}", uuid::Uuid::new_v4()));
        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staging)
                .await?;
            file.write_all(&data).await?;
            file.sync_all().await?;
            drop(file);
            match tokio::fs::hard_link(&staging, &target).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    // Never trust a pre-existing file merely because its name matches.
                    self.resolve(&attachment_id, CancellationToken::new())
                        .await?;
                }
                Err(error) => return Err(Error::Io(error)),
            }
            #[cfg(unix)]
            tokio::fs::File::open(&self.root).await?.sync_all().await?;
            Ok::<(), Error>(())
        }
        .await;
        let cleanup = tokio::fs::remove_file(&staging).await;
        result?;
        cleanup?;
        Ok(
            json!({"attachment_id":attachment_id,"media_type":media_type,"bytes":data.len(),"width":metadata.width,"height":metadata.height}),
        )
    }
}

fn extension(media_type: &str) -> Option<&'static str> {
    match microcrab_session::admission::image::image_type(media_type)? {
        "image/png" => Some("png"),
        "image/jpeg" => Some("jpeg"),
        "image/gif" => Some("gif"),
        "image/webp" => Some("webp"),
        _ => None,
    }
}
fn identity(id: &str) -> Result<(&str, &'static str, &str), Error> {
    let invalid = || Error::config("invalid attachment identity");
    let leaf = id.strip_prefix("sha256:").ok_or_else(invalid)?;
    let (digest, extension) = leaf.split_once('.').ok_or_else(invalid)?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid());
    }
    let mime = microcrab_session::admission::image::image_type(match extension {
        "png" => "image/png",
        "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        _ => return Err(invalid()),
    })
    .ok_or_else(invalid)?;
    Ok((digest, mime, leaf))
}

#[async_trait]
impl AttachmentResolver for SessionAttachments {
    async fn resolve(
        &self,
        id: &str,
        cancellation: CancellationToken,
    ) -> Result<ResolvedAttachment, Error> {
        let (digest, media_type, leaf) = identity(id)?;
        let path = self.root.join(leaf);
        let read = async {
            let metadata = tokio::fs::symlink_metadata(&path).await?;
            if !metadata.file_type().is_file() || metadata.len() > MAX_IMAGE_BYTES as u64 {
                return Err(Error::config(
                    "attachment is not a bounded ordinary image file",
                ));
            }
            let file = tokio::fs::File::open(&path).await?;
            let mut data = Vec::new();
            file.take(MAX_IMAGE_BYTES as u64 + 1)
                .read_to_end(&mut data)
                .await?;
            if data.len() > MAX_IMAGE_BYTES || format!("{:x}", Sha256::digest(&data)) != digest {
                return Err(Error::config(
                    "attachment bytes do not match their identity",
                ));
            }
            validate_image(media_type, &data)?;
            Ok(ResolvedAttachment {
                media_type: media_type.into(),
                data,
            })
        };
        tokio::select! { biased; _ = cancellation.cancelled() => Err(Error::Cancelled), result = read => result }
    }
}

impl Manager {
    pub(crate) async fn read_attachment(
        &self,
        principal: &Principal,
        id: &str,
        attachment_id: &str,
    ) -> Result<ResolvedAttachment, WireError> {
        if principal.expires_at <= now() / 1000 {
            return Err(WireError::new("unauthenticated", "Authentication expired"));
        }
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "Command capacity reached"))?;
        let record = self.record(&principal.owner, id).await?;
        let attachments = SessionAttachments::new(&self.config.data_dir, &record.id);
        let attachment_id = attachment_id.to_owned();
        self.track(async move {
            let _permit = permit;
            attachments
                .resolve(&attachment_id, CancellationToken::new())
                .await
                .map_err(|_| WireError::new("unavailable", "Attachment unavailable"))
        })?
        .await
        .map_err(|_| WireError::new("operation_failed", "Attachment reader failed"))?
    }

    /// Hold normal Manager request capacity while the authenticated body is read.
    pub(crate) async fn authorize_attachment(
        &self,
        principal: &Principal,
        id: &str,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, WireError> {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(WireError::new("closed", "Manager unavailable"));
        }
        if principal.expires_at <= now() / 1000 {
            return Err(WireError::new("unauthenticated", "Authentication expired"));
        }
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "Command capacity reached"))?;
        let record = self.record(&principal.owner, id).await?;
        if record.archived {
            return Err(WireError::new("archived", "Session is archived"));
        }
        Ok(permit)
    }

    pub(crate) async fn upload_attachment(
        &self,
        principal: &Principal,
        id: &str,
        media_type: &str,
        data: Vec<u8>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Value, WireError> {
        if principal.expires_at <= now() / 1000 {
            return Err(WireError::new("unauthenticated", "Authentication expired"));
        }
        let record = self.record(&principal.owner, id).await?;
        if record.archived {
            return Err(WireError::new("archived", "Session is archived"));
        }
        // Authorization and capacity precede gate creation. Revalidate after waiting:
        // archive/delete may have retired this Session while the body was uploaded.
        let lifecycle = self.journal_open_gate(&record.id).lock_owned().await;
        let record = self.record(&principal.owner, id).await?;
        if record.archived {
            return Err(WireError::new("archived", "Session is archived"));
        }
        let attachments = SessionAttachments::new(&self.config.data_dir, &record.id);
        let mime = media_type.to_owned();
        let personal_library = self.personal_library.clone();
        let owner = principal.owner.clone();
        let session_id = record.id.clone();
        let limits = self.config.personal_library.limits();
        // Retain the effect owner across a disconnected HTTP waiter. Uploaded but
        // unsubmitted objects are inert; no Context or historical node is changed.
        let saved = self.track(async move {
            let _permit = permit;
            let _lifecycle = lifecycle;
            let mut value = attachments.save(&mime, data.clone()).await.map_err(safe)?;
            let short_id = value["attachment_id"]
                .as_str()
                .and_then(|s| s.strip_prefix("sha256:"))
                .map(|s| &s[..s.len().min(12)])
                .unwrap_or("image");
            let created = personal_library
                .create(
                    &owner,
                    limits,
                    microcrab_session::state::reference::CreateLibraryObject::LocalBlob {
                        id: None,
                        title: format!("attachment:{short_id}"),
                        media_type: mime,
                        data,
                        origin: microcrab_session::state::reference::LocalBlobOrigin::Upload,
                        origin_execution_id: None,
                    },
                    Some(&session_id),
                    None,
                )
                .await
                .map_err(super::library::library_wire_error)?;
            if let Some(obj) = value.as_object_mut() {
                obj.insert("reference_id".into(), json!(created.summary.id));
            }
            Ok::<Value, WireError>(value)
        })?
        .await
        .map_err(|_| WireError::new("operation_failed", "Attachment owner failed"))??;
        if let Some(managed) = super::settings::active(self, &record.id).await
            && managed.commands.try_lock().is_ok()
        {
            let _ = managed.session.refresh_reference_library().await;
        }
        Ok(saved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn png() -> Vec<u8> {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD
            .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j7S8AAAAASUVORK5CYII=")
            .unwrap()
    }
    #[tokio::test]
    async fn immutable_objects_are_scoped_verified_and_deduplicated() {
        let dir = tempfile::tempdir().unwrap();
        let first = SessionAttachments::new(dir.path(), "first");
        let second = SessionAttachments::new(dir.path(), "second");
        let saved = first.save("image/png", png()).await.unwrap();
        let id = saved["attachment_id"].as_str().unwrap();
        assert_eq!(saved, first.save("image/png", png()).await.unwrap());
        assert_eq!(
            first
                .resolve(id, CancellationToken::new())
                .await
                .unwrap()
                .data,
            png()
        );
        assert!(second.resolve(id, CancellationToken::new()).await.is_err());
        assert!(
            first
                .resolve("../first", CancellationToken::new())
                .await
                .is_err()
        );
        let (_, _, leaf) = identity(id).unwrap();
        tokio::fs::write(first.root.join(leaf), b"tampered")
            .await
            .unwrap();
        assert!(first.resolve(id, CancellationToken::new()).await.is_err());
    }
    #[tokio::test]
    async fn cancellation_does_not_read_a_reference() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionAttachments::new(dir.path(), "s");
        let token = CancellationToken::new();
        token.cancel();
        let id = format!("sha256:{}.png", "0".repeat(64));
        assert!(matches!(
            store.resolve(&id, token).await,
            Err(Error::Cancelled)
        ));
    }
}
