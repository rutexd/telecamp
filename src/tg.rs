use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Context, Result};
use grammers_client::client::{UpdateStream, UpdatesConfiguration};
use grammers_client::media::{InputMedia as GInputMedia, Uploaded};
use grammers_client::message::{InputMessage, Message as GMessage};
use grammers_client::peer::Peer as GPeer;
use grammers_client::{Client, SenderPool};
use grammers_session::types::PeerRef;
use grammers_session::updates::UpdatesLike;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::session::FileSession;

/// A resolved Telegram peer (user / group / channel).
pub type Peer = GPeer;

/// Wrapper around the grammers 0.10 `Client` for Telegram operations.
#[derive(Clone)]
pub struct TgClient {
    client: Client,
    updates: Arc<Mutex<Option<UnboundedReceiver<UpdatesLike>>>>,
    _pool_task: Arc<JoinHandle<()>>,
}

impl TgClient {
    /// Connects to Telegram (MTProto), signs in as a bot if needed,
    /// and returns a ready client with a persisted session.
    pub async fn connect(config: &Config) -> Result<Self> {
        log::info!("connecting to Telegram via MTProto");

        let session = Arc::new(FileSession::open(&config.telegram_session_file)?);
        log::info!(
            "session storage at {}",
            config.telegram_session_file.display()
        );

        let pool = SenderPool::new(Arc::clone(&session), config.telegram_api_id);
        let client = Client::new(pool.handle.clone());
        let pool_task = tokio::spawn(pool.runner.run());

        if !client.is_authorized().await? {
            log::info!("signing in as bot");
            client
                .bot_sign_in(&config.telegram_bot_token, &config.telegram_api_hash)
                .await
                .context("bot sign-in failed")?;
            log::info!("bot signed in successfully");
            session.save().context("failed to save session")?;
        } else {
            log::info!("already authorized");
        }

        Ok(Self {
            client,
            updates: Arc::new(Mutex::new(Some(pool.updates))),
            _pool_task: Arc::new(pool_task),
        })
    }

    /// Resolves the target chat/channel from config (TELEGRAM_CHANNEL or TELEGRAM_CHAT_ID).
    pub async fn resolve_chat(&self, config: &Config) -> Result<Option<Peer>> {
        if let Some(channel) = &config.telegram_channel {
            let username = channel.trim_start_matches('@').to_string();
            log::info!("resolving channel @{username}");

            let chat = self
                .client
                .resolve_username(&username)
                .await
                .context("failed to resolve channel username")?
                .context("channel username not found")?;

            log::info!("resolved @{username} to peer {:?}", chat.id());
            return Ok(Some(chat));
        }

        if config.telegram_chat_id.is_some() {
            bail!("Numeric TELEGRAM_CHAT_ID is not yet supported with grammers. Please use TELEGRAM_CHANNEL with a @username instead.");
        }

        Ok(None)
    }

    /// Consumes the update receiver and returns the processed update stream
    /// (used by the interactive bot). Can only be called once.
    pub async fn updates(&self) -> Result<UpdateStream> {
        let receiver = self
            .updates
            .lock()
            .unwrap()
            .take()
            .context("update stream already consumed")?;

        let stream = self
            .client
            .stream_updates(
                receiver,
                UpdatesConfiguration {
                    catch_up: false,
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| anyhow!("failed to build update stream: {err}"))?;

        Ok(stream)
    }

    /// Sends a text message to a peer.
    pub async fn send_message(&self, peer: &Peer, text: impl Into<String>) -> Result<Message> {
        let peer_ref = to_peer_ref(peer).await?;
        let msg = self
            .client
            .send_message(peer_ref, text.into())
            .await
            .context("failed to send message")?;
        Ok(Message { inner: msg })
    }

    /// Sends a photo with caption to a peer.
    pub async fn send_photo(
        &self,
        peer: &Peer,
        photo_bytes: Vec<u8>,
        caption: impl Into<String>,
    ) -> Result<Message> {
        let peer_ref = to_peer_ref(peer).await?;

        let len = photo_bytes.len();
        let uploaded = self
            .client
            .upload_stream(
                &mut std::io::Cursor::new(photo_bytes),
                len,
                "cover.jpg".to_string(),
            )
            .await
            .context("failed to upload photo")?;

        let msg = self
            .client
            .send_message(peer_ref, InputMessage::new().text(caption).photo(uploaded))
            .await
            .context("failed to send photo")?;

        Ok(Message { inner: msg })
    }

    /// Sends a media album (photos/documents) to a peer.
    pub async fn send_album(&self, peer: &Peer, media: Vec<InputMedia>) -> Result<Vec<Message>> {
        let peer_ref = to_peer_ref(peer).await?;

        let grammers_media: Vec<GInputMedia> = media.into_iter().map(|m| m.inner).collect();
        let messages = self
            .client
            .send_album(peer_ref, grammers_media)
            .await
            .context("failed to send album")?;

        Ok(messages
            .into_iter()
            .flatten()
            .map(|m| Message { inner: m })
            .collect())
    }

    /// Edits a message's text.
    pub async fn edit_message(
        &self,
        peer: &Peer,
        message_id: i32,
        new_text: impl Into<String>,
    ) -> Result<()> {
        let peer_ref = to_peer_ref(peer).await?;

        self.client
            .edit_message(peer_ref, message_id, new_text.into())
            .await
            .context("failed to edit message")?;

        Ok(())
    }

    /// Deletes messages from a peer.
    pub async fn delete_messages(&self, peer: &Peer, message_ids: &[i32]) -> Result<()> {
        let peer_ref = to_peer_ref(peer).await?;

        self.client
            .delete_messages(peer_ref, message_ids)
            .await
            .context("failed to delete messages")?;
        Ok(())
    }

    /// Returns the underlying grammers Client (for advanced use).
    pub fn inner(&self) -> &Client {
        &self.client
    }
}

/// Converts a [`Peer`] into the cached [`PeerRef`] required by most methods.
async fn to_peer_ref(peer: &Peer) -> Result<PeerRef> {
    let peer_ref = peer
        .to_ref()
        .await
        .map_err(|err| anyhow!("failed to convert peer to a reference: {err}"))?;

    peer_ref.context("peer is not usable (not cached in the session yet)")
}

/// A Telegram message.
pub struct Message {
    inner: GMessage,
}

impl Message {
    pub fn from_raw(inner: GMessage) -> Self {
        Self { inner }
    }

    pub fn id(&self) -> i32 {
        self.inner.id()
    }

    pub fn outgoing(&self) -> bool {
        self.inner.outgoing()
    }

    pub fn peer(&self) -> Option<Peer> {
        self.inner.peer().cloned()
    }

    pub fn text(&self) -> String {
        self.inner.text().to_string()
    }
}

/// Input media for albums (audio/photo/document).
pub struct InputMedia {
    inner: GInputMedia,
}

impl InputMedia {
    pub fn audio(
        uploaded: Uploaded,
        title: String,
        performer: String,
        thumbnail: Option<Uploaded>,
    ) -> Self {
        use grammers_client::media::Attribute;
        use std::time::Duration;

        let mut media = GInputMedia::new()
            .document(uploaded)
            .attribute(Attribute::Audio {
                duration: Duration::from_secs(0), // Duration unknown
                title: Some(title),
                performer: Some(performer),
            });

        if let Some(thumb) = thumbnail {
            media = media.thumbnail(thumb);
        }

        Self { inner: media }
    }
}
