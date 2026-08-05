use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use grammers_session::types::{ChannelState, DcOption, PeerId, PeerInfo, UpdateState, UpdatesState};
use grammers_session::{BoxFuture, Session, SessionData};

/// Serde-friendly mirror of [`SessionData`], which itself does not implement
/// `Serialize`/`Deserialize`. The grammers types used as fields do, so we can
/// round-trip the whole session through JSON.
#[derive(serde::Serialize, serde::Deserialize)]
struct FileSessionData {
    home_dc: i32,
    dc_options: HashMap<i32, DcOption>,
    peer_infos: HashMap<PeerId, PeerInfo>,
    updates_state: UpdatesState,
}

impl From<&SessionData> for FileSessionData {
    fn from(data: &SessionData) -> Self {
        Self {
            home_dc: data.home_dc,
            dc_options: data.dc_options.clone(),
            peer_infos: data.peer_infos.clone(),
            updates_state: data.updates_state.clone(),
        }
    }
}

impl From<FileSessionData> for SessionData {
    fn from(data: FileSessionData) -> Self {
        Self {
            home_dc: data.home_dc,
            dc_options: data.dc_options,
            peer_infos: data.peer_infos,
            updates_state: data.updates_state,
        }
    }
}

/// Errors from the [`FileSession`] storage.
#[derive(Debug)]
pub enum FileSessionError {
    /// The internal mutex was poisoned.
    Poisoned,
    /// A filesystem operation failed.
    Io(std::io::Error),
    /// The session data could not be (de)serialized.
    Json(serde_json::Error),
}

impl fmt::Display for FileSessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileSessionError::Poisoned => write!(f, "session lock is poisoned"),
            FileSessionError::Io(err) => write!(f, "session file I/O error: {err}"),
            FileSessionError::Json(err) => write!(f, "session serialization error: {err}"),
        }
    }
}

impl std::error::Error for FileSessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FileSessionError::Io(err) => Some(err),
            FileSessionError::Json(err) => Some(err),
            FileSessionError::Poisoned => None,
        }
    }
}

/// A pure-Rust `Session` storage backed by a single JSON file.
///
/// Replaces the sqlite-backed `SqliteSession` so the binary stays free of
/// C/C++ code (libsql). The [`SessionData`] is serialized to disk on every
/// mutation, so the session survives restarts without logging in again.
pub struct FileSession {
    path: PathBuf,
    data: Mutex<SessionData>,
}

impl FileSession {
    /// Opens (or creates) the session file at `path`.
    pub fn open(path: &Path) -> Result<Self, FileSessionError> {
        let data = if path.exists() {
            let raw = std::fs::read_to_string(path).map_err(FileSessionError::Io)?;
            let stored: FileSessionData = serde_json::from_str(&raw).map_err(FileSessionError::Json)?;
            stored.into()
        } else {
            SessionData::default()
        };

        Ok(Self {
            path: path.to_path_buf(),
            data: Mutex::new(data),
        })
    }

    /// Persists the current session state to disk.
    pub fn save(&self) -> Result<(), FileSessionError> {
        let data = self.data.lock().map_err(|_| FileSessionError::Poisoned)?;
        let raw = serde_json::to_string(&FileSessionData::from(&*data)).map_err(FileSessionError::Json)?;
        std::fs::write(&self.path, raw).map_err(FileSessionError::Io)
    }

    fn data(&self) -> Result<MutexGuard<'_, SessionData>, FileSessionError> {
        self.data.lock().map_err(|_| FileSessionError::Poisoned)
    }
}

impl Session for FileSession {
    type Error = FileSessionError;

    fn home_dc_id(&self) -> Result<i32, Self::Error> {
        Ok(self.data()?.home_dc)
    }

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            self.data()?.home_dc = dc_id;
            self.save()
        })
    }

    fn dc_option(&self, dc_id: i32) -> Result<Option<DcOption>, Self::Error> {
        Ok(self.data()?.dc_options.get(&dc_id).cloned())
    }

    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, Result<(), Self::Error>> {
        let dc_option = dc_option.clone();
        Box::pin(async move {
            self.data()?
                .dc_options
                .insert(dc_option.id, dc_option.clone());
            self.save()
        })
    }

    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Result<Option<PeerInfo>, Self::Error>> {
        Box::pin(async move { Ok(self.data()?.peer_infos.get(&peer).cloned()) })
    }

    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, Result<(), Self::Error>> {
        let peer = peer.clone();
        Box::pin(async move {
            self.data()?
                .peer_infos
                .entry(peer.id())
                .or_insert_with(|| peer.clone())
                .extend_info(&peer);
            self.save()
        })
    }

    fn updates_state(&self) -> BoxFuture<'_, Result<UpdatesState, Self::Error>> {
        Box::pin(async move { Ok(self.data()?.updates_state.clone()) })
    }

    fn set_update_state(&self, update: UpdateState) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            let mut data = self.data()?;

            match update {
                UpdateState::All(updates_state) => {
                    data.updates_state = updates_state;
                }
                UpdateState::Primary { pts, date, seq } => {
                    data.updates_state.pts = pts;
                    data.updates_state.date = date;
                    data.updates_state.seq = seq;
                }
                UpdateState::Secondary { qts } => {
                    data.updates_state.qts = qts;
                }
                UpdateState::Channel { id, pts } => {
                    data.updates_state.channels.retain(|c| c.id != id);
                    data.updates_state.channels.push(ChannelState { id, pts });
                }
            }

            self.save()
        })
    }
}
