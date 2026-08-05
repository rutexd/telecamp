/// Trait for reporting progress during download/upload operations.
pub trait ProgressSink: Send + Sync {
    fn report(&self, event: ProgressEvent);
}

/// A progress event during download or upload.
#[derive(Debug, Clone)]
pub struct ProgressEvent {
    pub stage: Stage,
    pub track_num: usize,
    pub total_tracks: usize,
    pub bytes: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Download,
    Upload,
}

impl ProgressEvent {
    pub fn percent(&self) -> u8 {
        if self.total_bytes == 0 {
            0
        } else {
            ((self.bytes as f64 / self.total_bytes as f64) * 100.0).min(100.0) as u8
        }
    }

    pub fn format(&self) -> String {
        let stage = match self.stage {
            Stage::Download => "⬇️ Downloading",
            Stage::Upload => "⬆️ Uploading",
        };

        format!(
            "{} track {}/{} ({}%)",
            stage,
            self.track_num,
            self.total_tracks,
            self.percent()
        )
    }
}

/// A no-op progress sink for testing.
pub struct NoopProgressSink;

impl ProgressSink for NoopProgressSink {
    fn report(&self, _event: ProgressEvent) {}
}
