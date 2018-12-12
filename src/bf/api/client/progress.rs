// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::sync::Arc;
use std::path::{Path, PathBuf};

use bf::model::ImportId;

/// A trait defining a progress indicator callback. Every time a file part
/// successfully completes, `update` will be called with new, update statistics
/// for the file.
pub trait ProgressCallback: Send + Sync {
    /// Called when an uploaded progress update occurs.
    fn on_update(&self, &ProgressUpdate);
}

/// An implementation of `ProgressCallback` that does nothing.
#[derive(Debug, Clone, Hash)]
pub struct NoProgress;

impl ProgressCallback for NoProgress {
    fn on_update(&self, _update: &ProgressUpdate) {
        // Do nothing
    }
}

impl ProgressCallback for Box<dyn ProgressCallback> {
    fn on_update(&self, _update: &ProgressUpdate) {
        self.as_ref().on_update(_update)
    }
}

impl ProgressCallback for Arc<Box<dyn ProgressCallback>> {
    fn on_update(&self, _update: &ProgressUpdate) {
        let this = self.clone();
        if let Ok(cb) = Arc::try_unwrap(this) {
            cb.on_update(_update)
        }
    }
}

/// A type representing progress updates for a multipart upload.
#[derive(Debug, Clone, Hash)]
pub struct ProgressUpdate {
    part_number: usize,
    is_multipart: bool,
    import_id: ImportId,
    file_path: PathBuf,
    bytes_sent: u64,
    size: u64,
}

impl ProgressUpdate {
    pub fn new(
        part_number: usize,
        is_multipart: bool,
        import_id: ImportId,
        file_path: PathBuf,
        bytes_sent: u64,
        size: u64,
    ) -> Self {
        Self {
            part_number,
            is_multipart,
            import_id,
            file_path,
            bytes_sent,
            size,
        }
    }

    /// Returns whether the file was uploaded as a multipart upload.
    pub fn is_multipart(&self) -> bool {
        self.is_multipart
    }

    /// Returns the S3 part number of the uploading file.
    pub fn part_number(&self) -> usize {
        self.part_number
    }

    /// Returns the Blackfynn import ID the file is associated with.
    pub fn import_id(&self) -> &ImportId {
        &self.import_id
    }

    /// Returns the name, sans path, of the file being uploaded.
    pub fn file_path(&self) -> &Path {
        self.file_path.as_ref()
    }

    /// Returns the cumulative number of bytes sent to S3 for the given file.
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent
    }

    /// Returns the total size of the file in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns the upload percentage completed.
    pub fn percent_done(&self) -> f32 {
        (self.bytes_sent() as f32 / self.size() as f32) * 100.0
    }

    /// Tests if the file completed uploading.
    pub fn completed(&self) -> bool {
        self.percent_done() >= 100.0
    }
}
