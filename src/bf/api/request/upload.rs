// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use serde_derive::Serialize;

use crate::bf::model::S3File;

/// A preview of files to be uploaded to the Blackfynn platform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadPreview {
    files: Vec<S3File>,
}

impl UploadPreview {
    #[allow(dead_code)]
    pub fn new(files: &[S3File]) -> Self {
        Self {
            files: files.to_owned(),
        }
    }
}
