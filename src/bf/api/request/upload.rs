// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model::S3File;

/// A preview of files to be uploaded to the Blackfynn platform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewPackage {
    pub files: Vec<S3File>,
}

impl PreviewPackage {
    #[allow(dead_code)]
    pub fn new(files: &[S3File]) -> Self {
        Self {
            files: files.to_owned(),
        }
    }
}
