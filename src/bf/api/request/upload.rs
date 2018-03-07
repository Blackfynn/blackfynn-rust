// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// A type representing a preview of files to be uploaded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewPackage {
    pub files: Vec<model::upload::S3File>
}

impl PreviewPackage {
    #[allow(dead_code)]
    pub fn new(files: &Vec<model::upload::S3File>) -> Self {
        Self {
            files: files.clone()
        }
    }
}
