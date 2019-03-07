// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use serde_derive::{Deserialize, Serialize};

use crate::bf::api::BFName;

/// The representation type of a `model::File`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FileObjectType {
    File,
    View,
    Source,
}

/// A file on the Blackfynn platform.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct File {
    name: String,
    file_type: String, //TODO Make this typed
    s3bucket: String,
    s3key: String,
    object_type: FileObjectType,
    size: u64,
    created_at: String,
    updated_at: String,
}

impl BFName for File {
    fn name(&self) -> &String {
        &self.name
    }
}

impl File {
    #[allow(dead_code)]
    pub fn file_type(&self) -> &String {
        &self.file_type
    }

    #[allow(dead_code)]
    pub fn s3_bucket(&self) -> &String {
        &self.s3bucket
    }

    #[allow(dead_code)]
    pub fn s3_key(&self) -> &String {
        &self.s3key
    }

    #[allow(dead_code)]
    pub fn s3_url(&self) -> String {
        format!(
            "http://{bucket}.s3.amazonaws.com/{key}",
            bucket = self.s3_bucket(),
            key = self.s3_key()
        )
    }

    #[allow(dead_code)]
    pub fn object_type(&self) -> &FileObjectType {
        &self.object_type
    }

    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[allow(dead_code)]
    pub fn created_at(&self) -> &String {
        &self.created_at
    }

    #[allow(dead_code)]
    pub fn updated_at(&self) -> &String {
        &self.updated_at
    }
}
