// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

/// Representation of a Blackfynn API file

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FileObjectType {
    File,
    View,
    Source
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct File {
    name: String,
    file_type: String, //TODO Make this typed
    s3bucket: String,
    s3key: String,
    object_type: FileObjectType,
    size: u64
}

impl File {
    #[allow(dead_code)]
    pub fn name(&self) -> &String {
        &self.name
    }

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
    pub fn object_type(&self) -> &FileObjectType {
        &self.object_type
    }

    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        self.size
    }
}
