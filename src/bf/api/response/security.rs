// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use crate::bf::model;
use serde_derive::Deserialize;

/// Temporary credentials to perform an action, like uploading a file or stream data.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporaryCredential(model::TemporaryCredential);

impl TemporaryCredential {
    pub fn take(self) -> model::TemporaryCredential {
        self.0
    }
}

impl From<TemporaryCredential> for model::TemporaryCredential {
    fn from(credential: TemporaryCredential) -> Self {
        credential.0
    }
}

/// Credentials to upload a file.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadCredential(model::UploadCredential);

impl UploadCredential {
    pub fn take(self) -> model::UploadCredential {
        self.0
    }
}

impl From<UploadCredential> for model::UploadCredential {
    fn from(credential: UploadCredential) -> Self {
        credential.0
    }
}
