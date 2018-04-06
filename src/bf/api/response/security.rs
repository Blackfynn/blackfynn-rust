// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// Temporary credentials to perform an action, like uploading a file or stream data.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporaryCredential(model::TemporaryCredential);

impl TemporaryCredential {
    pub fn into_inner(self) -> model::TemporaryCredential {
        self.0
    }
}

impl From<TemporaryCredential> for model::TemporaryCredential {
    fn from(credential: TemporaryCredential) -> Self {
        credential.0
    }
}

impl AsRef<model::TemporaryCredential> for TemporaryCredential {
    fn as_ref(&self) -> &model::TemporaryCredential {
        &self.0
    }
}

/// Credentials to upload a file.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadCredential(model::UploadCredential);

impl UploadCredential {
    pub fn into_inner(self) -> model::UploadCredential {
        self.0
    }
}

impl From<UploadCredential> for model::UploadCredential {
    fn from(credential: UploadCredential) -> Self {
        credential.0
    }
}

impl AsRef<model::UploadCredential> for UploadCredential {
    fn as_ref(&self) -> &model::UploadCredential {
        &self.0
    }
}
