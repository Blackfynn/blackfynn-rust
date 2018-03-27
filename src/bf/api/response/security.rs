// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// A type representing temporary credentials to perform an action, like
/// uploading a file or stream data.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporaryCredential(model::TemporaryCredential);

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

/// A type representing credentials to upload a file.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadCredential(model::UploadCredential);

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
