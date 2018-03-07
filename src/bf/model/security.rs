// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use chrono::{DateTime, Utc};

use bf::model;

/// A type representing temporary credentials to perform an action, like
/// uploading a file or stream data.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporaryCredential {
    access_key: model::AccessKey,
    secret_key: model::SecretKey,
    region: String,
    session_token: model::SessionToken,
    expiration: DateTime<Utc>
}

impl TemporaryCredential {
    #[allow(dead_code)]
    pub fn access_key(&self) -> &model::AccessKey {
        &self.access_key
    }

    #[allow(dead_code)]
    pub fn secret_key(&self) -> &model::SecretKey {
        &self.secret_key
    }

    #[allow(dead_code)]
    pub fn region(&self) -> &String {
        &self.region
    }

    #[allow(dead_code)]
    pub fn session_token(&self) -> &model::SessionToken {
        &self.session_token
    }

    #[allow(dead_code)]
    pub fn expiration(&self) -> &DateTime<Utc> {
        &self.expiration
    }
}

/// A type representing credentials to upload a file.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadCredential {
    temp_credentials: TemporaryCredential,
    encryption_key_id: model::S3EncryptionKeyId,
    s3_bucket: model::S3Bucket,
    s3_key: model::S3Key
}

impl UploadCredential {
    #[allow(dead_code)]
    pub fn temp_credentials(&self) -> &TemporaryCredential {
        &self.temp_credentials
    }

    #[allow(dead_code)]
    pub fn encryption_key_id(&self) -> &model::S3EncryptionKeyId {
        &self.encryption_key_id
    }

    #[allow(dead_code)]
    pub fn s3_bucket(&self) -> &model::S3Bucket {
        &self.s3_bucket
    }

    #[allow(dead_code)]
    pub fn s3_key(&self) -> &model::S3Key {
        &self.s3_key
    }
}
