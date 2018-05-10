// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// An AWS S3 access key.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessKey(String);

impl AccessKey {
    pub fn new(key: String) -> Self {
        AccessKey(key)
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<String> for AccessKey {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for AccessKey {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<AccessKey> for String {
    fn from(key: AccessKey) -> Self {
        key.0
    }
}

impl From<String> for AccessKey {
    fn from(key: String) -> Self {
        AccessKey::new(key)
    }
}

/// An AWS S3 secret key.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretKey(String);

impl SecretKey {
    pub fn new(key: String) -> Self {
        SecretKey(key)
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<String> for SecretKey {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for SecretKey {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<SecretKey> for String {
    fn from(key: SecretKey) -> Self {
        key.0
    }
}

impl From<String> for SecretKey {
    fn from(key: String) -> Self {
        SecretKey::new(key)
    }
}

/// An AWS S3 bucket.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Bucket(String);

impl S3Bucket {
    pub fn new(s3_bucket: String) -> Self {
        S3Bucket(s3_bucket)
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<String> for S3Bucket {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for S3Bucket {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<S3Bucket> for String {
    fn from(s3_bucket: S3Bucket) -> Self {
        s3_bucket.0
    }
}

impl From<String> for S3Bucket {
    fn from(s3_bucket: String) -> Self {
        S3Bucket::new(s3_bucket)
    }
}

/// An AWS S3 object key.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Key(String);

impl S3Key {
    pub fn new(s3_key: String) -> Self {
        S3Key(s3_key)
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Converts a static `S3Key` into an appendable `S3UploadKey`. When
    /// converting a `S3Key` to `S3UploadKey`, the contents of the `S3Key`
    /// become the `email` property of the `S3UploadKey`:
    pub fn as_upload_key(&self, import_id: &model::ImportId, file_name: &str) -> S3UploadKey {
        S3UploadKey::new(&self.0, import_id, file_name)
    }
}

impl AsRef<String> for S3Key {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for S3Key {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<S3Key> for String {
    fn from(s3_key: S3Key) -> Self {
        s3_key.0
    }
}

impl From<String> for S3Key {
    fn from(s3_key: String) -> Self {
        S3Key::new(s3_key)
    }
}

/// An appendable, AWS S3 object key used for uploading to the Blackfynn platform.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct S3UploadKey {
    email: String,
    import_id: model::ImportId,
    file_name: String,
}

impl S3UploadKey {
    pub fn new(email: &str, import_id: &model::ImportId, file_name: &str) -> Self {
        Self {
            email: email.to_string(),
            import_id: import_id.clone(),
            file_name: file_name.to_string(),
        }
    }

    fn format_as_key(&self) -> String {
        format!(
            "{email}/{import_id}/{file_name}",
            email = self.email,
            import_id = AsRef::<String>::as_ref(&self.import_id),
            file_name = self.file_name
        )
    }
}

impl From<S3UploadKey> for String {
    fn from(s3_key: S3UploadKey) -> Self {
        s3_key.format_as_key()
    }
}

impl From<S3UploadKey> for S3Key {
    fn from(s3_upload_key: S3UploadKey) -> Self {
        S3Key::new(s3_upload_key.format_as_key())
    }
}

/// An AWS server-side encryption type.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3ServerSideEncryption {
    KMS,
    AES256,
}

impl From<S3ServerSideEncryption> for String {
    fn from(encryption_type: S3ServerSideEncryption) -> Self {
        String::from(Into::<&'static str>::into(encryption_type))
    }
}

impl From<S3ServerSideEncryption> for &'static str {
    fn from(encryption_type: S3ServerSideEncryption) -> Self {
        match encryption_type {
            S3ServerSideEncryption::KMS => "aws:kms",
            S3ServerSideEncryption::AES256 => "AES256",
        }
    }
}

impl Default for S3ServerSideEncryption {
    fn default() -> Self {
        S3ServerSideEncryption::KMS
    }
}

/// An AWS encryption key.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3EncryptionKeyId(String);

impl S3EncryptionKeyId {
    #[allow(dead_code)]
    pub fn new(encryption_key_id: String) -> Self {
        S3EncryptionKeyId(encryption_key_id)
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<String> for S3EncryptionKeyId {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for S3EncryptionKeyId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<S3EncryptionKeyId> for String {
    fn from(encryption_key_id: S3EncryptionKeyId) -> Self {
        encryption_key_id.0
    }
}

impl From<String> for S3EncryptionKeyId {
    fn from(encryption_key_id: String) -> Self {
        S3EncryptionKeyId::new(encryption_key_id)
    }
}

/// An AWS multipart upload identifier.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3UploadId(String);

impl S3UploadId {
    #[allow(dead_code)]
    pub fn new(upload_id: String) -> Self {
        S3UploadId(upload_id)
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<String> for S3UploadId {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for S3UploadId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for S3UploadId {
    fn from(upload_id: String) -> Self {
        S3UploadId::new(upload_id)
    }
}

impl From<S3UploadId> for String {
    fn from(upload_id: S3UploadId) -> Self {
        upload_id.0
    }
}
