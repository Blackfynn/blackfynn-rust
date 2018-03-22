// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use futures::*;

use bf::{self, model};

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportId(String);

impl From<ImportId> for String {
    fn from(id: ImportId) -> String {
        id.0
    }
}

impl ImportId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        ImportId(id.into())
    }
}

impl AsRef<String> for ImportId {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for ImportId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadId(i64);

impl UploadId {
    pub fn new(id: i64) -> Self {
        UploadId(id)
    }
}

impl AsRef<i64> for UploadId {
    fn as_ref(&self) -> &i64 {
        &self.0
    }
}

impl From<UploadId> for i64 {
    fn from(id: UploadId) -> i64 {
        id.0
    }
}

/// A type representing a file to be uploaded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3File {
    file_name: String,
    upload_id: Option<UploadId>,
    size: Option<u64>
}

impl S3File {
    /// Given a file path, this function checks to see if the path:
    ///
    /// 1) exists
    /// 2) does not contain invalid unicode symbols
    ///
    /// If neither condition hold, this function will return an error
    fn normalize<P: AsRef<Path>, Q: AsRef<Path>>(path: P, file: Q) -> bf::Result<(String, fs::Metadata)> {
        let file_path: PathBuf = path.as_ref().join(file.as_ref()).canonicalize()?;
        if !file_path.is_file() {
            return Err(bf::error::Error::IoError(io::Error::new(io::ErrorKind::Other,
                                                                format!("Not a file: {:?}", file_path))));
        };
        if !file_path.exists() {
            return Err(bf::error::Error::IoError(io::Error::new(io::ErrorKind::Other,
                                                                format!("Could not read: {:?}", file_path))));
        };

        // Get the full file path as a String:
        let file_name = file_path.file_name().and_then(|name| name.to_str())
            .ok_or(bf::error::Error::InvalidUnicodePath(file_path.clone()))
            .map(String::from)?;

        // And the resulting metadata so we can pull the file size:
        let metadata = fs::metadata(file_path)?;

        Ok((file_name, metadata))
    }

    #[allow(dead_code)]
    pub fn new<P: AsRef<Path>, Q: AsRef<Path>>(path: P, file: Q) -> bf::Result<Self> {
        let (file_name, metadata) = Self::normalize(path, file)?;
        Ok(Self {
            // Note: This value is only used in a meaningful way by the
            // frontend, but is still expected by the backend. We can just
            // plug in a dummy value to appease the API:
            upload_id: Some(UploadId(0)),
            file_name,
            size: Some(metadata.len())
        })
    }

    #[allow(dead_code)]
    pub fn from_file_path<P: AsRef<Path>>(file_path: P) -> bf::Result<Self> {
        let file_path = file_path.as_ref();
        let path = file_path.parent()
            .ok_or(bf::error::Error::IoError(io::Error::new(io::ErrorKind::Other, format!("Could not decompose: {:?}", file_path))))?;
        let file = file_path.file_name()
            .ok_or(bf::error::Error::IoError(io::Error::new(io::ErrorKind::Other, format!("Could not decompose: {:?}", file_path))))?;
        S3File::new(path, file)
    }

    #[allow(dead_code)]
    pub fn file_name(&self) -> &String {
        &self.file_name
    }

    #[allow(dead_code)]
    pub fn upload_id(&self) -> &Option<UploadId> {
        &self.upload_id
    }

    #[allow(dead_code)]
    pub fn size(&self) -> &Option<u64> {
        &self.size
    }

    #[allow(dead_code)]
    pub fn read_contents<P: AsRef<Path>>(&self, from_path: P) -> bf::Future<Vec<u8>> {
        let file_path: PathBuf = from_path.as_ref().join(self.file_name.to_owned());
        Box::new(future::lazy(move || {
            let f = match fs::File::open(file_path) {
                Ok(f) => f,
                Err(e) => return future::err(e.into())
            };
            future::ok(f.bytes().filter_map(Result::ok).collect::<Vec<u8>>())
        }))
    }
}

/// This serves as a minimal manifest.
/// See `blackfynn-app/api/src/main/scala/com/blackfynn/uploads/Manifest.scala`
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    append_to_package: Option<bool>,
    bucket: Option<model::S3Bucket>,
    dataset: Option<model::DatasetId>,
    destination: Option<String>,
    email: Option<String>,
    encryption_key: Option<model::S3EncryptionKeyId>,
    encryption_key_id: Option<model::S3EncryptionKeyId>,
    files: Vec<String>,
    file_type: Option<String>,
    group_id: Option<String>,
    import_id: Option<model::ImportId>,
    organization_id: model::OrganizationId,
    storage_directory: Option<String>,
    upload_directory: Option<String>,
    uploaded_files: Option<Vec<String>>
}

impl Manifest {
    #[allow(dead_code)]
    pub fn append_to_package(&self) -> &Option<bool> {
        &self.append_to_package
    }

    #[allow(dead_code)]
    pub fn bucket(&self) -> &Option<model::S3Bucket> {
        &self.bucket
    }

    #[allow(dead_code)]
    pub fn dataset(&self) -> &Option<model::DatasetId> {
        &self.dataset
    }

    #[allow(dead_code)]
    pub fn email(&self) -> &Option<String> {
        &self.email
    }

    #[allow(dead_code)]
    pub fn files(&self) -> &Vec<String> {
        &self.files
    }

    #[allow(dead_code)]
    pub fn group_id(&self) -> &Option<String> {
        &self.group_id
    }

    #[allow(dead_code)]
    pub fn import_id(&self) -> &Option<model::ImportId> {
        &self.import_id
    }

    #[allow(dead_code)]
    pub fn organization_id(&self) -> &model::OrganizationId {
        &self.organization_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackagePreview {
    package_name: String,
    import_id: ImportId,
    files: Vec<S3File>,
    group_size: i64
}

impl PackagePreview {
    #[allow(dead_code)]
    pub fn package_name(&self) -> &String {
        &self.package_name
    }

    #[allow(dead_code)]
    pub fn import_id(&self) -> &ImportId {
        &self.import_id
    }

    #[allow(dead_code)]
    pub fn files(&self) -> &Vec<S3File> {
        &self.files
    }

    #[allow(dead_code)]
    pub fn group_size(&self) -> &i64 {
        &self.group_size
    }
}
