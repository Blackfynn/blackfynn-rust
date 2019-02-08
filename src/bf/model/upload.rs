// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;
use std::fmt;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::{cmp, fs};

use futures::*;

use bf::util::futures::{into_future_trait, into_stream_trait};
use bf::{self, model};

/// An identifier returned by the Blackfynn platform used to group
/// a collection of files together for uploading.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ImportId(String);

impl ImportId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        ImportId(id.into())
    }

    /// Unwraps the value.
    pub fn take(self) -> String {
        self.0
    }
}

impl Borrow<String> for ImportId {
    fn borrow(&self) -> &String {
        &self.0
    }
}

impl Borrow<str> for ImportId {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl From<ImportId> for String {
    fn from(id: ImportId) -> String {
        id.0
    }
}

impl<'a> From<&'a ImportId> for String {
    fn from(id: &'a ImportId) -> String {
        id.0.to_string()
    }
}

impl From<String> for ImportId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl<'a> From<&'a str> for ImportId {
    fn from(id: &'a str) -> Self {
        Self::new(String::from(id))
    }
}

impl fmt::Display for ImportId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct UploadId(u64);

impl UploadId {
    pub fn new(id: u64) -> Self {
        UploadId(id)
    }

    /// Unwraps the value.
    pub fn take(self) -> u64 {
        self.0
    }
}

impl Borrow<u64> for UploadId {
    fn borrow(&self) -> &u64 {
        &self.0
    }
}

impl From<u64> for UploadId {
    fn from(id: u64) -> Self {
        UploadId(id)
    }
}

impl From<UploadId> for u64 {
    fn from(id: UploadId) -> u64 {
        id.0
    }
}

// /// A type representing a chunk of an S3 file.
pub struct S3FileChunk {
    handle: fs::File,
    file_size: u64,
    chunk_size: u64,
    index: u64,
}

impl S3FileChunk {
    #[allow(clippy::new_ret_no_self)]
    pub fn new<P: AsRef<Path>>(
        path: P,
        file_size: u64,
        chunk_size: u64,
        index: u64,
    ) -> bf::Result<Self> {
        let handle = fs::File::open(path)?;
        Ok(Self {
            handle,
            file_size,
            chunk_size,
            index,
        })
    }

    pub fn read(&mut self) -> bf::Result<Vec<u8>> {
        let offset = self.chunk_size * self.index;
        assert!(offset <= self.file_size);
        let read_amount = self.file_size - offset;
        let n = if read_amount > self.chunk_size {
            self.chunk_size
        } else {
            read_amount
        } as usize;
        //let mut buf = vec![0u8; n];
        let mut buf = Vec::with_capacity(n);
        unsafe {
            buf.set_len(n);
        }

        self.handle.seek(SeekFrom::Start(offset))?;
        self.handle.read_exact(buf.as_mut_slice())?;
        Ok(buf)
    }

    /// Returns the AWS S3 multipart file part number.
    /// Note: S3 part numbers are 1-based.
    pub fn part_number(&self) -> u64 {
        self.index + 1
    }
}

#[derive(Clone, Deserialize, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct Checksum(pub String);

#[derive(Clone, Deserialize, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct MultipartUploadId(pub String);

impl From<String> for MultipartUploadId {
    fn from(s: String) -> MultipartUploadId {
        MultipartUploadId(s)
    }
}

impl From<&MultipartUploadId> for String {
    fn from(id: &MultipartUploadId) -> String {
        id.0.to_string()
    }
}

#[derive(Copy, Clone, Deserialize, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChunkedUploadProperties {
    pub chunk_size: u64,
    total_chunks: usize,
}

/// A non canonical but validated path to a file
///
/// `file_name` is the name of the file being uploaded
///
/// `destination_path` represents the location in the platform
/// the file will be uploaded to
///
/// `metadata` is the metadata of the file being uploaded
struct NormalizedPath {
    file_name: String,
    destination_path: Option<Vec<String>>,
    metadata: fs::Metadata,
}

impl NormalizedPath {
    pub fn new(
        file_name: String,
        destination_path: Option<Vec<String>>,
        metadata: fs::Metadata,
    ) -> Self {
        Self {
            file_name,
            destination_path,
            metadata,
        }
    }

    pub fn file_name(&self) -> &String {
        &self.file_name
    }

    pub fn destination_path(&self) -> Option<&Vec<String>> {
        self.destination_path.as_ref()
    }

    pub fn metadata(&self) -> &fs::Metadata {
        &self.metadata
    }
}

/// A type representing a file to be uploaded.
#[derive(Clone, Deserialize, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3File {
    file_name: String,
    upload_id: Option<UploadId>,
    size: u64,
    chunked_upload: Option<ChunkedUploadProperties>,
    multipart_upload_id: Option<MultipartUploadId>,
    file_path: Option<Vec<String>>,
}

fn file_chunks<P: AsRef<Path>>(
    from_path: P,
    file_size: u64,
    chunk_size: u64,
) -> bf::Result<Vec<S3FileChunk>> {
    let nchunks = cmp::max(1, (file_size as f64 / chunk_size as f64).ceil() as u64);
    (0..nchunks)
        .map(move |part_number| {
            S3FileChunk::new(from_path.as_ref(), file_size, chunk_size, part_number)
        })
        .collect()
}

impl S3File {
    /// path is expected to be a base directory, and file is expected to be a filename + extension.
    /// When path and file are joined with a separator, a full (but not necessarily absolute) file
    /// path is constructed.
    ///
    /// If neither condition hold, this function will return an error
    fn normalize<P: AsRef<Path>, Q: AsRef<Path>>(path: P, file: Q) -> bf::Result<NormalizedPath> {
        let directory_path = path.as_ref();
        let file_path: PathBuf = directory_path.join(file.as_ref()).canonicalize()?;
        if !file_path.is_file() {
            return Err(bf::error::ErrorKind::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!("Not a file: {:?}", file_path),
            ))
            .into());
        };
        if !file_path.exists() {
            return Err(bf::error::ErrorKind::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!("Could not read: {:?}", file_path),
            ))
            .into());
        };

        // Get the file name as a String:
        let file_name: bf::Result<String> = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| bf::error::ErrorKind::InvalidUnicodePathError(file_path.clone()).into())
            .map(String::from);

        let file_name = file_name?;

        let canonical_dir_path = directory_path.canonicalize()?;

        let file_path_copy = file_path.clone();

        // the cannonical file path without the cannonical path of the
        // the direcctory being uploaded to
        // eg
        // "/user/pete/data/sample/image.png" is the canonical file_path
        // "/user/pete/" is the canonical_dir_path
        // result upload_dir_path is "/data/sample/"
        let upload_dir_path = file_path_copy
            .strip_prefix(&canonical_dir_path)
            .map_err(|err| {
                bf::error::Error::with_chain(
                    err,
                    format!(
                        "could not strip prefix from {:?} with {:?}",
                        file_path_copy, canonical_dir_path
                    ),
                )
            })?;

        // the directory the file is to be uploaded to
        let destination_path: Option<Vec<String>> = upload_dir_path
            .parent()
            .map(|path| {
                path
                    .to_path_buf()
                    .iter()
                    .map(|os_string| {
                        os_string
                            .to_str()
                            .map(|dir| dir.to_string())
                            .ok_or_else(|| bf::error::ErrorKind::InvalidUnicodePathError(path.to_path_buf().clone()).into())
                    })
                    .collect::<bf::Result<Vec<String>>>()
            })
            .map_or(Ok(None), |maybe_dir| maybe_dir.map(|dir| Some(dir)))?;

        // And the resulting metadata so we can pull the file size:
        let metadata = fs::metadata(file_path)?;

        Ok(NormalizedPath::new(file_name, destination_path, metadata))
    }

    #[allow(dead_code)]
    #[allow(clippy::new_ret_no_self)]
    pub fn new<P: AsRef<Path>, Q: AsRef<Path>>(
        path: P,
        file: Q,
        upload_id: Option<UploadId>,
    ) -> bf::Result<Self> {
        let normalized_path = Self::normalize(path.as_ref(), file.as_ref())?;

        Ok(Self {
            upload_id,
            file_name: normalized_path.file_name,
            size: normalized_path.metadata.len(),
            chunked_upload: None,
            multipart_upload_id: None,
            file_path: normalized_path.destination_path,
        })
    }

    /// Construct a S3File with the a `file_path` that is the difference
    /// from the `file_path` to the `directory_path`
    ///
    /// for `file_path` = "/data/sample/image.png" and
    /// `directory_path` = "data/" then the resulting `file_path`
    /// on the `S3File` will be "data/sample"
    pub fn retaining_file_path<P: AsRef<Path>, Q: AsRef<Path>>(
        directory_path: Q,
        file_path: P,
        // perview path should be expected uploaded directory
        upload_id: Option<UploadId>,
    ) -> bf::Result<Self> {
        let directory_path = directory_path.as_ref();
        let file_path = file_path.as_ref();

        if !directory_path.is_dir() {
            return Err(bf::error::ErrorKind::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!("Provided path was not a directory: {:?}", directory_path),
            ))
            .into());
        }

        let root_dir_path = directory_path.parent().ok_or_else(|| {
            bf::error::ErrorKind::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!("Could not destructure path: {:?}", directory_path),
            ))
        })?;

        S3File::new(root_dir_path, file_path, upload_id)
    }

    #[allow(dead_code)]
    pub fn from_file_path<P: AsRef<Path>>(
        file_path: P,
        upload_id: Option<UploadId>,
    ) -> bf::Result<Self> {
        let file_path = file_path.as_ref();
        let path = file_path.parent().ok_or_else(|| {
            bf::error::ErrorKind::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!("Could not destructure path: {:?}", file_path),
            ))
        })?;
        let file = file_path.file_name().ok_or_else(|| {
            bf::error::ErrorKind::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!("Could not destructure path: {:?}", file_path),
            ))
        })?;
        S3File::new(path, file, upload_id)
    }

    #[allow(dead_code)]
    pub fn with_chunk_size(self, chunk_size: Option<u64>) -> Self {
        Self {
            upload_id: self.upload_id.clone(),
            file_name: self.file_name.clone(),
            size: self.size,
            chunked_upload: chunk_size.map(|c| ChunkedUploadProperties {
                chunk_size: c,
                total_chunks: (self.size as f64 / c as f64).floor() as usize + 1,
            }),
            multipart_upload_id: self.multipart_upload_id,
            file_path: self.file_path,
        }
    }

    #[allow(dead_code)]
    pub fn with_multipart_upload_id(self, multipart_upload_id: Option<MultipartUploadId>) -> Self {
        Self {
            upload_id: self.upload_id.clone(),
            file_name: self.file_name.clone(),
            size: self.size,
            chunked_upload: self.chunked_upload,
            multipart_upload_id,
            file_path: self.file_path,
        }
    }

    #[allow(dead_code)]
    pub fn chunked_upload(&self) -> Option<&ChunkedUploadProperties> {
        self.chunked_upload.as_ref()
    }

    #[allow(dead_code)]
    pub fn file_name(&self) -> &String {
        &self.file_name
    }

    #[allow(dead_code)]
    pub fn upload_id(&self) -> Option<&UploadId> {
        self.upload_id.as_ref()
    }

    #[allow(dead_code)]
    pub fn multipart_upload_id(&self) -> Option<&MultipartUploadId> {
        self.multipart_upload_id.as_ref()
    }

    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[allow(dead_code)]
    pub fn read_bytes<P: AsRef<Path>>(&self, from_path: P) -> bf::Future<Vec<u8>> {
        let file_path: PathBuf = from_path.as_ref().join(self.file_name.to_owned());
        into_future_trait(future::lazy(move || {
            let f = match fs::File::open(file_path) {
                Ok(f) => f,
                Err(e) => return future::err(e.into()),
            };
            f.bytes()
                .collect::<Result<Vec<_>, _>>()
                .map_err(Into::into)
                .into_future()
        }))
    }

    pub fn chunks<P: AsRef<Path>>(&self, from_path: P, chunk_size: u64) -> bf::Stream<S3FileChunk> {
        let file_path = from_path.as_ref().join(self.file_name.clone());
        match file_chunks(file_path, self.size(), chunk_size) {
            Ok(ch) => into_stream_trait(stream::iter_ok(ch)),
            Err(e) => into_stream_trait(stream::once(Err(e))),
        }
    }
}

// An ETL processor job type
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
enum ETLJobType {
    Upload,
    Append,
}

// A manifest job, as generated by the Nextflow ETL processor.
#[derive(Clone, Deserialize, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ETLJob {
    // TODO: make this typed
    file_type: String,
    #[serde(deserialize_with = "model::PackageType::deserialize")]
    package_type: Option<model::PackageType>,
    uploaded_files: Vec<String>,
    upload_directory: String,
    storage_directory: String,
    encryption_key: model::S3EncryptionKeyId,
    size: u64,
    // has_Workflow: bool
}

impl ETLJob {
    #[allow(dead_code)]
    pub fn file_type(&self) -> &String {
        &self.file_type
    }

    #[allow(dead_code)]
    pub fn package_type(&self) -> Option<&model::PackageType> {
        self.package_type.as_ref()
    }

    #[allow(dead_code)]
    pub fn uploaded_files(&self) -> &Vec<String> {
        &self.uploaded_files
    }

    #[allow(dead_code)]
    pub fn upload_directory(&self) -> &String {
        &self.upload_directory
    }

    #[allow(dead_code)]
    pub fn storage_directory(&self) -> &String {
        &self.storage_directory
    }

    #[allow(dead_code)]
    pub fn encryption_key(&self) -> &model::S3EncryptionKeyId {
        &self.encryption_key
    }

    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        self.size
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ETLManifest {
    #[serde(rename = "type")]
    type_: ETLJobType,
    import_id: model::ImportId,
    //organizationId: i32,
    content: ETLJob,
}

impl ETLManifest {
    #[allow(dead_code)]
    pub fn import_id(&self) -> &model::ImportId {
        &self.import_id
    }

    #[allow(dead_code)]
    pub fn job_type(&self) -> &ETLJobType {
        &self.type_
    }

    #[allow(dead_code)]
    pub fn job_contents(&self) -> &ETLJob {
        &self.content
    }

    #[allow(dead_code)]
    /// Returns a collection of uploaded files, relative to the Blackfynn S3 bucket.
    pub fn files(&self) -> &Vec<String> {
        &self.content.uploaded_files
    }
}

// See `blackfynn-app/api/src/main/scala/com/blackfynn/uploads/Manifest.scala`
/// A file upload manifest.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestEntry {
    manifest: ETLManifest,
}

impl ManifestEntry {
    #[allow(dead_code)]
    /// Import ID of the upload.
    pub fn import_id(&self) -> &model::ImportId {
        &self.manifest.import_id()
    }

    #[allow(dead_code)]
    /// File type of the upload.
    pub fn file_type(&self) -> &String {
        &self.manifest.job_contents().file_type()
    }

    #[allow(dead_code)]
    /// Package type of the upload.
    pub fn package_type(&self) -> Option<&model::PackageType> {
        self.manifest.job_contents().package_type()
    }

    #[allow(dead_code)]
    /// A list of files that were uploaded.
    pub fn uploaded_files(&self) -> &Vec<String> {
        &self.manifest.job_contents().uploaded_files
    }

    #[allow(dead_code)]
    /// The directory files were uploaded to.
    pub fn upload_directory(&self) -> &String {
        &self.manifest.job_contents().upload_directory
    }

    #[allow(dead_code)]
    pub fn storage_directory(&self) -> &String {
        &self.manifest.job_contents().storage_directory
    }

    #[allow(dead_code)]
    pub fn encryption_key(&self) -> &model::S3EncryptionKeyId {
        &self.manifest.job_contents().encryption_key
    }

    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        self.manifest.job_contents().size
    }

    #[allow(dead_code)]
    /// A collection of uploaded files, relative to the Blackfynn S3 bucket.
    pub fn files(&self) -> &Vec<String> {
        &self.manifest.files()
    }
}

/// A preview of a collection of files uploaded to the Blackfynn platform.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackagePreview {
    package_name: String,
    #[serde(deserialize_with = "model::PackageType::deserialize")]
    package_type: Option<model::PackageType>,
    file_type: Option<String>,
    import_id: ImportId,
    files: Vec<S3File>,
    group_size: i64,
    preview_path: Option<Vec<String>>,
}

impl PackagePreview {
    #[allow(dead_code)]
    pub fn package_name(&self) -> &String {
        &self.package_name
    }

    #[allow(dead_code)]
    pub fn package_type(&self) -> Option<&model::PackageType> {
        self.package_type.as_ref()
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
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    #[allow(dead_code)]
    pub fn file_type(&self) -> Option<&String> {
        self.file_type.as_ref()
    }

    #[allow(dead_code)]
    pub fn group_size(&self) -> &i64 {
        &self.group_size
    }

    #[allow(dead_code)]
    pub fn preview_path(self) -> Option<String> {
        self.preview_path
            .map(|dirs| dirs.iter().cloned().collect::<PathBuf>())
            .and_then(|path_buf| {
                path_buf.as_path().to_str().map(|path_string| path_string.to_string())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    const USE_CHUNK_SIZE: u64 = 100;

    #[test]
    pub fn empty_file_chunking_works() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/small/empty_file").to_owned();
        let metadata = File::open(path.clone()).unwrap().metadata().unwrap();
        let result = file_chunks(path, metadata.len(), USE_CHUNK_SIZE);
        assert!(result.is_ok());
        let chunks = result.unwrap();
        assert!(chunks.len() == 1);
    }

    #[test]
    pub fn nonempty_file_chunking_works() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/small/example.csv").to_owned();
        let metadata = File::open(path.clone()).unwrap().metadata().unwrap();
        let result = file_chunks(path, metadata.len(), USE_CHUNK_SIZE);
        match result {
            Err(err) => panic!("file chunking error: {:?}", err),
            Ok(_) => {
                let chunks = result.unwrap();
                assert!(chunks.len() > 1);
            }
        }
    }

    #[test]
    pub fn during_directory_upload_root_upload_directory_path_finding_works() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/").to_owned();
        let file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/small/example.csv").to_owned();

        let result = S3File::retaining_file_path(path, file, None);

        match result {
            Err(err) => panic!("failed to get directory {:?}", err),
            Ok(s3_file) => assert!(s3_file.file_path == Some(vec!["data".to_string(), "small".to_string()])),
        }
    }

    pub fn during_directory_upload_directory_and_a_file_must_be_used() {
        let file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/small/example.csv").to_owned();
        let file_copy = file.clone();

        let result = S3File::retaining_file_path(file, file_copy, None);

        assert!(result.is_err(), true);
    }

    #[test]
    pub fn during_non_directory_upload_file_path_is_none() {
        let file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/small/example.csv").to_owned();

        let result = S3File::from_file_path(file, None);

        match result {
            Err(err) => panic!("failed to get directory {:?}", err),
            Ok(s3_file) => assert!(s3_file.file_path == Some(vec![])),
        }
    }
}
