// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::{cmp, fmt, fs, result};

use futures::*;
use serde_derive::{Deserialize, Serialize};

use crate::bf::util::futures::{into_future_trait, into_stream_trait};
use crate::bf::{model, Error, Future, Result, Stream};

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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
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
    ) -> Result<Self> {
        let handle = fs::File::open(path)?;
        Ok(Self {
            handle,
            file_size,
            chunk_size,
            index,
        })
    }

    pub fn read(&mut self) -> Result<Vec<u8>> {
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

/// A type representing a file to be uploaded.
#[derive(Clone, Deserialize, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FileUpload {
    RecursiveUpload {
        id: UploadId,
        base_path: PathBuf,
        relative_path: PathBuf,
    },
    NonRecursiveUpload {
        id: UploadId,
        absolute_path: PathBuf,
    },
}
impl FileUpload {
    /// Returns a FileUpload object that represents a single file
    /// within a flat directory upload. This means that this and all
    /// other files in this upload have the same parent directory.
    ///
    /// # Arguments
    ///
    /// * `id` - An identifier for this upload. This can be used to tie
    ///          the correct entry in the response from the Blackfynn upload
    ///          service back to this file.
    /// * `absolute_path` - The absolute path of the file to be uploaded
    ///
    /// # Example
    ///
    /// ```
    /// use blackfynn::model::{FileUpload, UploadId};
    ///
    /// let non_recursive_upload = FileUpload::new_non_recursive_upload(
    ///   UploadId::from(1), "/Users/matt/my_file.txt"
    /// );
    /// ```
    pub fn new_non_recursive_upload<P: AsRef<Path>>(
        id: UploadId,
        absolute_path: P,
    ) -> Result<Self> {
        let absolute_path = absolute_path.as_ref();

        let absolute_path = if absolute_path.is_absolute() {
            absolute_path.to_path_buf()
        } else {
            absolute_path.canonicalize()?
        };

        if !absolute_path.exists() {
            return Err(Error::path_does_not_exist(absolute_path));
        }

        if !absolute_path.is_file() {
            return Err(Error::path_is_not_a_file(absolute_path));
        }

        Ok(FileUpload::NonRecursiveUpload {
            id,
            absolute_path: absolute_path.to_path_buf(),
        })
    }

    /// Returns a FileUpload object that represents a single file
    /// within a recursive directory upload. This means that this and
    /// all other files in this upload have the same base directory,
    /// but could have different parent directories.
    ///
    /// # Arguments
    ///
    /// * `id` - An identifier for this upload. This can be used to tie
    ///          the correct entry in the response from the Blackfynn upload
    ///          service back to this file.
    /// * `base_path` - The path from which the recursive upload was started
    /// * `file_path` - The path to this file, relative to the parent of the `base_path`
    ///
    /// # Example
    ///
    /// ```
    /// use blackfynn::model::{FileUpload, UploadId};
    ///
    /// let recursive_upload = FileUpload::new_recursive_upload(
    ///   UploadId::from(1),                                        // id
    ///   "/Users/matt/folder_to_recursivly_upload",                // base_path
    ///   "folder_to_recursivly_upload/nested_folder/my_file.txt",  // file_path
    /// );
    /// ```
    pub fn new_recursive_upload<P: AsRef<Path>, Q: AsRef<Path>>(
        id: UploadId,
        base_path: P,
        file_path: Q,
    ) -> Result<Self> {
        let base_path = base_path.as_ref().canonicalize()?;
        if !base_path.is_dir() {
            return Err(Error::path_is_not_a_directory(base_path));
        }

        // the base path should actually be the parent of the given
        // base path in order to put all files into a collection
        let base_path = base_path
            .parent()
            .ok_or_else(|| Error::no_path_parent(base_path.to_path_buf()))?;

        // create a full file path in order to check that it is valid
        let file_path = base_path.join(file_path);

        if !base_path.is_dir() {
            return Err(Error::path_is_not_a_directory(base_path.to_path_buf()));
        }
        if !file_path.is_file() {
            return Err(Error::path_is_not_a_file(file_path));
        }

        // strip the base_path from the file_path to make it relative again
        let file_path = file_path.strip_prefix(&base_path)?;

        Ok(FileUpload::RecursiveUpload {
            id,
            base_path: base_path.to_path_buf(),
            relative_path: file_path.to_path_buf(),
        })
    }

    /// Get the absolute path on the local filesystem of the file that
    /// is represented by this FileUpload object
    fn absolute_file_path(&self) -> PathBuf {
        match self {
            FileUpload::RecursiveUpload {
                id: _,
                base_path,
                relative_path,
            } => base_path.join(relative_path.to_path_buf()),
            FileUpload::NonRecursiveUpload {
                id: _,
                absolute_path,
            } => absolute_path.to_path_buf(),
        }
    }

    /// Get the upload ID of this particular FileUpload object.
    fn id(&self) -> UploadId {
        match self {
            FileUpload::RecursiveUpload { id, .. } => *id,
            FileUpload::NonRecursiveUpload { id, .. } => *id,
        }
    }

    /// Get the name of the file that is represented by this
    /// FileUpload object
    fn file_name(&self) -> Result<String> {
        let absolute_path = self.absolute_file_path();
        absolute_path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .map(|file_name| file_name.to_string())
            .ok_or_else(|| Error::could_not_get_filename(absolute_path))
    }

    /// Get the size of the file that is represented by this
    /// FileUpload object
    fn file_size(&self) -> Result<u64> {
        let metadata = fs::metadata(self.absolute_file_path())?;
        Ok(metadata.len())
    }

    /// Get the elements of the path where this file will be stored on
    /// the platform.
    ///
    /// 'None' means that the file will live at the root of the
    /// dataset or target collection. All nonrecursive uploads will
    /// have `None` as their destionation path, recursive uploads will
    /// have all elements of the local `file_path` starting at the
    /// `base_path`.
    fn destination_path(&self) -> Result<Option<Vec<String>>> {
        match self {
            FileUpload::RecursiveUpload { base_path, .. } => {
                let absolute_path = self.absolute_file_path();

                let destination_path = absolute_path
                    .parent()
                    .ok_or_else(|| Error::no_path_parent(absolute_path.to_path_buf()))?;
                let destination_path = destination_path.strip_prefix(base_path)?;
                let destination_path = destination_path
                    .to_path_buf()
                    .iter()
                    .map(|os_string| {
                        os_string
                            .to_str()
                            .map(|dir| dir.to_string())
                            .ok_or_else(|| {
                                Error::invalid_unicode_path(destination_path.to_path_buf())
                            })
                    })
                    .collect::<Result<Vec<String>>>()?;
                if destination_path.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(destination_path))
                }
            }
            _ => Ok(None),
        }
    }

    /// Transform this `FileUpload` object into an `S3File`
    pub fn to_s3_file(&self) -> Result<S3File> {
        let file_size = self.file_size()?;
        let file_name = self.file_name()?;
        let destination_path = self.destination_path()?;

        Ok(S3File::new(
            file_name,
            file_size,
            destination_path,
            Some(self.id()),
        ))
    }
}

/// A generic serializeable type that represents all file upload
/// types.
///
/// This is a representation of a FileUpload that is understood and
/// readable by the upload service.
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
) -> Result<Vec<S3FileChunk>> {
    let nchunks = cmp::max(1, (file_size as f64 / chunk_size as f64).ceil() as u64);
    (0..nchunks)
        .map(move |part_number| {
            S3FileChunk::new(from_path.as_ref(), file_size, chunk_size, part_number)
        })
        .collect()
}

impl S3File {
    #[allow(dead_code)]
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        file_name: String,
        file_size: u64,
        destination_path: Option<Vec<String>>,
        upload_id: Option<UploadId>,
    ) -> Self {
        Self {
            upload_id,
            file_name,
            size: file_size,
            chunked_upload: None,
            multipart_upload_id: None,
            file_path: destination_path,
        }
    }

    #[allow(dead_code)]
    #[allow(clippy::new_ret_no_self)]
    pub fn from_file_path(
        file_path: String,
        destination_path: Option<Vec<String>>,
        upload_id: Option<UploadId>,
    ) -> Result<Self> {
        let file_path: PathBuf = file_path.into();

        let metadata = fs::metadata(file_path.clone())?;
        let file_size = metadata.len();

        let file_name = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::invalid_unicode_path(file_path.clone()))?;

        Ok(Self {
            upload_id,
            file_name: file_name.to_string(),
            size: file_size,
            chunked_upload: None,
            multipart_upload_id: None,
            file_path: destination_path,
        })
    }

    #[allow(dead_code)]
    pub fn with_chunk_size(self, chunk_size: Option<u64>) -> Self {
        let size = self.size;
        Self {
            upload_id: self.upload_id,
            file_name: self.file_name,
            size: self.size,
            chunked_upload: chunk_size.map(|c| ChunkedUploadProperties {
                chunk_size: c,
                total_chunks: (size as f64 / c as f64).floor() as usize + 1,
            }),
            multipart_upload_id: self.multipart_upload_id,
            file_path: self.file_path,
        }
    }

    pub fn with_multipart_upload_id(self, multipart_upload_id: Option<MultipartUploadId>) -> Self {
        Self {
            upload_id: self.upload_id,
            file_name: self.file_name,
            size: self.size,
            chunked_upload: self.chunked_upload,
            multipart_upload_id,
            file_path: self.file_path,
        }
    }

    pub fn with_upload_id(self, upload_id: UploadId) -> Self {
        Self {
            upload_id: Some(upload_id),
            file_name: self.file_name,
            size: self.size,
            chunked_upload: self.chunked_upload,
            multipart_upload_id: self.multipart_upload_id,
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
    pub fn destination_path(&self) -> Option<&Vec<String>> {
        self.file_path.as_ref()
    }

    #[allow(dead_code)]
    pub fn read_bytes<P: AsRef<Path>>(&self, from_path: P) -> Future<Vec<u8>> {
        let file_path: PathBuf = from_path.as_ref().join(self.file_name.to_owned());
        into_future_trait(future::lazy(move || {
            let f = match fs::File::open(file_path) {
                Ok(f) => f,
                Err(e) => return future::err(e.into()),
            };
            f.bytes()
                .collect::<result::Result<Vec<_>, _>>()
                .map_err(Into::into)
                .into_future()
        }))
    }

    pub fn chunks<P: AsRef<Path>>(&self, from_path: P, chunk_size: u64) -> Stream<S3FileChunk> {
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
    file_type: String,
    package_type: Option<String>,
    uploaded_files: Vec<String>,
    upload_directory: String,
    storage_directory: String,
    encryption_key: model::S3EncryptionKeyId,
    size: u64,
}

impl ETLJob {
    #[allow(dead_code)]
    pub fn file_type(&self) -> &String {
        &self.file_type
    }

    #[allow(dead_code)]
    pub fn package_type(&self) -> Option<&String> {
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
    pub fn package_type(&self) -> Option<&String> {
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
    package_type: Option<String>,
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
    pub fn package_type(&self) -> Option<&String> {
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
                path_buf
                    .as_path()
                    .to_str()
                    .map(|path_string| path_string.to_string())
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

        let s3_file = FileUpload::new_recursive_upload(UploadId(1), path, file)
            .and_then(|file_upload| file_upload.to_s3_file());

        match s3_file {
            Err(err) => panic!("failed to get directory {:?}", err),
            Ok(s3_file) => {
                assert!(s3_file.file_path == Some(vec!["data".to_string(), "small".to_string()]))
            }
        }
    }

    #[test]
    pub fn during_directory_upload_directory_and_a_file_must_be_used() {
        let file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/small/example.csv").to_owned();
        let file_copy = file.clone();

        let s3_file = FileUpload::new_recursive_upload(UploadId(1), file, file_copy)
            .and_then(|file_upload| file_upload.to_s3_file());

        assert!(s3_file.is_err(), true);
    }

    #[test]
    pub fn during_non_directory_upload_file_path_is_none() {
        let file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/small/example.csv").to_owned();

        let s3_file = FileUpload::new_non_recursive_upload(UploadId(1), file)
            .and_then(|file_upload| file_upload.to_s3_file());

        match s3_file {
            Err(err) => panic!("failed to get directory {:?}", err),
            Ok(s3_file) => assert!(s3_file.file_path == None),
        }
    }
}
