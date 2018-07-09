// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::{cmp, fs};

use futures::*;

use bf::util::futures::{into_future_trait, into_stream_trait};
use bf::{self, model};

/// An identifier returned by the Blackfynn platform used to group
/// a collection of files together for uploading.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportId(String);

impl ImportId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        ImportId(id.into())
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
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

impl From<ImportId> for String {
    fn from(id: ImportId) -> String {
        id.0
    }
}

impl From<String> for ImportId {
    fn from(id: String) -> Self {
        ImportId::new(id)
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadId(u64);

impl UploadId {
    pub fn new(id: u64) -> Self {
        UploadId(id)
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> u64 {
        self.0
    }
}

impl AsRef<u64> for UploadId {
    fn as_ref(&self) -> &u64 {
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
    pub fn new<P: AsRef<Path>>(
        path: P,
        file_size: u64,
        chunk_size: u64,
        index: u64,
    ) -> bf::Result<S3FileChunk> {
        let handle = fs::File::open(path)?;
        Ok(S3FileChunk {
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

/// A type representing a file to be uploaded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3File {
    file_name: String,
    upload_id: Option<UploadId>,
    size: u64,
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
    /// Given a file path, this function checks to see if the path:
    ///
    /// 1) exists
    /// 2) does not contain invalid unicode symbols
    ///
    /// If neither condition hold, this function will return an error
    fn normalize<P: AsRef<Path>, Q: AsRef<Path>>(
        path: P,
        file: Q,
    ) -> bf::Result<(String, fs::Metadata)> {
        let file_path: PathBuf = path.as_ref().join(file.as_ref()).canonicalize()?;
        if !file_path.is_file() {
            return Err(bf::error::ErrorKind::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!("Not a file: {:?}", file_path),
            )).into());
        };
        if !file_path.exists() {
            return Err(bf::error::ErrorKind::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!("Could not read: {:?}", file_path),
            )).into());
        };

        // Get the full file path as a String:
        let file_name: bf::Result<String> = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| bf::error::ErrorKind::InvalidUnicodePath(file_path.clone()).into())
            .map(String::from);

        let file_name = file_name?;

        // And the resulting metadata so we can pull the file size:
        let metadata = fs::metadata(file_path)?;

        Ok((file_name, metadata))
    }

    #[allow(dead_code)]
    pub fn new<P: AsRef<Path>, Q: AsRef<Path>>(
        path: P,
        file: Q,
        upload_id: Option<UploadId>,
    ) -> bf::Result<Self> {
        let (file_name, metadata) = Self::normalize(path, file)?;
        Ok(Self {
            upload_id,
            file_name,
            size: metadata.len(),
        })
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
    pub fn file_name(&self) -> &String {
        &self.file_name
    }

    #[allow(dead_code)]
    pub fn upload_id(&self) -> Option<&UploadId> {
        self.upload_id.as_ref()
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

// A manifest generated by the legacy ETL processor (`RabbitManifest`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyManifest {
    bucket: model::S3Bucket,
    group_id: String,
    organization_id: model::OrganizationId,
    email: String,
    files: Vec<String>,
    file_group_path: String,
    destination: Option<String>,
    dataset: model::DatasetId,
    encryption_key_id: model::S3EncryptionKeyId,
    append_to_package: bool,
}

// A manifest generated by the current Nextflow ETL processor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ETLManifest {
    type_: ETLJobType,
    content: ETLJob,
}

// An ETL processor job type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ETLJobType {
    Upload,
    Append,
}

// A manifest job, as generated by the Nextflow ETL processor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ETLJob {
    import_id: model::ImportId,
    // TODO: make this typed
    file_type: String,
    #[serde(deserialize_with = "model::PackageType::deserialize")]
    package_type: Option<model::PackageType>,
    uploaded_files: Vec<String>,
    upload_directory: String,
    storage_directory: String,
    encryption_key: model::S3EncryptionKeyId,
}

// See `blackfynn-app/api/src/main/scala/com/blackfynn/uploads/Manifest.scala`
/// A file upload manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ManifestEntry {
    Legacy(LegacyManifest),
    ETL(ETLManifest),
}

impl ManifestEntry {
    /// Returns a collection of uploaded files, relative to the Blackfynn S3 bucket.
    pub fn files(&self) -> &Vec<String> {
        use self::ManifestEntry::*;
        match *self {
            Legacy(ref manifest) => &manifest.files,
            ETL(ref manifest) => &manifest.content.uploaded_files,
        }
    }
}

/// A preview of a collection of files uploaded to the Blackfynn platform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackagePreview {
    package_name: String,
    #[serde(deserialize_with = "model::PackageType::deserialize")]
    package_type: Option<model::PackageType>,
    file_type: Option<String>,
    import_id: ImportId,
    files: Vec<S3File>,
    group_size: i64,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    const USE_CHUNK_SIZE: u64 = 10;

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
        assert!(result.is_ok());
        let chunks = result.unwrap();
        assert!(chunks.len() > 1);
    }
}
