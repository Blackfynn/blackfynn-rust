// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! AWS S3-specific functionality lives here.

use std::collections::HashMap;
use std::cell::Cell;
use std::f32;
use std::iter;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::{channel, Sender, Receiver};

use futures::*;

use rusoto_core::reactor::RequestDispatcher;
use rusoto_credential::StaticProvider;
use rusoto_s3::{
    self,
    S3,
    S3Client,
};

use bf;
use bf::model;
use bf::model::{
    AccessKey,
    ImportId,
    S3Bucket,
    S3File,
    S3Key,
    S3ServerSideEncryption,
    S3UploadId,
    SecretKey,
    SessionToken,
    UploadCredential
};
use bf::util::futures::{into_future_trait, into_stream_trait};

const KB: u64 = 1024;
const MB: u64 = KB * KB;

/// The smallest part size (chunk) for a multipart upload allowed by AWS.
pub const S3_MIN_PART_SIZE: u64 = 5 * MB;

/// Create a new S3 client.
fn create_s3_client(access_key: AccessKey, secret_key: SecretKey, session_token: SessionToken) -> S3Client<StaticProvider> {
    let credentials_provider = StaticProvider::new(access_key.into(),
                                                   secret_key.into(),
                                                   Some(Into::<String>::into(session_token)),
                                                   None);
    S3Client::new(RequestDispatcher::default(), credentials_provider, Default::default())
}

/// A type encoding the possible outcomes of a multipart upload.
#[derive(Debug)]
pub enum MultipartUploadResult {
    Abort(bf::error::Error, rusoto_s3::AbortMultipartUploadOutput),
    Complete(ImportId, rusoto_s3::CompleteMultipartUploadOutput)
}

impl MultipartUploadResult {
    /// Returns true if the multipart upload was aborted.
    pub fn is_aborted(&self) -> bool {
        use self::MultipartUploadResult::*;
        match *self {
            Abort(_, _) => true,
            _ => false
        }
    }

    /// Returns true if the multipart upload was completed.
    pub fn is_completed(&self) -> bool {
        use self::MultipartUploadResult::*;
        match *self {
            Complete(_, _) => true,
            _ => false
        }
    }
}

/// An abstration of an active multipart upload to AWS S3.
pub struct MultipartUploadFile {
    s3_client: Rc<S3Client<StaticProvider>>,
    file: S3File,
    import_id: ImportId,
    upload_id: Option<S3UploadId>,
    bucket: S3Bucket,
    key: S3Key,
    server_side_encryption: S3ServerSideEncryption,
    file_chunk_size: u64,
    concurrent_limit: Cell<usize>,
    bytes_sent: Rc<Cell<u64>>,
    total_bytes_requested: Cell<u64>,
    tx_bytes: Sender<ProgressUpdate>
}

// impl Drop for MultipartUploadFile {
//     fn drop(&mut self) {
//         println!("Dropped MultipartUploadFile");
//     }
// }

impl MultipartUploadFile {
    pub fn new(s3_client: &Rc<S3Client<StaticProvider>>,
               file: S3File,
               import_id: ImportId,
               upload_id: Option<S3UploadId>,
               file_chunk_size: u64,
               bucket: S3Bucket,
               key: S3Key,
               server_side_encryption: S3ServerSideEncryption,
               tx_bytes: Sender<ProgressUpdate>) -> Self
    {
        Self {
            s3_client: Rc::clone(s3_client),
            file,
            import_id,
            upload_id,
            file_chunk_size,
            bucket,
            key,
            server_side_encryption,
            concurrent_limit: Cell::new(3),
            bytes_sent: Rc::new(Cell::new(0)),
            total_bytes_requested: Cell::new(0),
            tx_bytes
        }
    }

    /// Returns the Blackfynn import ID that the upload is part of.
    pub fn import_id(&self) -> &ImportId {
        &self.import_id
    }

    /// Returns the AWS multipart upload ID this file upload is associated with.
    pub fn upload_id(&self) -> Option<&S3UploadId> {
        self.upload_id.as_ref()
    }

    /// Returns the name of the file being uploaded.
    pub fn file_name(&self) -> &String {
        self.file.file_name()
    }

    /// Returns the size of the file in bytes.
    pub fn file_size(&self) -> Option<u64> {
        self.file.size()
    }

    /// Returns the AWS bucket the file is to be uploaded to.
    pub fn bucket(&self) -> &S3Bucket {
        &self.bucket
    }

    /// Returns the AWS key the uploaded file is stored under.
    pub fn key(&self) -> &S3Key {
        &self.key
    }

    /// Returns the server side encryption scheme that is being used by AWS.
    pub fn server_side_encryption(&self) -> &S3ServerSideEncryption {
        &self.server_side_encryption
    }

    /// Returns the file chunk size in bytes that is being used for the file.
    pub fn file_chunk_size(&self) -> u64 {
        self.file_chunk_size
    }

    /// Returns the cumulative number of bytes sent to AWS S3.
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.get()
    }

    /// Returns the total number of bytes to be sent to AWS across all
    /// pending file uploads. Every time `upload_file` is called, this value
    /// is incremented with the size of the supplied file.
    pub fn total_bytes_requested(&self) -> u64 {
        self.total_bytes_requested.get()
    }

    /// Returns the file upload concurrency limit.
    pub fn concurrent_limit(&self) -> usize {
        self.concurrent_limit.get()
    }

    /// Sets the file upload concurrency limit.
    pub fn set_concurrent_limit(&self, limit: usize) -> &Self {
        self.concurrent_limit.set(limit);
        self
    }

    /// Uploads a file to AWS S3.
    pub fn upload<P>(&self, path: P) -> bf::Stream<rusoto_s3::CompletedPart>
        where P: AsRef<Path>
    {

        if let Some(upload_id) = self.upload_id.clone() {

            let file_name = self.file_name().clone();
            let file_size = self.file_size();
            let s3_client = Rc::clone(&self.s3_client);
            let s3_bucket: model::S3Bucket = self.bucket().clone();
            let s3_key: model::S3Key = self.key().clone();
            let concurrent_limit = self.concurrent_limit();

            // Chunk the file into chunks of size `file_chunk_size`:
            let bytes_sent = Rc::clone(&self.bytes_sent);
            let tx_bytes = self.tx_bytes.clone();

            // Bump up the total number of bytes requested for upload with
            // the included file size:
            self.total_bytes_requested.replace(self.total_bytes_requested.get() + self.file.size().unwrap_or(0));

            let f = self.file.enumerate_chunks(path.as_ref(), self.file_chunk_size())
                .map(move |(part_number, chunk)| {
                    let n = chunk.len();
                    let bytes_sent = Rc::clone(&bytes_sent);
                    let request = rusoto_s3::UploadPartRequest {
                        body: Some(chunk),
                        bucket: s3_bucket.clone().into(),
                        content_length: Some(n as i64),
                        key: s3_key.clone().into(),
                        part_number: part_number as i64,
                        upload_id: upload_id.clone().into(),
                        .. Default::default()
                    };

                    let tx_bytes = tx_bytes.clone();
                    let file_name = file_name.clone();

                    s3_client.upload_part(&request)
                        .join(future::ok(part_number))
                        .map_err(Into::into)
                        .and_then(move |(part_output, part_number)| {
                            // Update the sent byte count and signal that fact.
                            // If there's a send error, ignore it:
                            bytes_sent.replace(bytes_sent.get() + (n as u64));

                            let _ = tx_bytes.send(ProgressUpdate(part_number, file_name, bytes_sent.get(), file_size));

                            // Note: parts may (read: will) complete out of order.
                            // They will be sorted later, as required by S3.
                            Ok(rusoto_s3::CompletedPart {
                                e_tag: part_output.e_tag,
                                part_number: Some(part_number as i64)
                            })
                        })
                })
                .buffer_unordered(concurrent_limit);

            into_stream_trait(f)
        } else {
            into_stream_trait(stream::once(Err(bf::error::Error::S3MissingUploadId)))
        }
    }

    /// Aborts a multipart upload.
    pub fn abort(self) -> bf::Future<rusoto_s3::AbortMultipartUploadOutput> {
        if let Some(upload_id) = self.upload_id.clone() {
            let request = rusoto_s3::AbortMultipartUploadRequest {
                upload_id: upload_id.into(),
                bucket: self.bucket().clone().into(),
                key: self.key().clone().into(),
                .. Default::default()
            };

            let f = self.s3_client.abort_multipart_upload(&request)
                .map_err(Into::into);

            into_future_trait(f)
        } else {
            into_future_trait(future::result(Err(bf::error::Error::S3MissingUploadId)))
        }
    }

    /// Completes a multipart upload.
    pub fn complete(&self, mut parts: Vec<rusoto_s3::CompletedPart>) -> bf::Future<rusoto_s3::CompleteMultipartUploadOutput>
    {
        if let Some(upload_id) = self.upload_id.clone() {
            // Parts must be sorted according to part_number, otherwise
            // S3 will reject the request:
            parts.sort_by(|a, b| a.part_number.cmp(&b.part_number));

            let request = rusoto_s3::CompleteMultipartUploadRequest {
                upload_id: upload_id.into(),
                bucket: self.bucket().clone().into(),
                key: self.key().clone().into(),
                multipart_upload: Some(rusoto_s3::CompletedMultipartUpload { parts: Some(parts) }),
                .. Default::default()
            };

            let f = self.s3_client.complete_multipart_upload(&request)
                .map_err(Into::into);

            into_future_trait(f)
        } else {
            into_future_trait(future::result(Err(bf::error::Error::S3MissingUploadId)))
        }
    }
}

/// A type representing progress updates for a multipart upload.
#[derive(Debug, Clone, Hash)]
pub struct ProgressUpdate(usize, String, u64, Option<u64>);

impl ProgressUpdate {
    /// Returns the S3 part number of the uploading file.
    pub fn part_number(&self) -> usize {
        self.0
    }

    /// Returns the name, sans path, of the file being uploaded.
    pub fn file_name(&self) -> &String {
        &self.1
    }

    /// Returns the cumulative number of bytes sent to S3 for the given file.
    pub fn bytes_sent(&self) -> u64 {
        self.2
    }

    /// Returns the total size of the file in bytes
    pub fn size(&self) -> Option<u64> {
        self.3
    }

    pub fn percent_done(&self) -> f32 {
        match self.size() {
            Some(size) => (self.bytes_sent() as f32 / size as f32) * 100.0,
            None => f32::NAN
        }
    }
}

/// A type that tracks the progress of all file being uploaded to S3.
pub struct PollUploads {
    file_stats: HashMap<String, ProgressUpdate>,
    rx_bytes: Receiver<ProgressUpdate>
}

impl PollUploads {
    pub fn new(rx_bytes: Receiver<ProgressUpdate>) -> Self {
        Self {
            file_stats: HashMap::new(),
            rx_bytes
        }
    }

    pub fn poll(&mut self) {
        while let Ok(update) = self.rx_bytes.try_recv() {
            let mut updated = false;
            {
                if let Some(stats) = self.file_stats.get_mut(update.file_name()) {
                    *stats = update.clone();
                    updated = true;
                }
            };
            if !updated {
                self.file_stats.insert(update.file_name().to_string(), update);
            }
        }
        self.file_stats.iter().for_each(|(file, update)| {
            println!("{} => {}%", file, update.percent_done());
        });
    }
}

/// A type representing a AWS S3 file uploader.
pub struct S3Uploader {
    server_side_encryption: S3ServerSideEncryption,
    s3_client: Rc<S3Client<StaticProvider>>,
    tx_bytes: Sender<ProgressUpdate>,
    rx_bytes: Option<Receiver<ProgressUpdate>>
}

// impl Drop for S3Uploader {
//     fn drop(&mut self) {
//
//     }
// }

impl S3Uploader {
    pub fn new(server_side_encryption: S3ServerSideEncryption,
               access_key: AccessKey,
               secret_key: SecretKey,
               session_token: SessionToken) -> Self
    {
        let (tx_bytes, rx_bytes) = channel::<ProgressUpdate>();
        Self {
            server_side_encryption,
            s3_client: Rc::new(create_s3_client(access_key, secret_key, session_token)),
            tx_bytes,
            rx_bytes: Some(rx_bytes)
        }
    }

    /// Produces a `futures::stream::Stream`, where each entry represents a
    /// `futures::future::Future` that uploads the specified files to the
    /// Blackfynn S3 bucket.
    pub fn upload<P>
        (&self, path: P, files: &[S3File], import_id: &ImportId, credentials: &UploadCredential) -> bf::Future<ImportId>
        where P: AsRef<Path>
    {
        let import_id = import_id.clone();
        let s3_server_side_encryption: String = self.server_side_encryption.clone().into();

        let f = stream::futures_unordered(files.iter().map(|file: &S3File| {

            let s3_client = Rc::clone(&self.s3_client);

            let s3_server_side_encryption = s3_server_side_encryption.clone();
            let s3_encryption_key_id: String = credentials.encryption_key_id().clone().into();
            let s3_bucket: model::S3Bucket = credentials.s3_bucket().clone();
            let s3_upload_key: model::S3UploadKey = credentials.s3_key().as_upload_key(&import_id, file.file_name());
            let s3_key: model::S3Key = s3_upload_key.clone().into();

            // Read the contents of the file as a byte vector and use the AWS
            // S3 Put Object Request to perform the actual upload:
            file.read_bytes(path.as_ref())
                .and_then(move |contents: Vec<u8>| {
                    let request = rusoto_s3::PutObjectRequest {
                        body: Some(contents),
                        bucket: s3_bucket.into(),
                        key: s3_key.into(),
                        ssekms_key_id: Some(s3_encryption_key_id),
                        server_side_encryption: Some(s3_server_side_encryption),
                        .. Default::default()
                    };
                    s3_client.put_object(&request).map_err(Into::into)
                })
        }))
        .into_future()
        .map_err(|(e, _)| e)
        .and_then(move |_| Ok(import_id));

        into_future_trait(f)
    }

    /// Returns a file progress poller used to update and check the number
    /// of bytes sent to S3 for all files uploaded by this `S3Uploader` instance.
    pub fn poll_uploads(&mut self) -> Result<PollUploads, ()> {
        if let Some(rx_bytes) = self.rx_bytes.take() {
            Ok(PollUploads::new(rx_bytes))
        } else {
            Err(())
        }
    }

    /// Initiates a multi-part file upload.
    /// TODO: implement retry
    pub fn begin_multipart_upload
        (&self, file: S3File, import_id: &ImportId, credentials: &UploadCredential, file_chunk_size: u64) -> bf::Future<MultipartUploadFile>
    {
        let s3_client = Rc::clone(&self.s3_client);
        let import_id = import_id.clone();

        let s3_server_side_encryption = self.server_side_encryption.clone();
        let s3_bucket: model::S3Bucket = credentials.s3_bucket().clone();
        let s3_upload_key: model::S3UploadKey = credentials.s3_key().as_upload_key(&import_id, file.file_name());
        let s3_key: model::S3Key = s3_upload_key.clone().into();

        let request = rusoto_s3::CreateMultipartUploadRequest {
            bucket: s3_bucket.clone().into(),
            key: s3_key.clone().into(),
            server_side_encryption: Some(s3_server_side_encryption.clone().into()),
            .. Default::default()
        };

        let tx_bytes = self.tx_bytes.clone();

        let f = s3_client.create_multipart_upload(&request)
            .and_then(move |output: rusoto_s3::CreateMultipartUploadOutput| {
                Ok(MultipartUploadFile::new(&s3_client,
                                            file,
                                            import_id,
                                            output.upload_id.map(Into::into),
                                            file_chunk_size,
                                            s3_bucket,
                                            s3_key,
                                            s3_server_side_encryption,
                                            tx_bytes))
            })
            .map_err(Into::into);

        into_future_trait(f)
    }

    /// Initiates a multi-part upload for a single file.
    fn multipart_upload_file<P>
        (&self, path: P, file: S3File, import_id: &ImportId, credentials: &UploadCredential, chunk_size: u64) -> bf::Future<MultipartUploadResult>
        where P: 'static + AsRef<Path>
    {
        let file = file.clone();
        let import_id = import_id.clone();

        let f = self.begin_multipart_upload(file, &import_id, credentials, chunk_size).join(future::ok(path))
           .and_then(move |(multipart, path): (MultipartUploadFile, P)| {
                // Divide the file into parts of size `chunk_size`, and upload each part.
                multipart.upload(path)
                    .collect()
                    .then(move |result: bf::Result<Vec<rusoto_s3::CompletedPart>>| {
                        match result {
                            // if all of the parts were received successfully, attempt to complete it:
                            Ok(parts) => {
                                let f = multipart
                                    .complete(parts).map(|output| MultipartUploadResult::Complete(import_id, output))
                                    .or_else(move |err| multipart.abort().map(|output| MultipartUploadResult::Abort(err, output)));

                                into_future_trait(f)
                            },
                            // otherwise, abort the whole upload:
                            Err(err) => {
                                let f = multipart
                                    .abort()
                                    .map(|output| MultipartUploadResult::Abort(err, output))
                                    .or_else(Err);

                                into_future_trait(f)
                            }
                        }
                    })
           });

        into_future_trait(f)
    }

    /// Initiates a multi-part upload for multiple files.
    pub fn multipart_upload_files<P>
        (&self, path: P, files: &[S3File], import_id: &ImportId, credentials: &UploadCredential, chunk_size: u64) -> bf::Stream<MultipartUploadResult>
        where P: AsRef<Path>
    {
        let futures = files.iter()
            .zip(iter::repeat(path.as_ref().to_path_buf()))
            .map(move |(file, path): (&S3File, PathBuf)| {
                self.multipart_upload_file(path, file.clone(), import_id, &credentials.clone(), chunk_size)
            });

        into_stream_trait(stream::futures_unordered(futures))
    }
}
