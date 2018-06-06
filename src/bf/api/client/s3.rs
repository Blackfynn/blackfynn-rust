// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! AWS S3-specific functionality lives here.

use std::cell::Cell;
use std::collections::hash_map;
use std::iter;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};

use futures::*;

use rusoto_core::reactor::RequestDispatcher;
use rusoto_credential::StaticProvider;
use rusoto_s3::{self, S3, S3Client};

use bf;
use bf::model;
use bf::model::{AccessKey, ImportId, S3Bucket, S3File, S3Key, S3ServerSideEncryption, S3UploadId,
                SecretKey, SessionToken, UploadCredential};
use bf::util::futures::{into_future_trait, into_stream_trait};

const KB: u64 = 1024;
const MB: u64 = KB * KB;
const DEFAULT_CONCURRENCY_LIMIT: usize = 4;

/// The smallest part size (chunk, in bytes) for a multipart upload allowed by AWS.
pub const S3_MIN_PART_SIZE: u64 = 5 * MB;

/// Create a new S3 client.
fn create_s3_client(
    access_key: AccessKey,
    secret_key: SecretKey,
    session_token: SessionToken,
) -> S3Client<StaticProvider> {
    let credentials_provider = StaticProvider::new(
        access_key.into(),
        secret_key.into(),
        Some(Into::<String>::into(session_token)),
        None,
    );
    S3Client::new(
        RequestDispatcher::default(),
        credentials_provider,
        Default::default(),
    )
}

/// The possible outcomes of a multipart upload.
#[derive(Debug)]
pub enum MultipartUploadResult {
    Abort(bf::error::Error, rusoto_s3::AbortMultipartUploadOutput),
    Complete(ImportId, rusoto_s3::CompleteMultipartUploadOutput),
}

impl MultipartUploadResult {
    /// Returns true if the multipart upload was aborted.
    pub fn is_aborted(&self) -> bool {
        use self::MultipartUploadResult::*;
        match *self {
            Abort(_, _) => true,
            _ => false,
        }
    }

    /// Returns true if the multipart upload was completed.
    pub fn is_completed(&self) -> bool {
        use self::MultipartUploadResult::*;
        match *self {
            Complete(_, _) => true,
            _ => false,
        }
    }
}

/// An abstration of an active multipart upload to AWS S3.
struct MultipartUploadFile<C: ProgressCallback> {
    s3_client: Arc<S3Client<StaticProvider>>,
    file: S3File,
    import_id: ImportId,
    upload_id: Option<S3UploadId>,
    bucket: S3Bucket,
    key: S3Key,
    server_side_encryption: S3ServerSideEncryption,
    file_chunk_size: u64,
    concurrent_limit: usize,
    bytes_sent: Arc<Mutex<u64>>,
    total_bytes_requested: Cell<u64>,
    tx_progress: Sender<ProgressUpdate>,
    cb: Arc<Mutex<C>>,
}

impl <C> MultipartUploadFile<C> where C: 'static + ProgressCallback {
    #[allow(unknown_lints, too_many_arguments)]
    fn new(
        s3_client: &Arc<S3Client<StaticProvider>>,
        file: S3File,
        import_id: ImportId,
        upload_id: Option<S3UploadId>,
        file_chunk_size: u64,
        bucket: S3Bucket,
        key: S3Key,
        server_side_encryption: S3ServerSideEncryption,
        tx_progress: Sender<ProgressUpdate>,
        cb: C,
    ) -> Self {
        Self {
            s3_client: Arc::clone(s3_client),
            file,
            import_id,
            upload_id,
            file_chunk_size,
            bucket,
            key,
            server_side_encryption,
            concurrent_limit: DEFAULT_CONCURRENCY_LIMIT,
            bytes_sent: Arc::new(Mutex::new(0)),
            total_bytes_requested: Cell::new(0),
            tx_progress,
            cb: Arc::new(Mutex::new(cb)),
        }
    }

    /// Returns the Blackfynn import ID that the upload is part of.
    #[allow(dead_code)]
    pub fn import_id(&self) -> &ImportId {
        &self.import_id
    }

    /// Returns the AWS multipart upload ID this file upload is associated with.
    #[allow(dead_code)]
    pub fn upload_id(&self) -> Option<&S3UploadId> {
        self.upload_id.as_ref()
    }

    /// Returns the name of the file being uploaded.
    #[allow(dead_code)]
    pub fn file_name(&self) -> &String {
        self.file.file_name()
    }

    /// Returns the size of the file in bytes.
    #[allow(dead_code)]
    pub fn file_size(&self) -> u64 {
        self.file.size()
    }

    /// Returns the AWS bucket the file is to be uploaded to.
    #[allow(dead_code)]
    pub fn bucket(&self) -> &S3Bucket {
        &self.bucket
    }

    /// Returns the AWS key the uploaded file is stored under.
    #[allow(dead_code)]
    pub fn key(&self) -> &S3Key {
        &self.key
    }

    /// Returns the server side encryption scheme that is being used by AWS.
    #[allow(dead_code)]
    pub fn server_side_encryption(&self) -> &S3ServerSideEncryption {
        &self.server_side_encryption
    }

    /// Returns the file chunk size in bytes that is being used for the file.
    #[allow(dead_code)]
    pub fn file_chunk_size(&self) -> u64 {
        self.file_chunk_size
    }

    /// Returns the total number of bytes to be sent to AWS across all
    /// pending file uploads. Every time `upload_file` is called, this value
    /// is incremented with the size of the supplied file.
    #[allow(dead_code)]
    pub fn total_bytes_requested(&self) -> u64 {
        self.total_bytes_requested.get()
    }

    /// Uploads a file's parts to an AWS S3 bucket.
    pub fn upload_parts<P>(&self, path: P) -> bf::Stream<rusoto_s3::CompletedPart>
    where
        P: 'static + AsRef<Path>,
    {
        if let Some(upload_id) = self.upload_id.clone() {

            let cb = Arc::clone(&self.cb);
            let import_id = self.import_id().clone();
            let file_path = path.as_ref().to_path_buf().join(self.file_name());
            let file_size = self.file_size();
            let s3_client = Arc::clone(&self.s3_client);
            let s3_bucket: model::S3Bucket = self.bucket().clone();
            let s3_key: model::S3Key = self.key().clone();
            let concurrent_limit = self.concurrent_limit;

            // Divide the file into chunks of size `file_chunk_size`:
            let bytes_sent = Arc::clone(&self.bytes_sent);
            let tx_progress = self.tx_progress.clone();

            // Bump up the total number of bytes requested for upload with
            // the included file size:
            self.total_bytes_requested.replace(self.total_bytes_requested.get() + self.file.size());

            let f = self.file.chunks(path.as_ref(), self.file_chunk_size())
                .map(move |mut chunk| {
                    let bytes = match chunk.read() {
                        Ok(bytes) => bytes,
                        Err(e) => return into_future_trait(future::err(e))
                    };
                    let n = bytes.len();
                    let part_number = chunk.part_number();
                    let bytes_sent = Arc::clone(&bytes_sent);

                    let request = rusoto_s3::UploadPartRequest {
                        body: Some(bytes),
                        bucket: s3_bucket.clone().into(),
                        content_length: Some(n as i64),
                        key: s3_key.clone().into(),
                        part_number: part_number as i64,
                        upload_id: upload_id.clone().into(),
                        .. Default::default()
                    };

                    let cb = Arc::clone(&cb);
                    let tx_progress = tx_progress.clone();
                    let s3_client = Arc::clone(&s3_client);
                    let file_path = file_path.clone();
                    let import_id = import_id.clone();

                    let f = future::lazy(move || {
                        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        // TODO: REMOVE sync() after rusoto `RusotoFuture` implements Send!
                        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        s3_client.upload_part(&request)
                            .sync()
                            .into_future()
                            .map(move |output| (output, part_number))
                            .map_err(|e| bf::Error::with_chain(e, "bf:api:s3:upload parts"))
                            .and_then(move |(part_output, part_number)| {

                                // Update the sent byte count and signal the fact.
                                // If there's a send error, ignore it:
                                let mut bytes_sent_ref = bytes_sent.lock().unwrap();
                                *bytes_sent_ref += n as u64;
                                let updated_bytes_sent: u64 = *bytes_sent_ref;

                                let update = ProgressUpdate::new(part_number as usize,
                                    true,
                                    import_id,
                                    file_path,
                                    updated_bytes_sent,
                                    file_size);

                                let progress = cb.lock().unwrap();

                                // Call the provided progress callback with the update:
                                progress.on_update(&update);

                                // and send the actual update information to the progress update
                                // channel:
                                let _ = tx_progress.send(update);

                                // Note: parts may (read: will) complete out of order.
                                // They will be sorted later, as required by S3.
                                Ok(rusoto_s3::CompletedPart {
                                    e_tag: part_output.e_tag,
                                    part_number: Some(part_number as i64)
                                })
                            })
                    });

                    into_future_trait(f)
                })
                .buffer_unordered(concurrent_limit);

            into_stream_trait(f)

        } else {
            into_stream_trait(stream::once(
                Err(bf::error::ErrorKind::S3MissingUploadId.into()),
            ))
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
            // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            // TODO: REMOVE sync() after rusoto `RusotoFuture` implements Send!
            // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            let f = self.s3_client
                .abort_multipart_upload(&request)
                .sync()
                .into_future()
                .map_err(|e| bf::Error::with_chain(e, "bf:api:s3:multipart upload abort"));

            into_future_trait(f)
        } else {
            into_future_trait(Err(bf::error::ErrorKind::S3MissingUploadId.into()).into_future())
        }
    }

    /// Completes a multipart upload.
    pub fn complete(
        &self,
        mut parts: Vec<rusoto_s3::CompletedPart>,
    ) -> bf::Future<rusoto_s3::CompleteMultipartUploadOutput> {
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

            // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            // TODO: REMOVE sync() after rusoto `RusotoFuture` implements Send!
            // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            let f = self.s3_client
                .complete_multipart_upload(&request)
                .sync()
                .into_future()
                .map_err(|e| bf::Error::with_chain(e, "bf:api:s3:multipart upload complete"));

            into_future_trait(f)
        } else {
            into_future_trait(Err(bf::error::ErrorKind::S3MissingUploadId.into()).into_future())
        }
    }
}

/// A trait defining a progress indicator callback. Every time a file part
/// successfully completes, `update` will be called with new, update statistics
/// for the file.
pub trait ProgressCallback: Clone + Send {
    /// Called when an uploaded progress update occurs.
    fn on_update(&self, &ProgressUpdate);
}

/// An implementation of `ProgressCallback` that does nothing.
#[derive(Debug, Clone, Hash)]
struct NoProgress;

impl ProgressCallback for NoProgress {
    fn on_update(&self, _update: &ProgressUpdate) {
        // Do nothing
    }
}

/// A type representing progress updates for a multipart upload.
#[derive(Debug, Clone, Hash)]
pub struct ProgressUpdate {
    part_number: usize,
    is_multipart: bool,
    import_id: ImportId,
    file_path: PathBuf,
    bytes_sent: u64,
    size: u64,
}

impl ProgressUpdate {
    pub fn new(
        part_number: usize,
        is_multipart: bool,
        import_id: ImportId,
        file_path: PathBuf,
        bytes_sent: u64,
        size: u64,
    ) -> Self {
        Self {
            part_number,
            is_multipart,
            import_id,
            file_path,
            bytes_sent,
            size,
        }
    }

    /// Returns whether the file was uploaded as a multipart upload.
    pub fn is_multipart(&self) -> bool {
        self.is_multipart
    }

    /// Returns the S3 part number of the uploading file.
    pub fn part_number(&self) -> usize {
        self.part_number
    }

    /// Returns the Blackfynn import ID the file is associated with.
    pub fn import_id(&self) -> &ImportId {
        &self.import_id
    }

    /// Returns the name, sans path, of the file being uploaded.
    pub fn file_path(&self) -> &Path {
        self.file_path.as_ref()
    }

    /// Returns the cumulative number of bytes sent to S3 for the given file.
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent
    }

    /// Returns the total size of the file in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns the upload percentage completed.
    pub fn percent_done(&self) -> f32 {
        (self.bytes_sent() as f32 / self.size() as f32) * 100.0
    }

    /// Tests if the file completed uploading.
    pub fn completed(&self) -> bool {
        self.percent_done() >= 100.0
    }
}

/// Tracks the progress of all files being uploaded to S3.
pub struct UploadProgress {
    file_stats: hash_map::HashMap<PathBuf, ProgressUpdate>,
    rx_progress: Receiver<ProgressUpdate>,
}

/// An iterator over file upload progress updates.
pub struct UploadProgressIter<'a> {
    #[allow(dead_code)]
    iter: hash_map::Iter<'a, PathBuf, ProgressUpdate>,
}

impl<'a> Iterator for UploadProgressIter<'a> {
    type Item = (&'a Path, &'a ProgressUpdate);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(p, u)| (p.as_ref(), u))
    }
}

impl<'a> IntoIterator for &'a mut UploadProgress {
    type Item = (&'a Path, &'a ProgressUpdate);
    type IntoIter = UploadProgressIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl UploadProgress {
    pub fn new(rx_progress: Receiver<ProgressUpdate>) -> Self {
        Self {
            file_stats: hash_map::HashMap::new(),
            rx_progress,
        }
    }

    /// This updates the number of bytes written to S3 for each file being
    /// uploaded. This function is non-blocking, and byte countes will _only_
    /// be updated when this method is called.
    pub fn update(&mut self) {
        while let Ok(update) = self.rx_progress.try_recv() {
            let mut updated = false;
            if let Some(stats) = self.file_stats.get_mut(update.file_path()) {
                *stats = update.clone();
                updated = true;
            }
            if !updated {
                self.file_stats
                    .insert(update.file_path().to_owned(), update);
            }
        }
    }

    /// Returns an iterator over file upload progress updates.
    pub fn iter(&mut self) -> UploadProgressIter {
        self.update();
        UploadProgressIter {
            iter: self.file_stats.iter(),
        }
    }
}

/// An AWS S3 file uploader.
pub struct S3Uploader {
    server_side_encryption: S3ServerSideEncryption,
    s3_client: Arc<S3Client<StaticProvider>>,
    tx_progress: Sender<ProgressUpdate>,
    rx_progress: Option<Receiver<ProgressUpdate>>,
    file_chunk_size: u64,
}

impl S3Uploader {
    pub fn new(
        server_side_encryption: S3ServerSideEncryption,
        access_key: AccessKey,
        secret_key: SecretKey,
        session_token: SessionToken,
    ) -> Self {
        let (tx_progress, rx_progress) = channel::<ProgressUpdate>();
        Self {
            server_side_encryption,
            s3_client: Arc::new(create_s3_client(access_key, secret_key, session_token)),
            tx_progress,
            rx_progress: Some(rx_progress),
            file_chunk_size: S3_MIN_PART_SIZE,
        }
    }

    /// Returns a file uploade progress poller.
    pub fn progress(&mut self) -> Result<UploadProgress, ()> {
        if let Some(rx_progress) = self.rx_progress.take() {
            Ok(UploadProgress::new(rx_progress))
        } else {
            Err(())
        }
    }

    /// Set the file chunk size to be used when uploading to AWS S3.
    pub fn set_file_chunk_size(&mut self, file_chunk_size: u64) -> &Self {
        self.file_chunk_size = file_chunk_size;
        self
    }

    /// Like [`upload_cb`](#method.upload_cb), but does not take a `ProgressCallback` instance.
    pub fn upload<P>(
        &self,
        path: P,
        files: Vec<S3File>,
        import_id: ImportId,
        credentials: UploadCredential,
    ) -> bf::Stream<ImportId>
    where
        P: 'static + AsRef<Path>,
    {
        self.upload_cb(path, files, import_id, credentials, NoProgress)
    }

    /// Upload files to S3. Files with sizes below the AWS multipart chunk
    /// size threshold will be uploaded using `PutObjectRequest`, otherwise
    /// multipart uploading will be used.
    pub fn upload_cb<C, P>(
        &self,
        path: P,
        files: Vec<S3File>,
        import_id: ImportId,
        credentials: UploadCredential,
        cb: C,
    ) -> bf::Stream<ImportId>
    where
        C: 'static + ProgressCallback,
        P: 'static + AsRef<Path>,
    {
        // Divide the files into large and small groups. Large groups will be multipart uploaded.
        let (small_s3_files, large_s3_files) = files
            .into_iter()
            .partition(|file| file.size() < S3_MIN_PART_SIZE);

        let s = self.multipart_upload_files_cb(
            path.as_ref().to_path_buf(),
            &large_s3_files,
            import_id.clone(),
            credentials.clone(),
            cb.clone(),
        ).map(move |result| match result {
                MultipartUploadResult::Complete(import_id, _) => stream::once(Ok(import_id)),
                MultipartUploadResult::Abort(reason, _) => stream::once(Err(reason)),
            })
            .flatten()
            .chain(
                self.put_objects_cb(path, &small_s3_files, import_id, credentials, cb)
                    .into_stream(),
            );

        into_stream_trait(s)
    }

    /// Uploads a single file to S3 using the AWS `PutObjectRequest` interface.
    #[allow(dead_code)]
    fn put_object_cb<C, P>(
        &self,
        path: P,
        file: &S3File,
        import_id: ImportId,
        credentials: &UploadCredential,
        cb: C,
    ) -> bf::Future<ImportId>
    where
        C: 'static + ProgressCallback,
        P: 'static + AsRef<Path>,
    {
        let s3_client = Arc::clone(&self.s3_client);

        let s3_server_side_encryption: String = self.server_side_encryption.clone().into();
        let s3_encryption_key_id: String = credentials.encryption_key_id().clone().into();
        let s3_bucket: model::S3Bucket = credentials.s3_bucket().clone();
        let s3_upload_key: model::S3UploadKey = credentials
            .s3_key()
            .as_upload_key(&import_id, file.file_name());
        let s3_key: model::S3Key = s3_upload_key.clone().into();
        let file_size = file.size();
        let file_path = path.as_ref().join(file.file_name());

        // Read the contents of the file as a byte vector and use the AWS
        // S3 Put Object Request to perform the actual upload:
        let f = file.read_bytes(path.as_ref())
            .and_then(move |contents: Vec<u8>| {
                let request = rusoto_s3::PutObjectRequest {
                    body: Some(contents),
                    bucket: s3_bucket.into(),
                    key: s3_key.into(),
                    ssekms_key_id: Some(s3_encryption_key_id),
                    server_side_encryption: Some(s3_server_side_encryption),
                    ..Default::default()
                };
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // TODO: REMOVE sync() after rusoto `RusotoFuture` implements Send!
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                s3_client
                    .put_object(&request)
                    .sync()
                    .into_future()
                    .map_err(|e| bf::Error::with_chain(e, "bf:api:s3:put object"))
            })
            .and_then(move |_| {
                let update = ProgressUpdate::new(
                    1,
                    false,
                    import_id.clone(),
                    file_path,
                    file_size,
                    file_size,
                );
                cb.on_update(&update);
                Ok(import_id)
            });

        into_future_trait(f)
    }

    /// Uploads a collection of files to S3 using AWS `PutObjectRequest` interface,
    /// returning a `Future` representing the completion of the entire collection.
    #[allow(dead_code)]
    pub fn put_objects_cb<C, P>(
        &self,
        path: P,
        files: &[S3File],
        import_id: ImportId,
        credentials: UploadCredential,
        cb: C,
    ) -> bf::Future<ImportId>
    where
        C: 'static + ProgressCallback,
        P: 'static + AsRef<Path>,
    {
        let ret_import_id = import_id.clone();

        let fs = files
            .iter()
            .zip(iter::repeat(path.as_ref().to_path_buf()))
            .map(move |(file, path): (&S3File, PathBuf)| {
                self.put_object_cb(path, &file, import_id.clone(), &credentials, cb.clone())
            });

        let f = stream::futures_unordered(fs)
            .into_future()
            .map_err(|(e, _)| bf::Error::with_chain(e, "bf:api:s3:put objects"))
            .and_then(|_| Ok(ret_import_id));

        into_future_trait(f)
    }

    /// Like [`put_objects_cb`](#method.put_objects_cb), but does not take a `ProgressCallback` instance.
    #[allow(dead_code)]
    pub fn put_objects<P>(
        &self,
        path: P,
        files: &[S3File],
        import_id: ImportId,
        credentials: UploadCredential,
    ) -> bf::Future<ImportId>
    where
        P: 'static + AsRef<Path>,
    {
        self.put_objects_cb(path, files, import_id, credentials, NoProgress)
    }
    
    /// Initiates a multi-part file upload.
    fn begin_multipart_upload<C>(
        &self,
        file: S3File,
        import_id: ImportId,
        credentials: &UploadCredential,
        cb: C,
    ) -> bf::Future<MultipartUploadFile<C>>
    where
        C: 'static + ProgressCallback,
    {
        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // TODO: implement retry logic here
        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        let s3_client = Arc::clone(&self.s3_client);

        let s3_server_side_encryption = self.server_side_encryption.clone();
        let s3_bucket: model::S3Bucket = credentials.s3_bucket().clone();
        let s3_upload_key: model::S3UploadKey = credentials
            .s3_key()
            .as_upload_key(&import_id, file.file_name());
        let s3_key: model::S3Key = s3_upload_key.clone().into();

        let request = rusoto_s3::CreateMultipartUploadRequest {
            bucket: s3_bucket.clone().into(),
            key: s3_key.clone().into(),
            server_side_encryption: Some(s3_server_side_encryption.clone().into()),
            .. Default::default()
        };

        let tx_progress = self.tx_progress.clone();
        let file_chunk_size = self.file_chunk_size;

        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // TODO: REMOVE sync() after rusoto `RusotoFuture` implements Send!
        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        let f = s3_client
            .create_multipart_upload(&request)
            .sync()
            .into_future()
            .and_then(move |output: rusoto_s3::CreateMultipartUploadOutput| {
                Ok(MultipartUploadFile::new(
                    &s3_client,
                    file,
                    import_id,
                    output.upload_id.map(Into::into),
                    file_chunk_size,
                    s3_bucket,
                    s3_key,
                    s3_server_side_encryption,
                    tx_progress,
                    cb,
                ))
            })
            .map_err(|e| bf::Error::with_chain(e, "bf:api:s3:begin multipart upload"));

        into_future_trait(f)
    }

    /// Initiates a multi-part upload for a single file.
    fn multipart_upload_file<C, P>(
        &self,
        path: P,
        file: S3File,
        import_id: ImportId,
        credentials: &UploadCredential,
        cb: C,
    ) -> bf::Future<MultipartUploadResult>
    where
        C: 'static + ProgressCallback,
        P: 'static + Send + AsRef<Path>,
    {
        let f = self.begin_multipart_upload(file, import_id.clone(), &credentials, cb)
            .join(Ok(path))
            .and_then(move |(multipart, path): (MultipartUploadFile<C>, P)| {
                // Divide the file into parts of size `chunk_size`, and upload each part.
                multipart.upload_parts(path).collect().then(
                    move |result| {
                        match result {
                            // if all of the parts were received successfully, attempt to complete it:
                            Ok(parts) => {
                                into_future_trait(multipart
                                    .complete(parts)
                                    .map(|output| {
                                        MultipartUploadResult::Complete(import_id, output)
                                    })
                                    .or_else(move |err| {
                                        multipart
                                            .abort()
                                            .map(|output| MultipartUploadResult::Abort(err, output))
                                    }))
                            }
                            // otherwise, abort the whole upload:
                            Err(err) => {
                                into_future_trait(multipart
                                    .abort()
                                    .map(|output| MultipartUploadResult::Abort(err, output))
                                    .or_else(Err))
                            }
                        }
                    },
                )
            });

        into_future_trait(f)
    }

    /// Initiates a multi-part upload for multiple files with a progress
    /// indicator callback.
    pub fn multipart_upload_files_cb<C, P>(
        &self,
        path: P,
        files: &Vec<S3File>,
        import_id: ImportId,
        credentials: UploadCredential,
        cb: C,
    ) -> bf::Stream<MultipartUploadResult>
    where
        C: 'static + ProgressCallback,
        P: 'static + AsRef<Path>,
    {
        let fs = files
            .iter()
            .zip(iter::repeat(path.as_ref().to_path_buf()))
            .map(move |(file, path): (&S3File, PathBuf)| {
                self.multipart_upload_file(
                    path,
                    file.clone(),
                    import_id.clone(),
                    &credentials,
                    cb.clone(),
                )
            });

        into_stream_trait(stream::futures_unordered(fs))
    }

    /// Initiates a multi-part upload for multiple files.
    pub fn multipart_upload_files<P>(
        &self,
        path: P,
        files: &Vec<S3File>,
        import_id: ImportId,
        credentials: UploadCredential,
    ) -> bf::Stream<MultipartUploadResult>
    where
        P: 'static + AsRef<Path>,
    {
        self.multipart_upload_files_cb(path, files, import_id, credentials, NoProgress)
    }
}
