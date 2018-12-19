// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use hyper;
use hyper::body::Payload;

use futures::Async;

use std::sync::{Arc, Mutex};
use std::fs::File;
use std::io::{Error, Read};
use std::path::{Path, PathBuf};

use bf::api::client::progress::{ProgressCallback, ProgressUpdate};
use bf::model::ImportId;

pub struct ChunkedFilePayload<C: ProgressCallback> {
    import_id: ImportId,
    file_path: PathBuf,
    file: File,
    chunk_size_bytes: usize,
    bytes_sent: u64,
    file_size: u64,
    parts_sent: usize,
    progress_callback: Arc<Mutex<C>>,
}

impl<C: ProgressCallback> ChunkedFilePayload<C> {
    pub fn new<P>(import_id: ImportId, file_path: P, progress_callback: Arc<Mutex<C>>) -> Self
    where
        P: AsRef<Path>,
    {
        let default_chunk_size_bytes = 8000;

        Self::new_with_chunk_size(
            import_id,
            file_path,
            default_chunk_size_bytes,
            progress_callback,
        )
    }

    pub fn new_with_chunk_size<P>(
        import_id: ImportId,
        file_path: P,
        chunk_size_bytes: usize,
        progress_callback: Arc<Mutex<C>>,
    ) -> Self
    where
        P: AsRef<Path>,
    {
        let file_path = file_path.as_ref().to_path_buf();

        let file = File::open(file_path.clone()).unwrap();
        let file_size = file.metadata().unwrap().len();

        Self {
            import_id,
            file_path: file_path.clone(),
            file: File::open(file_path).unwrap(),
            chunk_size_bytes: chunk_size_bytes,
            bytes_sent: 0,
            file_size: file_size,
            parts_sent: 0,
            progress_callback,
        }
    }
}

impl<C> Payload for ChunkedFilePayload<C>
where C: 'static + ProgressCallback
{
    type Data = hyper::Chunk;
    type Error = Error;

    fn poll_data(&mut self) -> Result<Async<Option<Self::Data>>, Self::Error> {
        // let mut buffer = Vec::with_capacity(self.chunk_size_bytes);
        let mut buffer = vec![0; self.chunk_size_bytes];
        let read_result = self.file.read(&mut buffer);

        read_result.map(|bytes_read| {
            if bytes_read > 0 {
                self.parts_sent += 1;
                self.bytes_sent += bytes_read as u64;

                let progress_update = ProgressUpdate::new(
                    self.parts_sent,
                    true,
                    self.import_id.clone(),
                    self.file_path.clone(),
                    self.bytes_sent,
                    self.file_size,
                );
                self.progress_callback.lock().unwrap().on_update(&progress_update);

                buffer.truncate(bytes_read);
                Async::Ready(Some(hyper::Chunk::from(buffer)))
            } else {
                Async::Ready(None)
            }
        })
    }
}
