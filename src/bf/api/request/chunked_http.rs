// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use bf::api::client::progress::{ProgressCallback, ProgressUpdate};
use bf::model::ImportId;

// 1MB
const DEFAULT_CHUNK_SIZE_BYTES: u64 = 1000 * 1000;

pub struct ChunkedFilePayload<C: ProgressCallback> {
    import_id: ImportId,
    file_path: PathBuf,
    file: File,
    chunk_size_bytes: u64,
    bytes_sent: u64,
    file_size: u64,
    parts_sent: usize,
    pub total_chunks: usize,
    progress_callback: C,
}

impl<C: ProgressCallback> ChunkedFilePayload<C> {
    pub fn new<P>(
        import_id: ImportId,
        file_path: P,
        progress_callback: C,
    ) -> Self
    where
        P: AsRef<Path>,
    {
        Self::new_with_chunk_size(
            import_id,
            file_path,
            DEFAULT_CHUNK_SIZE_BYTES,
            progress_callback,
        )
    }

    pub fn new_with_chunk_size<P>(
        import_id: ImportId,
        file_path: P,
        chunk_size_bytes: u64,
        progress_callback: C,
    ) -> Self
    where
        P: AsRef<Path>,
    {
        let file_path = file_path.as_ref().to_path_buf();

        let file = File::open(file_path.clone()).unwrap();
        let file_size = file.metadata().unwrap().len();

        let total_chunks = file_size as f64/chunk_size_bytes as f64;

        Self {
            import_id,
            file_path,
            file,
            chunk_size_bytes,
            bytes_sent: 0,
            file_size,
            parts_sent: 0,
            total_chunks: total_chunks.ceil() as usize,
            progress_callback,
        }
    }
}

impl<C> Iterator for ChunkedFilePayload<C>
where
    C: 'static + ProgressCallback,
{
    type Item = Result<hyper::Chunk, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = vec![0; self.chunk_size_bytes as usize];

        let chunk = self
            .file
            .seek(SeekFrom::Start(
                self.parts_sent as u64 * self.chunk_size_bytes,
            )).and_then(|_| self.file.read(&mut buffer))
            .map(|bytes_read| {
                if bytes_read > 0 {
                    self.parts_sent += 1;
                    self.bytes_sent += bytes_read as u64;

                    let progress_update = ProgressUpdate::new(
                        self.parts_sent,
                        self.import_id.clone(),
                        self.file_path.clone(),
                        self.bytes_sent,
                        self.file_size,
                    );
                    self.progress_callback.on_update(&progress_update);

                    buffer.truncate(bytes_read);

                    Some(hyper::Chunk::from(buffer))
                } else {
                    None
                }
            });

        match chunk {
            Ok(Some(chunk)) => Some(Ok(chunk)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}
