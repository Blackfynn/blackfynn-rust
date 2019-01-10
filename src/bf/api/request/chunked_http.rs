// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use sha2::{Sha256, Digest};

use bf::api::client::progress::{ProgressCallback, ProgressUpdate};
use bf::model::ImportId;
use bf::model::upload::Checksum;

// 1MB
const DEFAULT_CHUNK_SIZE_BYTES: u64 = 1000 * 1000;

// SHA256 hash of an empty byte array
const EMPTY_SHA256_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

pub struct ChunkedFilePayload<C: ProgressCallback> {
    import_id: ImportId,
    file_path: PathBuf,
    file: File,
    chunk_size_bytes: u64,
    bytes_sent: u64,
    file_size: u64,
    parts_sent: usize,
    progress_callback: C,
}

pub struct FileChunk {
    pub bytes: Vec<u8>,
    pub checksum: Checksum
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

        Self {
            import_id,
            file_path,
            file,
            chunk_size_bytes,
            bytes_sent: 0,
            file_size,
            parts_sent: 0,
            progress_callback,
        }
    }
}

impl<C> Iterator for ChunkedFilePayload<C>
where
    C: 'static + ProgressCallback,
{
    type Item = Result<FileChunk, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let chunk = if self.file_size == 0 {
            // When the file size is 0, our iterator just needs to
            // send a single element with an empty buffer
            if self.parts_sent == 0 {
                Ok(Some(FileChunk {
                    bytes: vec![],
                    checksum: Checksum(String::from(EMPTY_SHA256_HASH))
                }))
            } else {
                Ok(None)
            }
        } else {
            let mut buffer = vec![0; self.chunk_size_bytes as usize];

            self.file
                .seek(SeekFrom::Start(
                    self.parts_sent as u64 * self.chunk_size_bytes,
                )).and_then(|_| self.file.read(&mut buffer))
                .map(|bytes_read| {
                    if bytes_read > 0 {
                        self.bytes_sent += bytes_read as u64;

                        buffer.truncate(bytes_read);

                        let mut sha256_hasher = Sha256::new();
                        sha256_hasher.input(&buffer);

                        Some(FileChunk {
                            bytes: buffer,
                            checksum: Checksum(format!("{:x}", sha256_hasher.result()))
                        })
                    } else {
                        None
                    }
                })
        };

        self.parts_sent += 1;

        let progress_update = ProgressUpdate::new(
            self.parts_sent,
            self.import_id.clone(),
            self.file_path.clone(),
            self.bytes_sent,
            self.file_size,
        );
        self.progress_callback.on_update(&progress_update);

        match chunk {
            Ok(Some(chunk)) => Some(Ok(chunk)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}
