// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use futures::Async::Ready;
use sha2::{Digest, Sha256};
use tokio::prelude::{Async, Stream};

use bf::api::client::progress::{ProgressCallback, ProgressUpdate};
use bf::api::response::FileMissingParts;
use bf::model::upload::Checksum;
use bf::model::ImportId;

// 5MiB (the minimum part size for s3 multipart requests)
const DEFAULT_CHUNK_SIZE_BYTES: u64 = 5_242_880;

// SHA256 hash of an empty byte array
const EMPTY_SHA256_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

pub struct ChunkedFilePayload {
    import_id: ImportId,
    file_path: PathBuf,
    file: File,
    chunk_size_bytes: u64,
    bytes_sent: u64,
    file_size: u64,
    parts_sent: usize,
    expected_total_parts: Option<usize>,
    missing_parts: Vec<usize>,
    progress_callback: Box<dyn ProgressCallback>,
}

pub struct FileChunk {
    pub bytes: Vec<u8>,
    pub checksum: Checksum,
    pub chunk_number: usize,
}

impl ChunkedFilePayload {
    pub fn new<P, C>(
        import_id: ImportId,
        file_path: P,
        missing_parts: Option<&FileMissingParts>,
        progress_callback: C,
    ) -> Self
    where
        P: AsRef<Path>,
        C: 'static + ProgressCallback,
    {
        Self::new_with_chunk_size(
            import_id,
            file_path,
            DEFAULT_CHUNK_SIZE_BYTES,
            missing_parts,
            progress_callback,
        )
    }

    pub fn new_with_chunk_size<P, C>(
        import_id: ImportId,
        file_path: P,
        chunk_size_bytes: u64,
        missing_parts: Option<&FileMissingParts>,
        progress_callback: C,
    ) -> Self
    where
        P: AsRef<Path>,
        C: 'static + ProgressCallback,
    {
        // ensure missing parts are sorted
        let mut sorted_missing_parts = missing_parts
            .iter()
            .map(|mp| mp.missing_parts.clone())
            .next()
            .unwrap_or_else(|| vec![]);
        sorted_missing_parts.sort_unstable();

        let file_path = file_path.as_ref().to_path_buf();

        let file = File::open(file_path.clone()).unwrap();
        let file_size = file.metadata().unwrap().len();

        // update the 'parts_sent' and 'bytes_sent' to reflect any
        // parts that were already sent based on missing_parts
        let (parts_sent, bytes_sent, expected_total_parts) = match missing_parts {
            Some(ref missing_parts) => {
                let parts_sent =
                    missing_parts.expected_total_parts - missing_parts.missing_parts.len();
                let missing_final_chunk = missing_parts
                    .missing_parts
                    .iter()
                    .cloned()
                    .fold(0, usize::max)
                    == missing_parts.expected_total_parts - 1;
                let bytes_sent = if missing_final_chunk {
                    parts_sent as u64 * chunk_size_bytes
                } else {
                    let final_chunk_size = file_size % chunk_size_bytes;
                    ((parts_sent - 1) as u64 * chunk_size_bytes) + final_chunk_size as u64
                };
                (
                    parts_sent,
                    bytes_sent,
                    Some(missing_parts.expected_total_parts),
                )
            }
            None => (0, 0, None),
        };

        let payload = Self {
            import_id,
            file_path,
            file,
            chunk_size_bytes,
            bytes_sent,
            file_size,
            parts_sent,
            expected_total_parts,
            missing_parts: sorted_missing_parts,
            progress_callback: Box::new(progress_callback),
        };

        payload.update_progress_callback();

        payload
    }

    fn update_progress_callback(&self) {
        // initialize progress_callback with percentage
        let progress_update = ProgressUpdate::new(
            self.parts_sent,
            self.import_id.clone(),
            self.file_path.clone(),
            self.bytes_sent,
            self.file_size,
        );
        self.progress_callback.on_update(&progress_update);
    }
}

impl Stream for ChunkedFilePayload {
    type Item = FileChunk;
    type Error = io::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        let chunk = if self.file_size == 0 {
            // When the file size is 0, our iterator just needs to
            // send a single element with an empty buffer
            if self.parts_sent == 0 {
                self.parts_sent += 1;
                Ok(Ready(Some(FileChunk {
                    bytes: vec![],
                    checksum: Checksum(String::from(EMPTY_SHA256_HASH)),
                    chunk_number: self.parts_sent,
                })))
            } else {
                Ok(Ready(None))
            }
        } else if self.expected_total_parts == Some(self.parts_sent) {
            Ok(Ready(None))
        } else {
            let mut buffer = vec![0; self.chunk_size_bytes as usize];

            // if expected_total_parts is not defined, the upload
            // service has not given any information about this
            // upload.  by default, assume all chunks are required.
            let mut seek_from_chunk_number = match self.expected_total_parts {
                None => self.parts_sent,
                Some(expected_total_parts) => {
                    if self.missing_parts.is_empty() {
                        self.parts_sent
                    } else {
                        self.missing_parts[((self.parts_sent as isize
                            - expected_total_parts as isize)
                            + self.missing_parts.len() as isize)
                            as usize]
                    }
                }
            };

            self.file
                .seek(SeekFrom::Start(
                    seek_from_chunk_number as u64 * self.chunk_size_bytes,
                ))
                .and_then(|_| self.file.read(&mut buffer))
                .map(|bytes_read| {
                    if bytes_read > 0 {
                        self.bytes_sent += bytes_read as u64;

                        buffer.truncate(bytes_read);

                        let mut sha256_hasher = Sha256::new();
                        sha256_hasher.input(&buffer);

                        self.parts_sent += 1;

                        Ready(Some(FileChunk {
                            bytes: buffer,
                            checksum: Checksum(format!("{:x}", sha256_hasher.result())),
                            chunk_number: seek_from_chunk_number,
                        }))
                    } else {
                        Ready(None)
                    }
                })
        };

        self.update_progress_callback();

        chunk
    }
}

#[cfg(test)]
mod tests {
    use std::path;

    use super::*;
    use bf::api::client;

    use futures::Future;

    const TEST_FILE_NAME: &str = "earth.jpg";

    fn test_file_path() -> PathBuf {
        let mut test_file_path =
            path::Path::new(&client::tests::MEDIUM_TEST_DATA_DIR.to_string()).to_path_buf();
        test_file_path.push(TEST_FILE_NAME);
        test_file_path
    }

    fn progress_indicator() -> client::tests::ProgressIndicator {
        client::tests::ProgressIndicator::new()
    }

    fn chunked_payload() -> ChunkedFilePayload {
        ChunkedFilePayload::new_with_chunk_size(
            ImportId::new("import id"),
            test_file_path(),
            1000 * 1000, // 1mb
            None,
            progress_indicator(),
        )
    }

    fn chunked_payload_missing_parts(missing_parts: &FileMissingParts) -> ChunkedFilePayload {
        ChunkedFilePayload::new_with_chunk_size(
            ImportId::new("import id"),
            test_file_path(),
            1000 * 1000, // 1mb
            Some(missing_parts),
            progress_indicator(),
        )
    }

    fn chunks(payload: &mut ChunkedFilePayload) -> Vec<FileChunk> {
        payload.collect().wait().unwrap()
    }

    #[test]
    fn actual_chunk_sizes_are_correct() {
        let mut chunked_payload = chunked_payload();

        let chunks = chunks(chunked_payload.by_ref());
        let (last, all_but_last) = chunks.split_last().unwrap();

        assert!(all_but_last
            .iter()
            .all(|c| c.bytes.len() as u64 == chunked_payload.chunk_size_bytes));
        assert!(
            last.bytes.len() as u64 == chunked_payload.file_size % chunked_payload.chunk_size_bytes
        );
    }

    #[test]
    fn chunk_numbers_are_correct() {
        let mut chunked_payload = chunked_payload();
        let chunks = chunks(chunked_payload.by_ref());

        assert!(chunks
            .iter()
            .enumerate()
            .all(|(num, c)| c.chunk_number == num));
    }

    #[test]
    fn bytes_sent_is_updated() {
        let mut chunked_payload = chunked_payload();
        assert!(chunked_payload.bytes_sent == 0);

        chunked_payload.poll().unwrap();
        assert!(chunked_payload.bytes_sent == chunked_payload.chunk_size_bytes);

        chunked_payload.poll().unwrap();
        assert!(chunked_payload.bytes_sent == chunked_payload.chunk_size_bytes * 2);

        chunks(chunked_payload.by_ref());
        assert!(chunked_payload.bytes_sent == chunked_payload.file_size);
    }

    #[test]
    fn parts_sent_is_updated() {
        let mut chunked_payload = chunked_payload();
        assert!(chunked_payload.parts_sent == 0);

        chunked_payload.poll().unwrap();
        assert!(chunked_payload.parts_sent == 1);

        chunked_payload.poll().unwrap();
        assert!(chunked_payload.parts_sent == 2);

        let expected_total_parts = (chunked_payload.file_size as f64
            / chunked_payload.chunk_size_bytes as f64)
            .ceil() as usize;

        chunks(chunked_payload.by_ref());
        assert!(chunked_payload.parts_sent == expected_total_parts);
    }

    #[test]
    fn missing_parts_are_sorted() {
        let missing_parts = FileMissingParts {
            file_name: TEST_FILE_NAME.to_string(),
            missing_parts: vec![1, 0],
            expected_total_parts: 8,
        };

        let chunked_payload = chunked_payload_missing_parts(&missing_parts);

        assert!(chunked_payload.missing_parts == vec![0, 1]);
    }

    #[test]
    fn parts_and_bytes_sent_are_calculated_for_missing_parts_file_ending() {
        let missing_parts = FileMissingParts {
            file_name: TEST_FILE_NAME.to_string(),
            missing_parts: vec![1, 0],
            expected_total_parts: 8,
        };

        let chunked_payload = chunked_payload_missing_parts(&missing_parts);

        assert!(chunked_payload.parts_sent == 6);
        assert!(
            chunked_payload.bytes_sent
                == (chunked_payload.chunk_size_bytes * 5)
                    + (chunked_payload.file_size % chunked_payload.chunk_size_bytes)
        );
    }

    #[test]
    fn parts_and_bytes_sent_are_calculated_for_missing_parts() {
        let missing_parts = FileMissingParts {
            file_name: TEST_FILE_NAME.to_string(),
            missing_parts: vec![6, 7],
            expected_total_parts: 8,
        };
        let chunked_payload = chunked_payload_missing_parts(&missing_parts);
        assert!(chunked_payload.parts_sent == 6);
        assert!(chunked_payload.bytes_sent == (chunked_payload.chunk_size_bytes * 6));
    }

    #[test]
    fn only_missing_parts_are_sent() {
        let missing_parts = FileMissingParts {
            file_name: TEST_FILE_NAME.to_string(),
            missing_parts: vec![3, 4, 5, 7],
            expected_total_parts: 8,
        };

        let mut chunked_payload = chunked_payload_missing_parts(&missing_parts);

        let chunks = chunks(chunked_payload.by_ref());
        assert!(chunks.len() == 4);
    }

    #[test]
    fn zero_byte_files_progress_is_updated_correctly() {
        use std::sync;

        struct Inner(sync::Mutex<bool>);

        impl Inner {
            pub fn new() -> Self {
                Inner(sync::Mutex::new(false))
            }
        }
        pub struct ProgressIndicator {
            inner: sync::Arc<Inner>,
        }

        impl ProgressIndicator {
            pub fn new() -> Self {
                Self {
                    inner: sync::Arc::new(Inner::new()),
                }
            }
        }

        impl ProgressCallback for ProgressIndicator {
            fn on_update(&self, _update: &ProgressUpdate) {
                if _update.part_number() == 0 {
                    assert_eq!(_update.percent_done(), 0 as f32)
                } else {
                    assert_eq!(_update.percent_done(), 100 as f32);
                }

                *self.inner.0.lock().unwrap() = true;
            }
        }

        let mut zero_byte_chunked_payload = ChunkedFilePayload::new(
            ImportId::new("import_id"),
            concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/small/empty_file").to_owned(),
            None,
            ProgressIndicator::new(),
        );

        assert!(zero_byte_chunked_payload.parts_sent == 0);

        zero_byte_chunked_payload.poll().unwrap();

        assert!(zero_byte_chunked_payload.parts_sent == 1);
    }

}
