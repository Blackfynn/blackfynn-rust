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
    total_expected_parts: usize,
    missing_parts: Vec<usize>,
    progress_callback: C,
}

pub struct FileChunk {
    pub bytes: Vec<u8>,
    pub checksum: Checksum,
    pub chunk_number: usize,
}

impl<C: ProgressCallback> ChunkedFilePayload<C> {
    pub fn new<P>(
        import_id: ImportId,
        file_path: P,
        missing_parts: Option<FileMissingParts>,
        progress_callback: C,
    ) -> Self
    where
        P: AsRef<Path>,
    {
        Self::new_with_chunk_size(
            import_id,
            file_path,
            DEFAULT_CHUNK_SIZE_BYTES,
            missing_parts,
            progress_callback,
        )
    }

    pub fn new_with_chunk_size<P>(
        import_id: ImportId,
        file_path: P,
        chunk_size_bytes: u64,
        missing_parts: Option<FileMissingParts>,
        progress_callback: C,
    ) -> Self
    where
        P: AsRef<Path>,
    {
        // ensure missing parts are sorted
        let mut sorted_missing_parts = missing_parts
            .iter()
            .cloned()
            .map(|mp| mp.missing_parts)
            .next()
            .unwrap_or_else(|| vec![]);
        sorted_missing_parts.sort_unstable();

        let file_path = file_path.as_ref().to_path_buf();

        let file = File::open(file_path.clone()).unwrap();
        let file_size = file.metadata().unwrap().len();
        let total_expected_parts =
            (file_size as f64 / chunk_size_bytes as f64).floor() as usize + 1;

        // update the 'parts_sent' and 'bytes_sent' to reflect any
        // parts that were already sent based on missing_parts
        let (parts_sent, bytes_sent) = match missing_parts {
            Some(ref missing_parts) => {
                let parts_sent = total_expected_parts - missing_parts.missing_parts.len();
                let missing_final_chunk = missing_parts
                    .missing_parts
                    .iter()
                    .cloned()
                    .fold(0, usize::max)
                    == missing_parts.expected_total_parts;
                let bytes_sent = if missing_final_chunk {
                    parts_sent as u64 * chunk_size_bytes
                } else {
                    let final_chunk_size = file_size % chunk_size_bytes;
                    ((parts_sent - 1) as u64 * chunk_size_bytes) + final_chunk_size as u64
                };
                (parts_sent, bytes_sent)
            }
            None => (0, 0),
        };

        let payload = Self {
            import_id,
            file_path,
            file,
            chunk_size_bytes,
            bytes_sent,
            file_size,
            parts_sent,
            total_expected_parts,
            missing_parts: sorted_missing_parts,
            progress_callback,
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

impl<C> Stream for ChunkedFilePayload<C>
where
    C: 'static + ProgressCallback,
{
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
        } else if self.total_expected_parts == self.parts_sent {
            Ok(Ready(None))
        } else {
            let mut buffer = vec![0; self.chunk_size_bytes as usize];

            // if there are missing parts, only seek to those chunks
            let mut seek_from_chunk_number = if self.missing_parts.is_empty() {
                self.parts_sent
            } else {
                self.missing_parts[((self.parts_sent as isize - self.total_expected_parts as isize)
                    + self.missing_parts.len() as isize)
                    as usize]
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

    // this file should be big enough to provide at least 3 chunks at
    // DEFAULT_CHUNK_SIZE_BYTES
    fn test_file() -> String {
        client::tests::BIG_TEST_FILES[0].clone()
    }

    fn test_file_path() -> PathBuf {
        let mut test_file_path =
            path::Path::new(&client::tests::BIG_TEST_DATA_DIR.to_string()).to_path_buf();
        test_file_path.push(test_file());
        test_file_path
    }

    fn progress_indicator() -> client::tests::ProgressIndicator {
        client::tests::ProgressIndicator::new()
    }

    fn chunked_payload() -> ChunkedFilePayload<client::tests::ProgressIndicator> {
        ChunkedFilePayload::new(
            ImportId::new("import id"),
            test_file_path(),
            None,
            progress_indicator(),
        )
    }

    fn chunked_payload_missing_parts(
        missing_parts: FileMissingParts,
    ) -> ChunkedFilePayload<client::tests::ProgressIndicator> {
        ChunkedFilePayload::new(
            ImportId::new("import id"),
            test_file_path(),
            Some(missing_parts),
            progress_indicator(),
        )
    }

    fn chunks(
        payload: &mut ChunkedFilePayload<client::tests::ProgressIndicator>,
    ) -> Vec<FileChunk> {
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
            file_name: test_file(),
            missing_parts: vec![2, 1],
            expected_total_parts: 8,
        };

        let chunked_payload = chunked_payload_missing_parts(missing_parts);

        assert!(chunked_payload.missing_parts == vec![0, 1]);
    }

    #[test]
    fn parts_and_bytes_sent_are_calculated_for_missing_parts_file_ending() {
        let missing_parts = FileMissingParts {
            file_name: test_file(),
            missing_parts: vec![2, 1],
            expected_total_parts: 8,
        };

        let chunked_payload = chunked_payload_missing_parts(missing_parts);

        assert!(chunked_payload.parts_sent == 6);
        assert!(
            chunked_payload.bytes_sent
                == (chunked_payload.chunk_size_bytes * 5)
                    + (chunked_payload.file_size % chunked_payload.chunk_size_bytes)
        );
    }

    #[test]
    fn file_size_is_calculated_for_missing_parts() {
        let missing_parts = FileMissingParts {
            file_name: test_file(),
            missing_parts: vec![7, 8],
            expected_total_parts: 8,
        };
        let chunked_payload = chunked_payload_missing_parts(missing_parts);

        assert!(chunked_payload.parts_sent == 6);
        assert!(chunked_payload.bytes_sent == (chunked_payload.chunk_size_bytes * 6));
    }

    #[test]
    fn only_missing_parts_are_sent() {
        let missing_parts = FileMissingParts {
            file_name: test_file(),
            missing_parts: vec![3, 4, 5, 8],
            expected_total_parts: 8,
        };

        let mut chunked_payload = chunked_payload_missing_parts(missing_parts);

        let chunks = chunks(chunked_payload.by_ref());
        assert!(chunks.len() == 4);
    }
}
