// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! IO-related utility code lives here.

use std::io::{Bytes, Read};

/// Given a type that implements `std::io::Read`, returns an iterator over
/// byte chunks of a given size.
pub fn byte_chunks<R: Read>(readable: R, chunk_size: u64) -> ByteChunks<R> {
    ByteChunks {
        byte_stream: readable.bytes(),
        chunk_size
    }
}

/// An iterator to over byte chunks of a file.
pub struct ByteChunks<R> {
    byte_stream: Bytes<R>,
    chunk_size: u64
}

impl <R: Read> Iterator for ByteChunks<R> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        // See http://xion.io/post/code/rust-iter-patterns.html for turning
        // `Vec<Result<u8, Error>>` to `Result<Vec<u8>, Error>`
        self.byte_stream
            .by_ref()
            .take(self.chunk_size as usize)
            .collect::<Result<Vec<_>, _>>()
            .ok()
            .and_then(|bytes| if bytes.is_empty() { None } else { Some(bytes) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io;

    #[test]
    fn chunking_works_as_expected_on_nonempty_file() {
        let this_file = file!();
        let f = File::open(this_file).unwrap();
        let num_bytes = f.metadata().unwrap().len();
        let reader = io::BufReader::new(f);
        let chunks = byte_chunks(reader, 256);
        let mut total_chunk_size = 0;
        for chunk in chunks {
            assert_ne!(chunk.len(), 0);
            total_chunk_size += chunk.len() as u64;
        }
        assert_eq!(total_chunk_size, num_bytes);
    }
}
