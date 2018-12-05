// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use hyper::body::Payload;
use hyper;

use futures::Async;

use std::fs::File;
use std::path::Path;
use std::io::{ Read, Error };

pub struct ChunkedFilePayload {
    file: File,
    chunk_size_bytes: usize
}

impl ChunkedFilePayload {
    pub fn new<P>(filepath: P) -> Self
    where P: AsRef<Path> {
        Self {
            file: File::open(filepath).unwrap(),
            chunk_size_bytes: 8000
        }
    }
}

impl Payload for ChunkedFilePayload {
    type Data = hyper::Chunk;
    type Error = Error;

    fn poll_data(&mut self) -> Result<Async<Option<Self::Data>>, Self::Error> {
        // let mut buffer = Vec::with_capacity(self.chunk_size_bytes);
        let mut buffer = vec![0; 8000];
        let read_result = self.file.read(&mut buffer);

        read_result.map(|bytes_read| if bytes_read > 0 {
            println!("bytes-read: {}", bytes_read);
            buffer.truncate(bytes_read);

            Async::Ready(Some(hyper::Chunk::from(buffer)))
        } else {
            println!("no bytes read!");
            Async::Ready(None)
        })
    }
}
