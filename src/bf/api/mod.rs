// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! The Blackfynn platform API.

mod client;
pub mod request;
pub mod response;

pub use self::client::s3::{
    MultipartUploadResult, S3Uploader, UploadProgress,
    UploadProgressIter, S3_MIN_PART_SIZE,
};
pub use self::client::progress::{ProgressCallback, ProgressUpdate};

pub use self::client::Blackfynn;
