// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! The Blackfynn platform API.

mod client;
pub mod request;
pub mod response;

// Re-export:
pub use self::client::Blackfynn;
pub use self::client::get::Get;
pub use self::client::post::Post;
pub use self::client::put::Put;
pub use self::client::s3::{MultipartUploadResult, ProgressCallback, ProgressUpdate, S3Uploader,
                           S3_MIN_PART_SIZE, UploadProgress, UploadProgressIter};
