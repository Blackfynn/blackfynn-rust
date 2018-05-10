// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Errors specific to the Blackfynn platform.

use std::{error, fmt, io, path};

use hyper;

use rusoto_s3;

use serde_json;

use url;

#[derive(Debug)]
pub enum Error {
    ApiError(hyper::StatusCode, String),
    HttpError(hyper::error::Error),
    InvalidUnicodePath(path::PathBuf),
    IoError(io::Error),
    JsonError(serde_json::Error),
    S3AbortMultipartUploadError(rusoto_s3::AbortMultipartUploadError),
    S3CreateMultipartUploadError(rusoto_s3::CreateMultipartUploadError),
    S3CompleteMultipartUploadError(rusoto_s3::CompleteMultipartUploadError),
    S3MissingUploadId,
    S3PutObjectError(rusoto_s3::PutObjectError),
    S3UploadPartError(rusoto_s3::UploadPartError),
    UriError(hyper::error::UriError),
    UrlParseError(url::ParseError),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IoError(error)
    }
}

impl From<hyper::error::Error> for Error {
    fn from(error: hyper::error::Error) -> Self {
        Error::HttpError(error)
    }
}

impl From<hyper::error::UriError> for Error {
    fn from(error: hyper::error::UriError) -> Self {
        Error::UriError(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::JsonError(error)
    }
}

impl From<rusoto_s3::AbortMultipartUploadError> for Error {
    fn from(error: rusoto_s3::AbortMultipartUploadError) -> Self {
        Error::S3AbortMultipartUploadError(error)
    }
}

impl From<rusoto_s3::CreateMultipartUploadError> for Error {
    fn from(error: rusoto_s3::CreateMultipartUploadError) -> Self {
        Error::S3CreateMultipartUploadError(error)
    }
}

impl From<rusoto_s3::CompleteMultipartUploadError> for Error {
    fn from(error: rusoto_s3::CompleteMultipartUploadError) -> Self {
        Error::S3CompleteMultipartUploadError(error)
    }
}

impl From<rusoto_s3::PutObjectError> for Error {
    fn from(error: rusoto_s3::PutObjectError) -> Self {
        Error::S3PutObjectError(error)
    }
}

impl From<rusoto_s3::UploadPartError> for Error {
    fn from(error: rusoto_s3::UploadPartError) -> Self {
        Error::S3UploadPartError(error)
    }
}

impl From<url::ParseError> for Error {
    fn from(error: url::ParseError) -> Self {
        Error::UrlParseError(error)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        use self::Error::*;
        match *self {
            ApiError(_, _) => "API error",
            HttpError(_) => "HTTP error",
            InvalidUnicodePath(_) => "Invalid unicode path",
            IoError(_) => "IO error",
            JsonError(_) => "JSON error",
            S3AbortMultipartUploadError(_) => "S3: abort multipart upload error",
            S3CreateMultipartUploadError(_) => "S3: create multipart upload error",
            S3CompleteMultipartUploadError(_) => "S3: complete multipart upload error",
            S3MissingUploadId => "S3: missing upload ID",
            S3PutObjectError(_) => "S3: put object error",
            S3UploadPartError(_) => "S3: upload part error",
            UriError(_) => "URI error",
            UrlParseError(_) => "URL parse error",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match *self {
            ApiError(status_code, ref message) => {
                write!(f, "API error :: {} {}", status_code, message)
            }
            HttpError(ref err) => write!(f, "HTTP error :: {}", err),
            InvalidUnicodePath(ref path) => {
                write!(f, "Invalid unicode characters in path :: {:?}", path)
            }
            IoError(ref err) => write!(f, "IO error :: {}", err),
            JsonError(ref err) => write!(f, "JSON error :: {}", err),
            S3AbortMultipartUploadError(ref err) => {
                write!(f, "S3: abort multipart upload error :: {}", err)
            }
            S3CreateMultipartUploadError(ref err) => {
                write!(f, "S3: create multipart upload error :: {}", err)
            }
            S3CompleteMultipartUploadError(ref err) => {
                write!(f, "S3: complete multipart upload error :: {}", err)
            }
            S3MissingUploadId => write!(f, "S3: missing upload ID"),
            S3PutObjectError(ref err) => write!(f, "S3 put object error :: {}", err),
            S3UploadPartError(ref err) => write!(f, "S3 upload part error :: {}", err),
            UriError(ref err) => write!(f, "URI error :: {}", err),
            UrlParseError(ref err) => write!(f, "URL parse error :: {}", err),
        }
    }
}
