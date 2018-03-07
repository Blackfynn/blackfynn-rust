// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Blackfynn-specific API errors and such.

use std::{error, fmt, path, io};

use hyper;

use rusoto_s3;

use serde_json;

use url;

#[derive(Debug)]
pub enum Error {
    ApiError(String),
    HttpError(hyper::error::Error),
    InvalidUnicodePath(path::PathBuf),
    IoError(io::Error),
    JsonError(serde_json::Error),
    S3PutObjectError(rusoto_s3::PutObjectError),
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

impl From<rusoto_s3::PutObjectError> for Error {
    fn from(error: rusoto_s3::PutObjectError) -> Self {
        Error::S3PutObjectError(error)
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
            ApiError(_) => "api error",
            HttpError(_) => "http error",
            InvalidUnicodePath(_) => "invalid unicode path",
            IoError(_) => "io error",
            JsonError(_) => "json error",
            S3PutObjectError(_) => "S3: put object error",
            UriError(_) => "uri error",
            UrlParseError(_) => "url parse error"
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match *self {
            ApiError(ref message) => write!(f, "API error :: {}", message),
            HttpError(ref err) => write!(f, "HTTP error :: {}", err),
            InvalidUnicodePath(ref path) => write!(f, "Invalid unicode characters in path :: {:?}", path),
            IoError(ref err) => write!(f, "IO error :: {}", err),
            JsonError(ref err) => write!(f, "JSON error :: {}", err),
            S3PutObjectError(ref err) => write!(f, "S3 put object error :: {}", err),
            UriError(ref err) => write!(f, "URI error :: {}", err),
            UrlParseError(ref err) => write!(f, "URL parse error :: {}", err)
        }
    }
}
