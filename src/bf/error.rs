// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Errors specific to the Blackfynn platform.
use std::path::PathBuf;
use std::{fmt, io, num, result};

use failure::{Backtrace, Context, Fail};

use hyper::http::header::ToStrError;

/// Type alias for handling errors throughout the agent
pub type Result<T> = result::Result<T, Error>;

/// An error that can occur while interacting with the agent
#[derive(Debug)]
pub struct Error {
    ctx: Context<ErrorKind>,
}

impl Error {
    /// Return the kind of this error.
    pub fn kind(&self) -> &ErrorKind {
        self.ctx.get_context()
    }

    pub fn api_error<S: Into<String>>(status_code: hyper::StatusCode, message: S) -> Error {
        ErrorKind::ApiError {
            status_code,
            message: message.into(),
        }
        .into()
    }

    pub fn upload_error<S: Into<String>>(message: S) -> Error {
        ErrorKind::UploadError {
            message: message.into(),
        }
        .into()
    }

    pub fn invalid_dataset_name<S: Into<String>>(name: S) -> Error {
        ErrorKind::InvalidDatasetName { name: name.into() }.into()
    }

    pub fn invalid_arguments<S: Into<String>>(message: S) -> Error {
        ErrorKind::InvalidArguments {
            message: message.into(),
        }
        .into()
    }

    pub fn env_parse_error<S: Into<String>>(value: S) -> Error {
        ErrorKind::EnvParseError {
            value: value.into(),
        }
        .into()
    }

    pub fn no_path_parent(path: PathBuf) -> Error {
        ErrorKind::NoPathParent { path }.into()
    }

    pub fn path_does_not_exist(path: PathBuf) -> Error {
        ErrorKind::PathDoesNotExist { path }.into()
    }

    pub fn path_is_not_a_file(path: PathBuf) -> Error {
        ErrorKind::PathIsNotAFile { path }.into()
    }

    pub fn could_not_get_filename(path: PathBuf) -> Error {
        ErrorKind::CouldNotGetFilename { path }.into()
    }

    pub fn path_is_not_a_directory(path: PathBuf) -> Error {
        ErrorKind::PathIsNotADirectory { path }.into()
    }

    pub fn invalid_unicode_path(path: PathBuf) -> Error {
        ErrorKind::InvalidUnicodePath { path }.into()
    }
}

impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.ctx.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.ctx.backtrace()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.ctx.fmt(f)
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        self.kind().clone().into()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Fail)]
pub enum ErrorKind {
    #[fail(display = "api error: {} {}", status_code, message)]
    ApiError {
        status_code: hyper::StatusCode,
        message: String,
    },

    #[fail(display = "couldn't find dataset: \"{}\"", name)]
    InvalidDatasetName { name: String },

    #[fail(display = "upload error: {}", message)]
    UploadError { message: String },

    #[fail(display = "invalid environment string: {}", value)]
    EnvParseError { value: String },

    #[fail(display = "invalid unicode characters in path: {:?}", path)]
    InvalidUnicodePath { path: PathBuf },

    #[fail(display = "{}", message)]
    InvalidArguments { message: String },

    #[fail(display = "could not get path parent: {:?}", path)]
    NoPathParent { path: PathBuf },

    #[fail(display = "path does not exist: {:?}", path)]
    PathDoesNotExist { path: PathBuf },

    #[fail(display = "path is not a file: {:?}", path)]
    PathIsNotAFile { path: PathBuf },

    #[fail(display = "couldn't get filename from path: {:?}", path)]
    CouldNotGetFilename { path: PathBuf },

    #[fail(display = "path is not a directory: {:?}", path)]
    PathIsNotADirectory { path: PathBuf },

    #[fail(display = "no organization set")]
    NoOrganizationSet,

    #[fail(display = "missing upload id")]
    S3MissingUploadId,

    #[fail(display = "io error: {}", error)]
    IoError { error: String },

    #[fail(display = "strip prefix error: {}", error)]
    StripPrefixError { error: String },

    #[fail(display = "hyper error: {}", error)]
    HyperError { error: String },

    #[fail(display = "tokio error: {}", error)]
    TokioError { error: String },

    #[fail(display = "json serialization error: {}", error)]
    SerdeJsonError { error: String },

    #[fail(display = "error parsing string: {}", error)]
    ParseIntError { error: String },
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error::from(Context::new(kind))
    }
}
impl From<Context<ErrorKind>> for Error {
    fn from(ctx: Context<ErrorKind>) -> Error {
        Error { ctx }
    }
}

/// map from StripPrefixError errors
impl From<std::path::StripPrefixError> for Error {
    fn from(error: std::path::StripPrefixError) -> Error {
        Error::from(Context::new(ErrorKind::StripPrefixError {
            error: error.to_string(),
        }))
    }
}

/// map from io errors
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::from(Context::new(ErrorKind::IoError {
            error: error.to_string(),
        }))
    }
}

/// map from serde_json errors
impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Error {
        Error::from(Context::new(ErrorKind::SerdeJsonError {
            error: error.to_string(),
        }))
    }
}

/// map from tokio errors
impl From<tokio::timer::Error> for Error {
    fn from(error: tokio::timer::Error) -> Error {
        Error::from(Context::new(ErrorKind::TokioError {
            error: error.to_string(),
        }))
    }
}

/// map from hyper errors
impl From<hyper::Error> for Error {
    fn from(error: hyper::Error) -> Error {
        Error::from(Context::new(ErrorKind::HyperError {
            error: error.to_string(),
        }))
    }
}
impl From<hyper::http::uri::InvalidUri> for Error {
    fn from(error: hyper::http::uri::InvalidUri) -> Error {
        Error::from(Context::new(ErrorKind::HyperError {
            error: error.to_string(),
        }))
    }
}
impl From<ToStrError> for Error {
    fn from(error: ToStrError) -> Error {
        Error::from(Context::new(ErrorKind::HyperError {
            error: error.to_string(),
        }))
    }
}
impl From<num::ParseIntError> for Error {
    fn from(error: num::ParseIntError) -> Error {
        Error::from(Context::new(ErrorKind::ParseIntError {
            error: error.to_string(),
        }))
    }
}
