// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Errors specific to the Blackfynn platform.
use std::{io, path};

use futures;

use hyper;

use rusoto_core;

use rusoto_s3;

use serde_json;

use url;

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        Cancelled(futures::Canceled);
        HttpError(hyper::error::Error);
        IoError(io::Error);
        StripPrefixError(path::StripPrefixError);
        JsonError(serde_json::Error);
        S3AbortMultipartUploadError(rusoto_s3::AbortMultipartUploadError);
        S3CreateMultipartUploadError(rusoto_s3::CreateMultipartUploadError);
        S3CompleteMultipartUploadError(rusoto_s3::CompleteMultipartUploadError);
        S3PutObjectError(rusoto_s3::PutObjectError);
        S3UploadPartError(rusoto_s3::UploadPartError);
        TlsError(rusoto_core::request::TlsError);
        UrlParseError(url::ParseError);
    }

    errors {
        ApiError(status_code: hyper::StatusCode, message: String) {
            description("API error")
            display("API error :: {} {}", status_code, message)
        }
        UploadError(message: String) {
            description("Upload error")
            display("Upload error :: {}", message)
        }
        EnvParseError(s: String) {
            description("API: Invalid environment string")
            display("API: Invalid environment string :: {}", s)
        }
        InvalidUnicodePathError(p: path::PathBuf) {
            description("API: Invalid unicode characters in path")
            display("API: Invalid unicode characters in path :: {:?}", p)
        }
        NoPathParentError(p: path::PathBuf) {
            description("Could not get the parent of path")
            display("Could not get the parent of path :: {:?}", p)
        }
        NoOrganizationSetError {
            description("API: No organization set")
            display("API: No organization set")
        }
        S3MissingUploadIdError {
            description("S3: missing upload ID")
            display("S3: missing upload ID")
        }
    }
}
