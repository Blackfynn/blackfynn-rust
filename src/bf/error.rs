/// Blackfynn-specific API errors and such.

use std::{error, fmt};

use hyper;

use serde_json;

use url;

#[derive(Debug)]
pub enum Error {
    ApiError(String),
    HttpError(hyper::error::Error),
    JsonError(serde_json::Error),
    UriError(hyper::error::UriError),
    UrlParseError(url::ParseError),
}

impl From<hyper::error::Error> for Error {
    fn from(error: hyper::error::Error) -> Self {
        Error::HttpError(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::JsonError(error)
    }
}

impl From<hyper::error::UriError> for Error {
    fn from(error: hyper::error::UriError) -> Self {
        Error::UriError(error)
    }
}

impl From<url::ParseError> for Error {
    fn from(error: url::ParseError) -> Self {
        Error::UrlParseError(error)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            &Error::ApiError(_) => "api error",
            &Error::HttpError(_) => "http error",
            &Error::JsonError(_) => "json error",
            &Error::UriError(_) => "uri error",
            &Error::UrlParseError(_) => "url parse error"
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::ApiError(ref message) => write!(f, "API error :: {}", message),
            &Error::HttpError(ref e) => write!(f, "HTTP error :: {}", e),
            &Error::JsonError(ref e) => write!(f, "JSON error :: {}", e),
            &Error::UriError(ref e) => write!(f, "URI error :: {}", e),
            &Error::UrlParseError(ref e) => write!(f, "URL parse error :: {}", e)
        }
    }
}
