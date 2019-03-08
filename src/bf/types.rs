// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Library-wide type definitions.

use futures;

use crate::bf::error;

pub use crate::bf::error::{Error, ErrorKind, Result};

/// A `futures::future::Future` type parameterized by `bf::error::Error`
#[allow(dead_code)]
pub type Future<T> = Box<dyn futures::Future<Item = T, Error = error::Error> + Send>;

/// A `futures::stream::Stream` type parameterized by `bf::error::Error`
#[allow(dead_code)]
pub type Stream<T> = Box<dyn futures::stream::Stream<Item = T, Error = error::Error> + Send>;
