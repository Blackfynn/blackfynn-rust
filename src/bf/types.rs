// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Library-wide type definitions.

use futures;

use std::result;

use bf::error;

/// A Result type parameterized by `bf::error::Error`
pub type Result<T> = result::Result<T, error::Error>;

/// A Future type parameterized by `bf::error::Error`
#[allow(dead_code)]
pub type Future<T> = Box<futures::Future<Item=T, Error=error::Error>>;

/// A Stream type parameterized by `bf::error::Error`
#[allow(dead_code)]
pub type Stream<T> = Box<futures::stream::Stream<Item=T, Error=error::Error>>;
