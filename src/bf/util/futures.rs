// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Future-related utility code lives here.

use futures::*;

// This basically converts a concrete object implementing the `Future` trait
// into a `Box`ed trait object. This allows for a function to return a variety
// of Future-traited objects with different concrete types, while allow them
// all to be typed the same.
//
// Downside: this function introduces a heap allocated value to accomplish this
// until `impl traits` are available in the stable rustc channel.
//
// See https://github.com/rust-lang/rust/issues/34511 for tracking the status
// of `impl traits`.
#[allow(dead_code)]
pub fn into_future_trait<F, I, E>(f: F) -> Box<dyn Future<Item = I, Error = E> + Send>
where
    F: 'static + Send + Future<Item = I, Error = E>,
{
    Box::new(f)
}

#[allow(dead_code)]
pub fn into_stream_trait<S, I, E>(s: S) -> Box<dyn Stream<Item = I, Error = E> + Send>
where
    S: 'static + Send + Stream<Item = I, Error = E>,
{
    Box::new(s)
}
