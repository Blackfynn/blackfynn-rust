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
pub fn into_future_trait<F, I, E>(f: F) -> Box<Future<Item=I, Error=E>>
    where F: 'static + Future<Item=I, Error=E>
{
    Box::new(f)
}

#[allow(dead_code)]
pub fn into_stream_trait<S, I, E>(s: S) -> Box<Stream<Item=I, Error=E>>
    where S: 'static + Stream<Item=I, Error=E>
{
    Box::new(s)
}

#[allow(dead_code)]
pub fn into_sink_trait<S, I, E>(s: S) -> Box<Sink<SinkItem=I, SinkError=E>>
    where S: 'static + Sink<SinkItem=I, SinkError=E>
{
    Box::new(s)
}

#[allow(dead_code)]
pub fn return2<U, V, E, F, G>(f1: F, f2: G) -> Box<Future<Item=(U, V), Error=E>>
    where F: 'static + Future<Item=U, Error=E>,
          G: 'static + Future<Item=V, Error=E>,
{
    Box::new(f1.join(f2))
}

#[allow(dead_code)]
pub fn return3<U, V, W, E, F, G, H>(f1: F, f2: G, f3: H) -> Box<Future<Item=(U, V, W), Error=E>>
    where F: 'static + Future<Item=U, Error=E>,
          G: 'static + Future<Item=V, Error=E>,
          H: 'static + Future<Item=W, Error=E>
{
    Box::new(f1.join3(f2, f3))
}

#[allow(dead_code)]
pub fn return4<U, V, W, X, E, F, G, H, I>(f1: F, f2: G, f3: H, f4: I) -> Box<Future<Item=(U, V, W, X), Error=E>>
    where F: 'static + Future<Item=U, Error=E>,
          G: 'static + Future<Item=V, Error=E>,
          H: 'static + Future<Item=W, Error=E>,
          I: 'static + Future<Item=X, Error=E>
{
    Box::new(f1.join4(f2, f3, f4))
}

#[allow(dead_code)]
pub fn return5<U, V, W, X, Y, E, F, G, H, I, J>(f1: F, f2: G, f3: H, f4: I, f5: J) -> Box<Future<Item=(U, V, W, X, Y), Error=E>>
    where F: 'static + Future<Item=U, Error=E>,
          G: 'static + Future<Item=V, Error=E>,
          H: 'static + Future<Item=W, Error=E>,
          I: 'static + Future<Item=X, Error=E>,
          J: 'static + Future<Item=Y, Error=E>
{
    Box::new(f1.join5(f2, f3, f4, f5))
}
