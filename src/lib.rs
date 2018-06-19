// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

// `error_chain!` can recurse deeply
#![recursion_limit = "1024"]

extern crate chrono;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate hyper;
extern crate hyper_tls;
#[macro_use]
extern crate lazy_static;
extern crate rusoto_core;
extern crate rusoto_credential;
extern crate rusoto_s3;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tokio;
extern crate url;

mod bf;

// Publicly re-export:
pub use bf::api::Blackfynn;
pub use bf::config::{Config, Environment};
pub use bf::types::{Error, ErrorKind, Future, Result, ResultExt, Stream};
pub use bf::{api, error, model};
