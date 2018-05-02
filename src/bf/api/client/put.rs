// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::cell::Cell;

use futures::*;

use hyper::Method;

use serde;

use bf;
use bf::api::client::{Blackfynn, Request};

/// An abstraction of an HTTP `PUT` request.
///
/// # Examples
///
/// ```rust,ignore
/// Put::new(self, "/user/")
///                 .body(user)
///                 .and_then(move |user_response: model::User| {
///                     this.set_current_organization(user_response.preferred_organization());
///                     Ok(user_response)
///                 })
/// ```
pub struct Put<P, T> {
    bf: Blackfynn,
    route: String,
    params: Vec<(String, String)>,
    body: Option<P>,
    initialized: bool,
    request_fut: Cell<Option<bf::Future<T>>>,
}

impl<B, T> Put<B, T>
where
    B: serde::Serialize,
    T: 'static + serde::de::DeserializeOwned,
{
    #[allow(dead_code)]
    pub fn new<R: Into<String>>(bf: &Blackfynn, route: R) -> Self {
        Self {
            bf: bf.clone(),
            route: route.into(),
            params: vec![],
            body: None as Option<B>,
            initialized: false,
            request_fut: Cell::new(None),
        }
    }

    #[allow(dead_code)]
    pub fn param<S: Into<String>>(mut self, key: S, value: S) -> Self {
        self.params.push((key.into(), value.into()));
        self
    }

    #[allow(dead_code)]
    pub fn body(mut self, body: B) -> Self {
        self.body = Some(body);
        self
    }
}

impl<P: serde::Serialize, T> Request<T> for Put<P, T>
where
    T: 'static + serde::de::DeserializeOwned,
{
    fn new_request(&self) -> bf::Future<T> {
        self.bf.request(
            self.route.clone(),
            Method::Put,
            self.params.clone(),
            self.body.as_ref(),
        )
    }
}

impl<P: serde::Serialize, T> Future for Put<P, T>
where
    T: 'static + serde::de::DeserializeOwned,
{
    type Item = T;
    type Error = bf::error::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if !self.initialized {
            self.request_fut.replace(Some(self.new_request()));
            self.initialized = true
        };
        self.request_fut.get_mut().as_mut().unwrap().poll()
    }
}
