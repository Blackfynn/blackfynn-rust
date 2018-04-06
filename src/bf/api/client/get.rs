// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::cell::Cell;

use futures::*;

use hyper::Method;

use serde;

use bf;
use bf::api::client::{Blackfynn, Nothing, Request};

/// An abstraction of an HTTP `GET` request.
///
/// # Examples
///
/// ```rust,ignore
/// Get::new(self, format!("/organizations/{id}", id=Into::<String>::into(id)))
/// ```
///
/// ```rust,ignore
/// Get::new(self, format!("/security/user/credentials/upload/{dataset}", dataset=Into::<String>::into(dataset_id))
/// ```
pub struct Get<T> {
    bf: Blackfynn,
    route: String,
    params: Vec<(String, String)>,
    initialized: bool,
    request_fut: Cell<Option<bf::Future<T>>>
}

impl <T> Get<T>
where T: 'static + serde::de::DeserializeOwned
{
    #[allow(dead_code)]
    pub fn new<R: Into<String>>(bf: &Blackfynn, route: R) -> Self {
        Self {
            bf: bf.clone(),
            route: route.into(),
            params: vec![],
            initialized: false,
            request_fut: Cell::new(None)
        }
    }

    #[allow(dead_code)]
    pub fn param<S: Into<String>>(mut self, key: S, value: S) -> Self {
        self.params.push((key.into(), value.into()));
        self
    }
}

impl <T> Request<T> for Get<T>
where T: 'static + serde::de::DeserializeOwned
{
    fn new_request(&self) -> bf::Future<T> {
        self.bf.request(self.route.clone(), Method::Get, self.params.clone(), None as Option<&Nothing>)
    }
}

impl <T> Future for Get<T>
where T: 'static + serde::de::DeserializeOwned
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
