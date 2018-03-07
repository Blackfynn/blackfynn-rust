use std::cell::Cell;

use futures::*;

use hyper::Method;

use serde;

use bf;
use bf::api::client::{Blackfynn, Request};

pub struct Post<P, T> {
    bf: Blackfynn,
    route: String,
    params: Vec<(String, String)>,
    body: Option<P>,
    initialized: bool,
    request_fut: Cell<Option<bf::Future<T>>>
}

impl <B, T> Post<B, T>
where B: serde::Serialize,
      T: 'static + serde::de::DeserializeOwned
{
    #[allow(dead_code)]
    pub fn new<R: Into<String>>(bf: &Blackfynn, route: R) -> Self {
        Self {
            bf: bf.clone(),
            route: route.into(),
            params: vec![],
            body: None as Option<B>,
            initialized: false,
            request_fut: Cell::new(None)
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

impl <P: serde::Serialize, T> Request<T> for Post<P, T>
where T: 'static + serde::de::DeserializeOwned
{
    fn new_request(&self) -> bf::Future<T> {
        self.bf.request(self.route.clone(), Method::Post, self.params.clone(), self.body.as_ref())
    }
}

impl <P: serde::Serialize, T> Future for Post<P, T>
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
