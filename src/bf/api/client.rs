use std::cell::{RefCell};
use std::rc::{Rc};

use futures::*;

use hyper;
use hyper::client::{Client, HttpConnector};
use hyper_tls::HttpsConnector;

use serde;
use serde_json;

use tokio_core::reactor::{Core, Handle};

use bf;
use bf::config::{Config};
use bf::api::types::{SessionToken};
use bf::api::{request, response};

/// A custom session ID header for the Blackfynn API
header! { (XSessionId, "X-SESSION-ID") => [String] }

pub struct BlackFynnImpl {
    config: Config,
    client: Client<HttpsConnector<HttpConnector>>,
    session_token: Option<SessionToken>
}

/// The Blackfynn web client
pub struct Blackfynn {
    // See https://users.rust-lang.org/t/best-pattern-for-async-update-of-self-object/15205
    // for notes on this pattern:
    inner: Rc<RefCell<BlackFynnImpl>>
}

impl Clone for Blackfynn {
    fn clone(&self) -> Self {
        Blackfynn {
            inner: Rc::clone(&self.inner)
        }
    }
}

impl Blackfynn {
    #[allow(dead_code)]
    fn new(handle: &Handle, config: Config) -> Self {
        let client = Client::configure()
            .connector(HttpsConnector::new(4, handle).unwrap())
            .build(&handle);
        Blackfynn {
            inner: Rc::new(RefCell::new(BlackFynnImpl {
                config,
                client,
                session_token: None
            }))
        }
    }

    fn session_token(&self) -> Option<SessionToken> {
        self.inner.borrow().session_token.clone()
    }

    fn request<S: Into<String>, P, Q>(&self, route: S, method: hyper::Method, payload: Option<&P>) -> bf::Future<Q>
        where P: serde::Serialize,
              Q: 'static + serde::de::DeserializeOwned
    {
        // Build the request url: config environment base + route:
        let mut use_url = self.inner.borrow().config.env().url().clone();
        use_url.set_path(&route.into());

        // Lift the URL into a future:
        let url = future::result(use_url.to_string().parse::<hyper::Uri>()).map_err(|e| e.into());

        // If a body payload was provided, lift it into a future:
        let body: bf::Future<Option<String>> = if let Some(data) = payload {
            Box::new(future::result(serde_json::to_string(data)).map(Some).map_err(|e| e.into()))
        } else {
            Box::new(future::ok(None))
        };

        // Lift the session token into a future:
        let maybe_token = future::ok(self.session_token().clone());

        let bf = future::ok(self.clone());

        Box::new(bf.join4(url, body, maybe_token)
            .and_then(move |(bf, url, body, token): (Blackfynn, hyper::Uri, Option<String>, Option<SessionToken>)| {
                let uri = url.to_string().parse::<hyper::Uri>().unwrap();
                let mut req = hyper::Request::new(method.clone(), uri);
                // If a body was provided, set it in the outgoing request:
                if let Some(b) = body {
                    req.set_body(b);
                }
                Ok((bf, req, token))
            })
            .and_then(move |(bf, mut req, token): (Blackfynn, hyper::Request, Option<SessionToken>)| {
                // If a session token exists, use it to set the "X-SESSION-ID"
                // header to make subsequent requests:
                if let Some(session_token) = token {
                    req.headers_mut().set(XSessionId(session_token.into()));
                }
                // Make the actual request:
                bf.inner.borrow().client.request(req).map_err(|e| e.into())
            })
            .and_then(|response: hyper::Response| {
                // Check the status code. And 5XX code will result in the
                // future terminating with an error containing the message
                // emitted from the API:
                let status_code = response.status();
                response
                    .body()
                    .concat2()
                    .map_err(|e| e.into())
                    .and_then(move |body: hyper::Chunk| {
                        future::ok((status_code, body))
                    })
                    .and_then(move |(status_code, body): (hyper::StatusCode, hyper::Chunk)| {
                        if status_code.is_client_error() || status_code.is_server_error() {
                            return future::err(bf::error::Error::ApiError(String::from_utf8_lossy(&body).to_string()));
                        }
                        future::ok(body)
                    })
                    .and_then(|body: hyper::Chunk| {
                        // Finally, attempt to parse the JSON response into a
                        // typeful representation:
                        serde_json::from_slice::<Q>(&body).map_err(|e| e.into())
                    })
            })
        )
    }

    #[allow(dead_code)]
    fn get<S: Into<String>, Q>(&self, route: S) -> bf::Future<Q>
        where Q: 'static + serde::de::DeserializeOwned
    {
        self.request(route, hyper::Method::Get, None as Option<&serde_json::Value>)
    }

    #[allow(dead_code)]
    fn post<S: Into<String>, P, Q>(&self, route: S, payload: &P) -> bf::Future<Q>
        where P: serde::Serialize,
              Q: 'static + serde::de::DeserializeOwned
    {
        self.request(route, hyper::Method::Post, Some(payload))
    }

    #[allow(dead_code)]
    fn put<S: Into<String>, P, Q>(&self, route: S, payload: &P) -> bf::Future<Q>
        where P: serde::Serialize,
              Q: 'static + serde::de::DeserializeOwned
    {
        self.request(route, hyper::Method::Put, Some(payload))
    }

    #[allow(dead_code)]
    fn delete<S: Into<String>, Q>(&self, route: S) -> bf::Future<Q>
        where Q: 'static + serde::de::DeserializeOwned
    {
        self.request(route, hyper::Method::Delete, None as Option<&serde_json::Value>)
    }

    #[allow(dead_code)]
    ///
    /// # Example
    ///
    ///   ```
    ///   extern crate blackfynn;
    ///
    ///   fn main() {
    ///     use blackfynn::{Blackfynn, Config, Environment};
    ///
    ///     let config = Config::new(Environment::Local);
    ///     let result = Blackfynn::run(config, move |ref bf| {
    ///       // Not logged in
    ///       bf.organizations()
    ///     });
    ///     assert!(result.is_err());
    ///   }
    ///   ```
    ///
    pub fn run<F, T>(config: Config, runner: F) -> bf::Result<T>
        where
            F: Fn(Blackfynn) -> bf::Future<T>
    {
        let mut core = Core::new().expect("couldn't create event loop");
        Self::run_with_core(&mut core, config, runner)
    }

    #[allow(dead_code)]
    ///
    /// # Example
    ///
    ///   ```
    ///   extern crate blackfynn;
    ///   extern crate tokio_core;
    ///
    ///   fn main() {
    ///     use blackfynn::{Blackfynn, Config, Environment};
    ///     use tokio_core::reactor::Core;
    ///
    ///     let mut core = Core::new().unwrap();
    ///     let config = Config::new(Environment::Local);
    ///     let result = Blackfynn::run_with_core(&mut core, config, move |ref bf| {
    ///       // Not logged in
    ///       bf.organizations()
    ///     });
    ///     assert!(result.is_err());
    ///   }
    ///   ```
    ///
    pub fn run_with_core<F, T>(core: &mut Core, config: Config, runner: F) -> bf::Result<T>
        where
            F: Fn(Blackfynn) -> bf::Future<T>
    {
        let handle = core.handle();
        let bf = Self::new(&handle, config);
        let future_to_run = runner(bf);
        core.run(future_to_run)
    }

    /// Tests if the user is logged in and has an active session.
    pub fn has_session(&self) -> bool {
        self.session_token().is_some()
    }

    /// Return a Future that, when resolved, logs in to the Blackfynn API.
    /// If successful, the Blackfynn client will store the resulting session
    /// token for subsequent API calls.
    #[allow(dead_code)]
    pub fn login<S: Into<String>>(&self, email: S, password: S) -> bf::Future<response::Login> {
        let this = self.clone();
        Box::new(
            self.post("/account/login", &request::Login::new(email.into(), password.into()))
            .and_then(move |login_response: response::Login| {
                this.inner.borrow_mut().session_token = login_response.session_token.clone();
                Ok(login_response)
            })
        )
    }

    /// Return a Future, that when, resolved returns a listing of the
    /// organizations the user is a member of.
    pub fn organizations(&self) -> bf::Future<response::Organization> {
        self.get("/organizations/")
    }

    /// Return a Future, that when, resolved returns a listing of the
    /// datasets the user has access to.
    pub fn datasets(&self) -> bf::Future<Vec<response::Dataset>> {
        self.get("/datasets/")
    }

    /// Return a Future, that when, resolved returns the specified dataset.
    pub fn dataset_by_id<S: Into<String>>(&self, id: S) -> bf::Future<Option<response::Dataset>> {
        self.get(format!("/datasets/{}", id.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bf::config::{Environment};
    use tokio_core::reactor::{Core};

    const FIXTURE_DATASET: &'static str = "N:dataset:149b65da-6803-4a67-bf20-83076774a5c7";

    fn create_local_bf() -> (Blackfynn, Core) {
        let core = Core::new().expect("couldn't create tokio core");
        let handle = core.handle();
        let config = Config::new(Environment::Local);
        let bf = Blackfynn::new(&handle, config);
        (bf, core)
    }

    #[test]
    fn login_successfully_locally() {
        let (bf, mut core) = create_local_bf();
        let login = bf.login("test@blackfynn.com", "password")
            .then(|r| {
                assert!(r.is_ok());
                future::result(r)
            });
        assert!(core.run(login).is_ok());
        assert!(bf.session_token().is_some());
    }

    #[test]
    fn login_fails_locally() {
        let (bf, mut core) = create_local_bf();
        let login = bf.login("test@blackfynn.com", "this-is-a-bad-password")
            .then(|r| {
                assert!(r.is_err());
                future::result(r)
            });
        assert!(core.run(login).is_err());
        assert!(bf.session_token().is_none());
    }

    #[test]
    fn fetching_organizations_after_login_is_successful() {
        let config = Config::new(Environment::Local);
        let org = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login("test@blackfynn.com", "password")
                .and_then(move |_| {
                    bf.organizations()
                })
            )
        });
        assert!(org.is_ok());
    }

    #[test]
    fn fetching_organizations_fails_if_login_fails() {
        let config = Config::new(Environment::Local);
        let org = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login("test@blackfynn.com", "another-bad-password")
                .and_then(move |_| {
                    bf.organizations()
                })
            )
        });
        assert!(org.is_err());
    }

    #[test]
    fn fetching_datasets_after_login_is_successful() {
        let config = Config::new(Environment::Local);
        let ds = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login("test@blackfynn.com", "password")
                .and_then(move |_| {
                    bf.datasets()
                })
            )
        });
        assert!(ds.is_ok());
    }

    #[test]
    fn fetching_datasets_fails_if_login_fails() {
        let config = Config::new(Environment::Local);
        let ds = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.datasets()
            )
        });
        assert!(ds.is_err());
    }

    #[test]
    fn fetching_dataset_by_id_successful_if_logged_in_and_exists() {
        let config = Config::new(Environment::Local);
        let ds = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login("test@blackfynn.com", "password")
                .and_then(move |_| {
                    bf.dataset_by_id(FIXTURE_DATASET)
                })
            )
        });
        assert!(ds.is_ok());
    }

    #[test]
    fn fetching_dataset_by_id_fails_if_logged_in_but_doesnt_exists() {
        let config = Config::new(Environment::Local);
        let ds = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login("test@blackfynn.com", "password")
                .and_then(move |_| {
                    bf.dataset_by_id("N:dataset:not-real-6803-4a67-bf20-83076774a5c7")
                })
            )
        });
        assert!(ds.is_err());
    }
}
