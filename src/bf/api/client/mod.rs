// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Functions to interact with the Blackfynn platform.

pub mod s3;

pub use self::s3::{
    MultipartUploadResult, ProgressCallback, ProgressUpdate, S3Uploader, UploadProgress,
    UploadProgressIter,
};

use std::env;
use std::path::Path;
use std::sync::{Arc, Mutex};

use futures::*;

use hyper;
use hyper::client::{Builder, Client, HttpConnector};
use hyper_tls::HttpsConnector;

use serde;
use serde_json;

use tokio;

use super::request::chunked_http::ChunkedFilePayload;
use super::{request, response};
use bf;
use bf::config::{Config, Environment};
use bf::model::{
    self, DatasetId, ImportId, OrganizationId, PackageId, SessionToken, TemporaryCredential, UserId,
};
use bf::util::futures::into_future_trait;

// Blackfynn session authentication header:
const X_SESSION_ID: &str = "X-SESSION-ID";

struct BlackFynnImpl {
    config: Config,
    http_client: Client<HttpsConnector<HttpConnector>>,
    chunked_http_client: Client<HttpsConnector<HttpConnector>, ChunkedFilePayload>,
    session_token: Option<SessionToken>,
    current_organization: Option<OrganizationId>,
}

/// The Blackfynn client.
pub struct Blackfynn {
    // See https://users.rust-lang.org/t/best-pattern-for-async-update-of-self-object/15205
    // for notes on this pattern:
    inner: Arc<Mutex<BlackFynnImpl>>,
}

impl Clone for Blackfynn {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// =============================================================================

// Request parameter
type RequestParam = (String, String);

// A useful alias when dealing with the fact that an endpoint does not expect
// a POST/PUT body, but a type is still expected:
type Nothing = serde_json::Value;

// =============================================================================

// Useful builder macros:
macro_rules! route {
    ($uri:expr, $($var:ident),*) => (
        format!($uri, $($var = Into::<String>::into($var)),*)
    )
}

macro_rules! param {
    ($key:expr, $value:expr) => {
        ($key.into(), $value.into())
    };
}

// Based on https://docs.rs/maplit/1.0.1/maplit/
macro_rules! params {
    () => (vec![]); // For empty parameter lists
    ($($key:expr => $value:expr),*) => {
        {
            let mut _p: Vec<RequestParam> = vec![];
            $(
                _p.push(param!($key, $value));
            )*
            _p
        }
    }
}

// Empty payload
macro_rules! payload {
    () => {
        None as Option<&Nothing>
    };
    ($target:expr) => {
        Some($target).as_ref()
    };
}

macro_rules! get {
    ($target:expr, $route:expr) => {
        $target.request($route, hyper::Method::GET, params!(), payload!())
    };
    ($target:expr, $route:expr, $params:expr) => {
        $target.request($route, hyper::Method::GET, $params, payload!())
    };
}

macro_rules! post {
    ($target:expr, $route:expr) => {
        $target.request($route, hyper::Method::POST, params!(), payload!())
    };
    ($target:expr, $route:expr, $params:expr) => {
        $target.request($route, hyper::Method::POST, $params, payload!())
    };
    ($target:expr, $route:expr, $params:expr, $payload:expr) => {
        $target.request($route, hyper::Method::POST, $params, payload!($payload))
    };
}

macro_rules! put {
    ($target:expr, $route:expr) => {
        $target.request($route, hyper::Method::PUT, params!(), payload!())
    };
    ($target:expr, $route:expr, $params:expr) => {
        $target.request($route, hyper::Method::PUT, $params, payload!())
    };
    ($target:expr, $route:expr, $params:expr, $payload:expr) => {
        $target.request($route, hyper::Method::PUT, $params, payload!($payload))
    };
}

macro_rules! delete {
    ($target:expr, $route:expr) => {
        $target.request($route, hyper::Method::DELETE, params!(), payload!())
    };
    ($target:expr, $route:expr, $params:expr) => {
        $target.request($route, hyper::Method::DELETE, $params, payload!())
    };
    ($target:expr, $route:expr, $params:expr, $payload:expr) => {
        $target.request($route, hyper::Method::DELETE, $params, payload!($payload))
    };
}

// ============================================================================s

impl Blackfynn {
    /// Create a new Blackfynn API client.
    pub fn new(config: Config) -> Self {
        let connector = HttpsConnector::new(4).expect("bf:couldn't create https connector");
        let http_client = Client::builder().build(connector.clone());
        let chunked_http_client = Builder::default()
            .build::<HttpsConnector<HttpConnector>, ChunkedFilePayload>(connector);
        Self {
            inner: Arc::new(Mutex::new(BlackFynnImpl {
                config,
                http_client,
                chunked_http_client,
                session_token: None,
                current_organization: None,
            })),
        }
    }

    fn session_token(&self) -> Option<SessionToken> {
        self.inner.lock().unwrap().session_token.clone()
    }

    fn chunk_to_string(body: &hyper::Chunk) -> String {
        let as_bytes: Vec<u8> = body.to_vec();
        String::from_utf8_lossy(&as_bytes).to_string()
    }

    fn get_url(&self) -> url::Url {
        self.inner.lock().unwrap().config.env().url().clone()
    }

    fn request<I, P, Q, S>(
        &self,
        route: S,
        method: hyper::Method,
        params: I,
        payload: Option<&P>,
    ) -> bf::Future<Q>
    where
        P: serde::Serialize,
        I: IntoIterator<Item = RequestParam>,
        Q: 'static + Send + serde::de::DeserializeOwned,
        S: Into<String>,
    {
        let url = self.get_url();
        let method_clone = method.clone();

        // Build the request url: config environment base + route:
        let mut use_url = url.clone();
        use_url.set_path(&route.into());

        let token = self.session_token().clone();
        let client = self.inner.lock().unwrap().http_client.clone();

        // If query parameters are provided, add them to the constructed URL:
        for (k, v) in params {
            use_url
                .query_pairs_mut()
                .append_pair(k.as_str(), v.as_str());
        }

        // Lift the URL and body into Future:
        let uri = use_url
            .to_string()
            .parse::<hyper::Uri>()
            .into_future()
            .map_err(|e| bf::Error::with_chain(e, "bf:request:url"));

        let f = payload
            .map(|p| {
                serde_json::to_string(p)
                    .map(Into::into)
                    .map_err(|e| bf::Error::with_chain(e, "bf:request:serde"))
            }).unwrap_or(Ok(hyper::Body::empty()))
            .map_err(Into::into)
            .into_future()
            .join(uri)
            .and_then(move |(body, uri)| {
                let mut req = hyper::Request::builder()
                    .method(method.clone())
                    .uri(uri)
                    .body(body)
                    .unwrap();

                // If a session token exists, use it to set the "X-SESSION-ID"
                // header to make subsequent requests:
                if let Some(session_token) = token {
                    req.headers_mut().insert(
                        X_SESSION_ID,
                        hyper::header::HeaderValue::from_str(session_token.as_ref()).unwrap(),
                    );
                }

                // By default make every content type "application/json"
                req.headers_mut().insert(
                    hyper::header::CONTENT_TYPE,
                    hyper::header::HeaderValue::from_str("application/json").unwrap(),
                );

                // Make the actual request:
                client.request(req).map_err(move |e| {
                    bf::Error::with_chain(
                        e,
                        format!(
                            "bf:request<{method}:{url}>:execute",
                            method = method.to_string(),
                            url = use_url.to_string()
                        ),
                    )
                })
            })
            .and_then(move |resp| Self::process_response(resp, method_clone.to_string(), url.to_string()));

        into_future_trait(f)
    }

    fn request_chunked<I, S, Q, T>(
        &self,
        route: S,
        params: I,
        filepath: T,
    ) -> bf::Future<Q>
    where
        I: IntoIterator<Item = RequestParam>,
        S: Into<String>,
        Q: 'static + Send + serde::de::DeserializeOwned,
        T: AsRef<Path>,
    {
        let chunked_file_payload = ChunkedFilePayload::new(filepath);
        let url = self.get_url();

        // Build the request url: config environment base + route:
        let mut use_url = url.clone();
        use_url.set_path(&route.into());

        let token = self.session_token().clone();
        let client = self.inner.lock().unwrap().chunked_http_client.clone();

        // If query parameters are provided, add them to the constructed URL:
        for (k, v) in params {
            use_url
                .query_pairs_mut()
                .append_pair(k.as_str(), v.as_str());
        }

        // Lift the URL and body into Future:
        let uri = use_url
            .to_string()
            .parse::<hyper::Uri>()
            .into_future()
            .map_err(|e| bf::Error::with_chain(e, "bf:request:url"));

        let f = uri.and_then(move |uri| {
            let mut req = hyper::Request::builder()
                .method(hyper::Method::POST)
                .uri(uri)
                .body(chunked_file_payload)
                .unwrap();

            // If a session token exists, use it to set the "X-SESSION-ID"
            // header to make subsequent requests:
            if let Some(session_token) = token {
                req.headers_mut().insert(
                    hyper::header::AUTHORIZATION,
                    hyper::header::HeaderValue::from_str(&format!(
                        "Bearer {}",
                        session_token.into_inner()
                    )).unwrap(),
                );
            }

            // Make the actual request:
            client.request(req).map_err(move |e| {
                bf::Error::with_chain(
                    e,
                    format!(
                        "bf:request<{method}:{url}>:execute",
                        method = hyper::Method::POST,
                        url = use_url.to_string()
                    ),
                )
            })
        }).and_then(move |resp| Self::process_response(resp, hyper::Method::POST.to_string(), url.to_string()));

        into_future_trait(f)
    }

    fn process_response<Q, S, T>(
        response: hyper::Response<hyper::Body>,
        method_string: S,
        url_string: T,
    ) -> bf::Future<Q>
    where
        Q: 'static + Send + serde::de::DeserializeOwned,
        S: Into<String>,
        T: Into<String>,
    {
        let method_string: String = method_string.into();
        let url_string: String = url_string.into();
        let method_string_clone = method_string.clone();
        let url_string_clone = url_string.clone();

        // Check the status code. And 5XX code will result in the
        // future terminating with an error containing the message
        // emitted from the API:
        let status_code = response.status();
        let f = response
            .into_body()
            .concat2()
            .map_err(move |e| {
                bf::Error::with_chain(
                    e,
                    format!(
                        "bf:request<{method}:{url}>:response",
                        method = method_string,
                        url = url_string
                    ),
                )
            }).and_then(move |body: hyper::Chunk| Ok((status_code, body)))
            .and_then(
                move |(status_code, body): (hyper::StatusCode, hyper::Chunk)| {
                    if status_code.is_client_error() || status_code.is_server_error() {
                        return future::err(
                            bf::error::ErrorKind::ApiError(
                                status_code,
                                String::from_utf8_lossy(&body).to_string(),
                            ).into(),
                        );
                    }
                    future::ok(body)
                },
            ).and_then(|body: hyper::Chunk| {
                // If the debug flag `BLACKFYNN_LOG_LEVEL` is present
                // and equal to `DEBUG`, dump the request contents to stderr:
                if let Ok(log_level) = env::var("BLACKFYNN_LOG_LEVEL") {
                    if log_level == "DEBUG" {
                        eprintln!(
                            "[DEBUG] bf:request<{method}:{url}>:serialize:payload = {payload}",
                            method = method_string_clone,
                            url = url_string_clone,
                            payload = Self::chunk_to_string(&body)
                        );
                    }
                }
                // Finally, attempt to parse the JSON response into a typeful representation:
                serde_json::from_slice(&body).map_err(move |e| {
                    bf::Error::with_chain(
                        e,
                        format!(
                            "bf:request<{method}:{url}>:serialize:payload = {payload}",
                            method = method_string_clone,
                            url = url_string_clone,
                            payload = Self::chunk_to_string(&body)
                        ),
                    )
                })
            });
        into_future_trait(f)
    }

    ///
    ///# Example
    ///
    ///  ```rust,ignore
    ///  extern crate blackfynn;
    ///
    ///  fn main() {
    ///    use blackfynn::{Blackfynn, Config, Environment};
    ///
    ///    let config = Config::new(Environment::Development);
    ///    let result = Blackfynn::run(&config, move |ref bf| {
    ///      // Not logged in
    ///      into_future_trait(bf.organizations())
    ///    });
    ///    assert!(result.is_err());
    ///  }
    ///  ```
    ///
    #[allow(dead_code)]
    fn run<F, T>(&self, runner: F) -> bf::Result<T>
    where
        F: Fn(Blackfynn) -> bf::Future<T>,
        T: 'static + Send,
    {
        let mut rt = tokio::runtime::Runtime::new()?;
        rt.block_on(runner(self.clone()))
    }

    /// Test if the user is logged into the Blackfynn platform.
    pub fn has_session(&self) -> bool {
        self.session_token().is_some()
    }

    /// Get the current organization the user is associated with.
    pub fn current_organization(&self) -> Option<OrganizationId> {
        self.inner.lock().unwrap().current_organization.clone()
    }

    /// Set the current organization the user is associated with.
    pub fn set_current_organization(&self, id: Option<&OrganizationId>) {
        self.inner.lock().unwrap().current_organization = id.cloned()
    }

    /// Set the session token the user is associated with.
    pub fn set_session_token(&self, token: Option<SessionToken>) {
        self.inner.lock().unwrap().session_token = token;
    }

    /// Set the active environment
    pub fn set_environment(&self, env: Environment) {
        self.inner.lock().unwrap().config = Config::new(env);
    }

    /// Log in to the Blackfynn API.
    ///
    /// If successful, the Blackfynn client will store the resulting session
    /// token for subsequent API calls.
    #[allow(dead_code)]
    pub fn login<S: Into<String>>(
        &self,
        api_key: S,
        api_secret: S,
    ) -> bf::Future<response::ApiSession> {
        let payload = request::ApiLogin::new(api_key.into(), api_secret.into());
        let this = self.clone();
        into_future_trait(
            post!(self, "/account/api/session", params!(), &payload).and_then(
                move |login_response: response::ApiSession| {
                    this.inner.lock().unwrap().session_token =
                        Some(login_response.session_token().clone());
                    Ok(login_response)
                },
            ),
        )
    }

    /// Get the current user.
    pub fn get_user(&self) -> bf::Future<model::User> {
        get!(self, "/user/")
    }

    /// Sets the preferred organization of the current user.
    pub fn set_preferred_organization(
        &self,
        organization_id: Option<OrganizationId>,
    ) -> bf::Future<model::User> {
        let this = self.clone();
        let user = request::User::with_organization(organization_id);
        into_future_trait(put!(self, "/user/", params!(), &user).and_then(
            move |user_response: model::User| {
                this.set_current_organization(user_response.preferred_organization());
                Ok(user_response)
            },
        ))
    }

    /// List the organizations the user is a member of.
    pub fn get_organizations(&self) -> bf::Future<response::Organizations> {
        get!(self, "/organizations/")
    }

    /// Get a specific organization.
    pub fn get_organization_by_id(&self, id: OrganizationId) -> bf::Future<response::Organization> {
        get!(self, route!("/organizations/{id}", id))
    }

    /// Get a listing of the datasets the current user has access to.
    pub fn get_datasets(&self) -> bf::Future<Vec<response::Dataset>> {
        get!(self, "/datasets/")
    }

    /// Create a new dataset.
    pub fn create_dataset(
        &self,
        payload: request::dataset::Create,
    ) -> bf::Future<response::Dataset> {
        post!(self, "/datasets/", params!(), payload!(payload))
    }

    /// Get a specific dataset.
    pub fn get_dataset_by_id(&self, id: DatasetId) -> bf::Future<response::Dataset> {
        get!(self, route!("/datasets/{id}", id))
    }

    /// Get the collaborators of the data set.
    pub fn get_dataset_collaborators(&self, id: DatasetId) -> bf::Future<response::Collaborators> {
        get!(self, route!("/datasets/{id}/collaborators", id))
    }

    /// Share this data set with another user.
    pub fn share_dataset(
        &self,
        id: DatasetId,
        users: Vec<UserId>,
    ) -> bf::Future<response::CollaboratorChanges> {
        put!(
            self,
            route!("/datasets/{id}/collaborators", id),
            params!(),
            payload!(users)
        )
    }

    /// Unshare this data set with another user.
    pub fn unshare_dataset(
        &self,
        id: DatasetId,
        users: Vec<UserId>,
    ) -> bf::Future<response::CollaboratorChanges> {
        delete!(
            self,
            route!("/datasets/{id}/collaborators", id),
            params!(),
            payload!(users)
        )
    }

    /// Update an existing dataset.
    pub fn update_dataset(
        &self,
        id: DatasetId,
        payload: request::dataset::Update,
    ) -> bf::Future<response::Dataset> {
        put!(
            self,
            route!("/datasets/{id}", id),
            params!(),
            payload!(payload)
        )
    }

    /// Delete an existing dataset.
    pub fn delete_dataset(&self, id: DatasetId) -> bf::Future<()> {
        let f: bf::Future<response::EmptyMap> = delete!(self, route!("/datasets/{id}", id));
        into_future_trait(f.map(|_| ()))
    }

    /// Create a new package.
    pub fn create_package(
        &self,
        payload: request::package::Create,
    ) -> bf::Future<response::Package> {
        post!(self, "/packages/", params!(), payload!(payload))
    }

    /// Get a specific package.
    pub fn get_package_by_id(&self, id: PackageId) -> bf::Future<response::Package> {
        get!(self, route!("/packages/{id}", id))
    }

    /// Get the source files that are part of a package.
    pub fn get_package_sources(&self, id: PackageId) -> bf::Future<response::Files> {
        get!(self, route!("/packages/{id}/sources", id))
    }

    /// Update an existing package.
    pub fn update_package(
        &self,
        id: PackageId,
        payload: request::package::Update,
    ) -> bf::Future<response::Package> {
        put!(
            self,
            route!("/packages/{id}", id),
            params!(),
            payload!(payload)
        )
    }

    /// Get the members that belong to the current users organization.
    pub fn get_members(&self) -> bf::Future<Vec<model::User>> {
        into_future_trait(match self.current_organization() {
            Some(org) => self.get_members_by_organization(org),
            None => into_future_trait(future::err::<_, bf::Error>(
                bf::error::ErrorKind::NoOrganizationSetError.into(),
            )),
        })
    }

    /// Get the members that belong to the specified organization.
    pub fn get_members_by_organization(&self, id: OrganizationId) -> bf::Future<Vec<model::User>> {
        get!(self, route!("/organizations/{id}/members", id))
    }

    /// Get the members that belong to the current users organization.
    pub fn get_teams(&self) -> bf::Future<Vec<response::Team>> {
        into_future_trait(match self.current_organization() {
            Some(org) => self.get_teams_by_organization(org),
            None => into_future_trait(future::err::<_, bf::Error>(
                bf::error::ErrorKind::NoOrganizationSetError.into(),
            )),
        })
    }

    /// Get the teams that belong to the specified organization.
    pub fn get_teams_by_organization(&self, id: OrganizationId) -> bf::Future<Vec<response::Team>> {
        get!(self, route!("/organizations/{id}/teams", id))
    }

    /// Grant temporary upload access to the specific dataset for the current session.
    pub fn grant_upload(&self, id: DatasetId) -> bf::Future<response::UploadCredential> {
        get!(self, route!("/security/user/credentials/upload/{id}", id))
    }

    /// Grant temporary streaming access for the current user.
    pub fn grant_streaming(&self) -> bf::Future<response::TemporaryCredential> {
        get!(self, "/security/user/credentials/streaming")
    }

    /// Generate a preview of the files to be uploaded.
    pub fn preview_upload<P, Q>(
        &self,
        path: P,
        files: &[Q],
        append: bool,
    ) -> bf::Future<response::UploadPreview>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        let results = files
            .into_iter()
            .enumerate()
            .map(|(id, file)| {
                model::S3File::new(path.as_ref(), file.as_ref(), Some(Into::into(id as u64)))
            }).collect::<Result<Vec<_>, _>>();

        let s3_files = match results {
            Ok(good) => good,
            Err(e) => return into_future_trait(future::err(e)),
        };

        post!(
            self,
            "/files/upload/preview",
            params!("append" => if append { "true" } else { "false" }),
            &request::UploadPreview::new(&s3_files)
        )
    }

    /// Get a S3 uploader.
    pub fn s3_uploader(&self, creds: TemporaryCredential) -> bf::Result<S3Uploader> {
        let (access_key, secret_key, session_token) = creds.take();
        S3Uploader::new(
            self.inner
                .lock()
                .unwrap()
                .config
                .s3_server_side_encryption()
                .clone(),
            access_key,
            secret_key,
            session_token,
        )
    }

    /// Completes the file upload process.
    pub fn complete_upload(
        &self,
        import_id: ImportId,
        dataset_id: DatasetId,
        destination_id: Option<&PackageId>,
        append: bool,
    ) -> bf::Future<response::Manifests> {
        let mut params = params!(
            "append" => if append { "true" } else { "false" },
            "datasetId" => dataset_id
        );
        if let Some(dest_id) = destination_id {
            params.push(param!("destinationId", dest_id.clone()));
        }

        post!(
            self,
            route!("/files/upload/complete/{import_id}", import_id),
            params
        )
    }

    /// Upload a file using the upload service.
    pub fn upload_using_upload_service<P>(
        &self,
        organization_id: &OrganizationId,
        import_id: &ImportId,
        filepath: P,
    ) -> bf::Future<response::UploadResponse>
    where
        P: AsRef<Path>,
    {
        self.request_chunked(
            route!(
                "/upload/organizations/{organization_id}/id/{import_id}",
                organization_id,
                import_id
            ),
            params!(
                "filename" => filepath.as_ref().file_name().unwrap().to_str().unwrap()
            ),
            filepath,
        )
    }

    pub fn complete_upload_using_upload_service(
        &self,
        import_id: &ImportId,
        dataset_id: &DatasetId,
    ) -> bf::Future<response::Manifests> {
        post!(
            self,
            route!("/files/upload/complete/{import_id}", import_id),
            params!(
                "append" => "false",
                "datasetId" => dataset_id,
                "uploadService" => "true"
            )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::{cell, fs, path, sync, thread, time};

    use bf::api::client::s3::MultipartUploadResult;
    use bf::config::Environment;
    use bf::util::futures::into_future_trait;
    use bf::util::rand_suffix;

    use error_chain::ChainedError;

    const TEST_ENVIRONMENT: Environment = Environment::Development;
    const TEST_API_KEY: &'static str = env!("BLACKFYNN_API_KEY");
    const TEST_SECRET_KEY: &'static str = env!("BLACKFYNN_SECRET_KEY");

    // "Blackfynn"
    const FIXTURE_ORGANIZATION: &'static str =
        "N:organization:c905919f-56f5-43ae-9c2a-8d5d542c133b";

    // Dedicated agent email (dev)
    #[allow(dead_code)]
    const FIXTURE_EMAIL: &'static str = "agent-test@blackfynn.com";

    // Dedicated agent user (dev)
    #[allow(dead_code)]
    const FIXTURE_USER: &'static str = "N:user:6caa1955-c39e-4198-83c6-aa8fe3afbe93";

    // "Blackfynn" (dev)
    const FIXTURE_DATASET: &'static str = "N:dataset:ef04462a-df3e-4a47-a657-f7ec80003b9e";

    // "Raw device readings" (dev)
    const FIXTURE_PACKAGE: &'static str = "N:collection:cb924124-afa9-49d8-8fdb-2135883312cf";

    lazy_static! {
        static ref CONFIG: Config = Config::new(TEST_ENVIRONMENT);
        static ref TEST_FILES: Vec<String> = test_data_files("/small");
        static ref TEST_DATA_DIR: String = test_data_dir("/small");
        static ref BIG_TEST_FILES: Vec<String> = test_data_files("/big");
        static ref BIG_TEST_DATA_DIR: String = test_data_dir("/big");
    }

    fn bf() -> Blackfynn {
        Blackfynn::new((*CONFIG).clone())
    }

    // Returns the test data directory `<project>/data/<data_dir>`:
    fn test_data_dir(data_dir: &str) -> String {
        concat!(env!("CARGO_MANIFEST_DIR"), "/test/data").to_string() + data_dir
    }

    // Returns a `Vec<String>` of test data filenames taken from the specified
    // test data directory:
    fn test_data_files(data_dir: &str) -> Vec<String> {
        match fs::read_dir(test_data_dir(data_dir)) {
            Ok(entries) => entries
                .map(|entry| entry.unwrap().file_name().into_string())
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            Err(e) => {
                eprintln!("{:?} :: {:?}", data_dir, e);
                vec![]
            }
        }
    }

    #[test]
    fn login_successfully_locally() {
        let bf = bf();
        let result = bf.run(move |bf| bf.login(TEST_API_KEY, TEST_SECRET_KEY));
        assert!(result.is_ok());
        assert!(bf.session_token().is_some());
    }

    #[test]
    fn login_fails_locally() {
        let bf = bf();
        let result = bf.run(move |bf| bf.login(TEST_API_KEY, "this-is-a-bad-secret"));
        assert!(result.is_err());
        assert!(bf.session_token().is_none());
    }

    #[test]
    fn fetching_organizations_after_login_is_successful() {
        let org = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_organizations()),
            )
        });

        if org.is_err() {
            panic!("{}", org.unwrap_err().display_chain().to_string());
        }
    }

    #[test]
    fn fetching_user_after_login_is_successful() {
        let user = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_user()),
            )
        });

        if user.is_err() {
            panic!("{}", user.unwrap_err().display_chain().to_string());
        }
    }

    #[test]
    fn updating_org_after_login_is_successful() {
        let user = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_user().map(|user| (user, bf)))
                    .and_then(move |(user, bf)| {
                        let org = user.preferred_organization().clone();
                        bf.set_preferred_organization(org.cloned()).map(|_| bf)
                    }).and_then(move |bf| bf.get_user()),
            )
        });

        if user.is_err() {
            panic!("{}", user.unwrap_err().display_chain().to_string());
        }
    }

    #[test]
    fn fetching_organizations_fails_if_login_fails() {
        let org = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, "another-bad-secret")
                    .and_then(move |_| bf.get_organizations()),
            )
        });

        assert!(org.is_err());
    }

    #[test]
    fn fetching_organization_by_id_is_successful() {
        let org = bf().run(move |bf| {
            into_future_trait(bf.login(TEST_API_KEY, TEST_SECRET_KEY).and_then(move |_| {
                bf.get_organization_by_id(OrganizationId::new(FIXTURE_ORGANIZATION))
            }))
        });

        if org.is_err() {
            panic!("{}", org.unwrap_err().display_chain().to_string());
        }
    }

    #[test]
    fn fetching_datasets_after_login_is_successful() {
        let ds = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_datasets()),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().display_chain().to_string());
        }
    }

    #[test]
    fn fetching_datasets_fails_if_login_fails() {
        let ds = bf().run(move |bf| into_future_trait(bf.get_datasets()));
        assert!(ds.is_err());
    }

    #[test]
    fn fetching_dataset_by_id_successful_if_logged_in_and_exists() {
        let ds = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_dataset_by_id(DatasetId::new(FIXTURE_DATASET))),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().display_chain().to_string());
        }
    }

    #[test]
    fn fetching_package_by_id_successful_if_logged_in_and_exists() {
        let package = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_package_by_id(PackageId::new(FIXTURE_PACKAGE))),
            )
        });
        if package.is_err() {
            panic!("{}", package.unwrap_err().display_chain().to_string());
        }
    }

    #[test]
    fn fetching_package_by_id_invalid_if_logged_in_and_exists() {
        let package = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_package_by_id(PackageId::new("invalid_package_id"))),
            )
        });

        if let Err(e) = package {
            match e {
                // blackfynn api returns 403 in this case..it should really be 404 I think
                bf::error::Error(bf::error::ErrorKind::ApiError(status, _), _) => {
                    assert_eq!(status.as_u16(), 404)
                }
                _ => assert!(false),
            }
        }
    }

    #[test]
    fn fetching_dataset_by_id_fails_if_logged_in_but_doesnt_exists() {
        let ds = bf().run(move |bf| {
            into_future_trait(bf.login(TEST_API_KEY, TEST_SECRET_KEY).and_then(move |_| {
                bf.get_dataset_by_id(DatasetId::new(
                    "N:dataset:not-real-6803-4a67-bf20-83076774a5c7",
                ))
            }))
        });
        assert!(ds.is_err());
    }

    #[test]
    fn fetch_members() {
        let members = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_user().map(|user| (user, bf)))
                    .and_then(move |(user, bf)| {
                        let org = user.preferred_organization().clone();
                        bf.set_preferred_organization(org.cloned()).map(|_| bf)
                    }).and_then(move |bf| bf.get_members()),
            )
        });
        assert!(members.is_ok());
    }

    #[test]
    fn fetch_teams() {
        let teams = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_user().map(|user| (user, bf)))
                    .and_then(move |(user, bf)| {
                        let org = user.preferred_organization().clone();
                        bf.set_preferred_organization(org.cloned()).map(|_| bf)
                    }).and_then(move |bf| bf.get_teams()),
            )
        });
        assert!(teams.is_ok());
    }

    #[test]
    fn creating_then_updating_then_delete_dataset_successful() {
        let result = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| {
                        bf.create_dataset(request::dataset::Create::new(
                            rand_suffix("$agent-test-dataset".to_string()),
                            Some("A test dataset created by the agent".to_string()),
                        )).map(|ds| (bf, ds))
                    }).and_then(move |(bf, ds)| Ok(ds.as_ref().id().clone()).map(|id| (bf, id)))
                    .and_then(move |(bf, id)| {
                        bf.update_dataset(
                            id.clone(),
                            request::dataset::Update::new(
                                "new-dataset-name",
                                None as Option<String>,
                            ),
                        ).map(|_| (bf, id))
                    }).and_then(move |(bf, id)| {
                        let id = id.clone();
                        bf.get_dataset_by_id(id.clone())
                            .and_then(|ds| {
                                assert_eq!(
                                    ds.into_inner().name().clone(),
                                    "new-dataset-name".to_string()
                                );
                                Ok(id)
                            }).map(|id| (bf, id))
                    }).and_then(move |(bf, id)| bf.delete_dataset(id)),
            )
        });

        if result.is_err() {
            panic!("{}", result.unwrap_err().display_chain().to_string());
        }
    }

    #[test]
    fn creating_then_updating_then_delete_package_successful() {
        let result = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| {
                        bf.create_dataset(request::dataset::Create::new(
                            rand_suffix("$agent-test-dataset".to_string()),
                            Some("A test dataset created by the agent".to_string()),
                        )).map(|ds| (bf, ds))
                    }).and_then(move |(bf, ds)| Ok(ds.as_ref().id().clone()).map(|id| (bf, id)))
                    .and_then(move |(bf, ds_id)| {
                        bf.create_package(request::package::Create::new(
                            rand_suffix("$agent-test-package"),
                            Default::default(),
                            ds_id.clone(),
                        )).map(|pkg| (bf, ds_id, pkg))
                    }).and_then(move |(bf, ds_id, pkg)| {
                        let pkg_id = pkg.into_inner().id().clone();
                        bf.update_package(
                            pkg_id.clone(),
                            request::package::Update::new("new-package-name"),
                        ).map(|_| (bf, pkg_id, ds_id))
                    }).and_then(move |(bf, pkg_id, ds_id)| {
                        bf.get_package_by_id(pkg_id).and_then(|pkg| {
                            assert_eq!(
                                pkg.into_inner().name().clone(),
                                "new-package-name".to_string()
                            );
                            Ok((bf, ds_id))
                        })
                    }).and_then(move |(bf, ds_id)| bf.delete_dataset(ds_id)),
            )
        });

        if result.is_err() {
            panic!("{}", result.unwrap_err().display_chain().to_string());
        }
    }

    #[test]
    fn fetching_upload_credential_granting_works() {
        let cred = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.grant_upload(DatasetId::new(FIXTURE_DATASET))),
            )
        });
        if cred.is_err() {
            panic!("{}", cred.unwrap_err().display_chain().to_string());
        }
    }

    #[test]
    fn preview_upload_file_working() {
        let preview = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.preview_upload(&*TEST_DATA_DIR, &*TEST_FILES, false)),
            )
        });
        if preview.is_err() {
            panic!("{}", preview.unwrap_err().display_chain().to_string());
        }
    }

    struct UploadScaffold {
        dataset_id: DatasetId,
        preview: response::UploadPreview,
        upload_credential: response::UploadCredential,
    }

    // Creates a scaffold used to build further tests for uploading:
    fn create_upload_scaffold(
        test_path: String,
        test_files: Vec<String>,
    ) -> Box<Fn(Blackfynn) -> bf::Future<(UploadScaffold, Blackfynn)>> {
        Box::new(move |bf| {
            let test_path = test_path.clone();
            let test_files = test_files.clone();

            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| {
                        bf.create_dataset(request::dataset::Create::new(
                            rand_suffix("$agent-test-dataset".to_string()),
                            Some("A test dataset created by the agent".to_string()),
                        )).map(move |ds| (bf, ds))
                    }).and_then(move |(bf, ds)| {
                        let id = ds.as_ref().id().clone();
                        bf.grant_upload(id.clone())
                            .map(move |cred| (bf, id.clone(), cred))
                    }).and_then(move |(bf, dataset_id, creds)| {
                        bf.preview_upload(test_path, &test_files, false)
                            .map(|preview| (bf, dataset_id, preview, creds))
                    }).and_then(|(bf, dataset_id, p, c)| {
                        future::ok(UploadScaffold {
                            dataset_id,
                            preview: p,
                            upload_credential: c,
                        }).join(Ok(bf))
                    }),
            )
        })
    }

    #[test]
    fn simple_file_uploading() {
        let result = bf().run(move |bf| {
            let f =
                create_upload_scaffold((*TEST_DATA_DIR).to_string(), (&*TEST_FILES).to_vec())(bf)
                    .and_then(move |(scaffold, bf)| {
                        let bf_clone = bf.clone();
                        let upload_credential = scaffold.upload_credential.clone();
                        let uploader = bf
                            .s3_uploader(
                                scaffold
                                    .upload_credential
                                    .into_inner()
                                    .take_temp_credentials(),
                            ).unwrap();
                        let dataset_id = scaffold.dataset_id.clone();
                        let outer_dataset_id = dataset_id.clone();
                        stream::futures_unordered(scaffold.preview.into_iter().map(
                            move |package| {
                                let dataset_id = dataset_id.clone();
                                let upload_credential = upload_credential.clone();
                                // Simple, non-multipart uploading:
                                uploader
                                    .put_objects(
                                        &*TEST_DATA_DIR,
                                        package.files(),
                                        package.import_id().clone(),
                                        upload_credential.into(),
                                    ).map(move |import_id| (dataset_id, import_id))
                            },
                        )).map(move |(dataset_id, import_id)| {
                            bf.complete_upload(import_id, dataset_id.clone(), None, false)
                        }).collect()
                        .map(move |fs| (bf_clone, outer_dataset_id, fs))
                    }).and_then(|(bf, dataset_id, fs)| {
                        stream::futures_unordered(fs)
                            .collect()
                            .map(|manifests| (bf, dataset_id, manifests))
                    }).and_then(|(bf, dataset_id, manifests)| {
                        let mut file_count = 0;
                        for manifest in manifests {
                            for entry in manifest.entries() {
                                let n = entry.files().len();
                                assert!(n > 0);
                                file_count += n;
                            }
                        }
                        assert_eq!(file_count, TEST_FILES.len());
                        Ok((bf, dataset_id))
                    }).and_then(move |(bf, dataset_id)| bf.delete_dataset(dataset_id));

            into_future_trait(f)
        });

        if result.is_err() {
            println!("{}", result.unwrap_err().display_chain().to_string());
            panic!();
        }
    }

    #[test]
    fn multipart_file_uploading() {
        let result = bf().run(move |bf| {
            let bf_clone = bf.clone();
            let f =
                create_upload_scaffold((*TEST_DATA_DIR).to_string(), (&*TEST_FILES).to_vec())(bf)
                    .and_then(move |(scaffold, bf)| {
                        let dataset_id = scaffold.dataset_id.clone();
                        let dataset_id_inner = scaffold.dataset_id.clone();
                        let cred = scaffold.upload_credential.clone();
                        let uploader = bf
                            .s3_uploader(
                                scaffold
                                    .upload_credential
                                    .into_inner()
                                    .take_temp_credentials(),
                            ).unwrap();
                        stream::iter_ok::<_, bf::error::Error>(scaffold.preview.into_iter().map(
                            move |package| {
                                uploader.multipart_upload_files(
                                    &*TEST_DATA_DIR,
                                    package.files(),
                                    package.import_id().clone(),
                                    cred.clone().into(),
                                )
                            },
                        )).flatten()
                        .filter_map(move |result| match result {
                            MultipartUploadResult::Complete(import_id, _) => {
                                Some(bf.complete_upload(import_id, dataset_id.clone(), None, false))
                            }
                            _ => None,
                        }).collect()
                        .map(|fs| (fs, dataset_id_inner))
                    }).and_then(|(fs, dataset_id)| {
                        stream::futures_unordered(fs)
                            .collect()
                            .map(|manifests| (dataset_id, manifests))
                    }).and_then(|(dataset_id, manifests)| {
                        let mut file_count = 0;
                        for manifest in manifests {
                            for entry in manifest.entries() {
                                let n = entry.files().len();
                                assert!(n > 0);
                                file_count += n;
                            }
                        }
                        assert_eq!(file_count, TEST_FILES.len());
                        Ok(dataset_id)
                    }).and_then(move |dataset_id| bf_clone.delete_dataset(dataset_id));

            into_future_trait(f)
        });

        if result.is_err() {
            println!("{}", result.unwrap_err().display_chain().to_string());
            panic!();
        }
    }

    #[derive(Debug)]
    enum UploadStatus<S: Debug, T: Debug> {
        Completed(S),
        Aborted(T),
    }

    #[test]
    fn multipart_big_file_uploading() {
        struct Inner(sync::Mutex<bool>);

        impl Inner {
            pub fn new() -> Self {
                Inner(sync::Mutex::new(false))
            }
        }

        struct ProgressIndicator {
            inner: sync::Arc<Inner>,
        }

        impl Clone for ProgressIndicator {
            fn clone(&self) -> Self {
                Self {
                    inner: Arc::clone(&self.inner),
                }
            }
        }

        impl ProgressIndicator {
            pub fn new() -> Self {
                Self {
                    inner: sync::Arc::new(Inner::new()),
                }
            }
        }

        impl ProgressCallback for ProgressIndicator {
            fn on_update(&self, _update: &ProgressUpdate) {
                *self.inner.0.lock().unwrap() = true;
            }
        }

        let cb = ProgressIndicator::new();

        let result = bf().run(move |bf| {
            let cb = cb.clone();

            let f = create_upload_scaffold(
                (*BIG_TEST_DATA_DIR).to_string(),
                (&*BIG_TEST_FILES).to_vec(),
            )(bf).and_then(|(scaffold, bf)| {
                let bf_clone = bf.clone();
                let cred = scaffold.upload_credential.clone();
                let dataset_id = scaffold.dataset_id.clone();
                let dataset_id_outer = dataset_id.clone();
                let mut uploader = bf
                    .s3_uploader(
                        scaffold
                            .upload_credential
                            .into_inner()
                            .take_temp_credentials(),
                    ).unwrap();
                // Check the progress of the upload by polling every 1s:
                if let Ok(mut indicator) = uploader.progress() {
                    thread::spawn(move || {
                        let done = cell::RefCell::new(HashSet::<path::PathBuf>::new());
                        loop {
                            thread::sleep(time::Duration::from_millis(1000));
                            for (path, update) in &mut indicator {
                                let p = path.to_path_buf();
                                if !done.borrow().contains(&p) {
                                    println!("{:?} => {}%", p, update.percent_done());
                                    if update.completed() {
                                        done.borrow_mut().insert(p);
                                    }
                                }
                            }
                        }
                    });
                }

                stream::iter_ok::<_, bf::error::Error>(scaffold.preview.into_iter().map(
                    move |package| {
                        let cb = cb.clone();
                        uploader.multipart_upload_files_cb(
                            &*BIG_TEST_DATA_DIR,
                            package.files(),
                            package.import_id().clone(),
                            cred.clone().into(),
                            cb,
                        )
                    },
                )).flatten()
                .map(move |result| {
                    match result {
                        MultipartUploadResult::Complete(import_id, _) => {
                            into_future_trait(
                                bf.complete_upload(import_id, dataset_id.clone(), None, false)
                                    .then(|r| {
                                        // wrap the results as an UploadStatus so we can return
                                        // errors as strictly value, rather something that will
                                        // affect the control flow of the future itself:
                                        match r {
                                            Ok(manifest) => Ok(UploadStatus::Completed(manifest)),
                                            Err(err) => Ok(UploadStatus::Aborted(err)),
                                        }
                                    }),
                            )
                        }
                        MultipartUploadResult::Abort(originating_err, _) => {
                            into_future_trait(future::ok(UploadStatus::Aborted(originating_err)))
                        }
                    }
                }).collect()
                .map(|fs| (bf_clone, fs, dataset_id_outer))
            }).and_then(|(bf, fs, dataset_id)| {
                stream::futures_unordered(fs)
                    .collect()
                    .map(|manifests| (bf, dataset_id, manifests))
            }).and_then(|(bf, dataset_id, manifests)| {
                for entry in manifests {
                    match entry {
                        UploadStatus::Completed(_) => assert!(true),
                        UploadStatus::Aborted(e) => {
                            println!("ABORTED => {:#?}", e);
                            assert!(false)
                        }
                    }
                }
                Ok((bf, dataset_id))
            }).and_then(move |(bf, dataset_id)| bf.delete_dataset(dataset_id).map(|_| ()));

            into_future_trait(f)
        });

        if result.is_err() {
            println!("{}", result.unwrap_err().display_chain().to_string());
            panic!();
        }
    }

    #[test]
    fn upload_using_upload_service() {
        // create upload
        let result = bf().run(move |bf| {
            let f = create_upload_scaffold((*TEST_DATA_DIR).to_string(), (&*TEST_FILES).to_vec())(
                bf,
            ).and_then(move |(scaffold, bf)| bf.get_user().map(|user| (user, scaffold, bf)))
            .and_then(move |(user, scaffold, bf)| {
                let org = user.preferred_organization().clone().unwrap();
                let bf_clone = bf.clone();
                let dataset_id = scaffold.dataset_id.clone();
                let dataset_id_clone = scaffold.dataset_id.clone();

                stream::futures_unordered(scaffold.preview.into_iter().map(move |package| {
                    // let mut filepath = Path::new(&TEST_DATA_DIR).to_path_buf();

                    let import_id = package.import_id().clone();
                    let bf_clone_2 = bf.clone();
                    let dataset_id_clone_2 = dataset_id.clone();

                    bf.upload_using_upload_service(
                        org,
                        &import_id,
                        Path::new(
                            "/Users/mattusifer/src/bf/blackfynn-rust/test/data/small/brain.jpg",
                        ).to_path_buf()
                        .canonicalize()
                        .unwrap(),
                    ).map(|_| (bf_clone_2, dataset_id_clone_2))
                    .and_then(move |(bf, dataset_id)| {
                        bf.complete_upload_using_upload_service(&import_id, &dataset_id)
                    })
                })).collect()
                .map(|_| (bf_clone, dataset_id_clone))
            }).and_then(move |(bf, dataset_id)| bf.delete_dataset(dataset_id));

            into_future_trait(f)
        });

        // check result
        if result.is_err() {
            println!("{}", result.unwrap_err().display_chain().to_string());
            panic!();
        }

        // TODO: check that upload exists in the platform
    }
}
