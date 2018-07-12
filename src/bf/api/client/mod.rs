// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Functions to interact with the Blackfynn platform.

pub mod s3;

pub use self::s3::{
    MultipartUploadResult, ProgressCallback, ProgressUpdate, S3Uploader, UploadProgress,
    UploadProgressIter,
};

use std::path::Path;
use std::sync::{Arc, Mutex};

use futures::*;

use hyper;
use hyper::client::{Client, HttpConnector};
use hyper_tls::HttpsConnector;

use serde;
use serde_json;

use tokio;

use super::{request, response};
use bf;
use bf::config::Config;
use bf::model::{
    self, DatasetId, ImportId, OrganizationId, PackageId, SessionToken, TemporaryCredential,
};
use bf::util::futures::into_future_trait;

// Blackfynn session authentication header:
const X_SESSION_ID: &str = "X-SESSION-ID";

struct BlackFynnImpl {
    config: Config,
    http_client: Client<HttpsConnector<HttpConnector>>,
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

// A useful alias when dealing with the fact that an endpoint does not expect
// a POST/PUT body, but a type is still expected:
type Nothing = serde_json::Value;

// =============================================================================

// Useful builder macros:
macro_rules! route {
    ($uri:expr, $($var:ident),*) => (
        format!($uri, $($var = Into::<String>::into($var))*)
    )
}

macro_rules! param {
    ($key:expr, $value:expr) => {
        ($key.into(), $value.into())
    };
}

macro_rules! get {
    ($target:expr, $route:expr) => {
        $target.request($route, hyper::Method::GET, vec![], None as Option<&Nothing>)
    };
}

macro_rules! post {
    ($target:expr, $route:expr) => {
        $target.request(
            $route,
            hyper::Method::POST,
            vec![],
            None as Option<&Nothing>,
        )
    };
    ($target:expr, $route:expr, $params:expr) => {
        $target.request(
            $route,
            hyper::Method::POST,
            $params,
            None as Option<&Nothing>,
        )
    };
    ($target:expr, $route:expr, $params:expr, $payload:expr) => {
        $target.request($route, hyper::Method::POST, $params, Some($payload))
    };
}

macro_rules! put {
    ($target:expr, $route:expr) => {
        $target.request($route, hyper::Method::PUT, vec![], None as Option<&Nothing>)
    };
    ($target:expr, $route:expr, $params:expr) => {
        $target.request(
            $route,
            hyper::Method::PUT,
            $params,
            None as Option<&Nothing>,
        )
    };
    ($target:expr, $route:expr, $params:expr, $payload:expr) => {
        $target.request($route, hyper::Method::PUT, $params, Some($payload))
    };
}

// ============================================================================

impl Blackfynn {
    pub fn new(config: Config) -> Self {
        let connector = HttpsConnector::new(4).expect("bf:couldn't create https connector");
        let http_client = Client::builder().build(connector);
        Self {
            inner: Arc::new(Mutex::new(BlackFynnImpl {
                config,
                http_client,
                session_token: None,
                current_organization: None,
            })),
        }
    }

    /// Sets the current organization the user is associated with and returns self.
    pub fn with_current_organization(self, id: OrganizationId) -> Self {
        self.inner.lock().unwrap().current_organization = Some(id);
        self
    }

    /// Sets the current organization the user is associated with and returns self.
    pub fn with_session_token(self, token: SessionToken) -> Self {
        self.inner.lock().unwrap().session_token = Some(token);
        self
    }

    fn session_token(&self) -> Option<SessionToken> {
        self.inner.lock().unwrap().session_token.clone()
    }

    fn request<I, S, P, Q>(
        &self,
        route: S,
        method: hyper::Method,
        params: I,
        payload: Option<&P>,
    ) -> bf::Future<Q>
    where
        I: IntoIterator<Item = (String, String)>,
        P: serde::Serialize,
        Q: 'static + Send + serde::de::DeserializeOwned,
        S: Into<String>,
    {
        // Build the request url: config environment base + route:
        let mut use_url = self.inner.lock().unwrap().config.env().url().clone();
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

        let body: Result<hyper::Body, serde_json::Error> = match payload {
            Some(p) => serde_json::to_string(p).map(Into::into),
            None => Ok(hyper::Body::empty()),
        };

        let body = future::result::<_, bf::error::Error>(body.map_err(Into::into))
            .map_err(|e| bf::Error::with_chain(e, "bf:request:body"));

        let f = uri.join(body)
            .and_then(move |(uri, body): (hyper::Uri, hyper::Body)| {
                let mut req = hyper::Request::builder()
                    .method(method)
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
                client
                    .request(req)
                    .map_err(|e| bf::Error::with_chain(e, "bf:request:execute"))
            })
            .and_then(|response: hyper::Response<hyper::Body>| {
                // Check the status code. And 5XX code will result in the
                // future terminating with an error containing the message
                // emitted from the API:
                let status_code = response.status();
                response
                    .into_body()
                    .concat2()
                    .map_err(|e| bf::Error::with_chain(e, "bf:request:response"))
                    .and_then(move |body: hyper::Chunk| Ok((status_code, body)))
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
                    )
                    .and_then(|body: hyper::Chunk| {
                        // Finally, attempt to parse the JSON response into a typeful representation:
                        serde_json::from_slice::<Q>(&body).map_err(move |e| {
                            let as_bytes: Vec<u8> = body.to_vec();
                            bf::Error::with_chain(
                                e,
                                format!(
                                    "bf:request:serialize - {}",
                                    String::from_utf8_lossy(&as_bytes).to_string()
                                ),
                            )
                        })
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
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(runner(self.clone()))
    }

    /// Tests if the user is logged in and has an active session.
    pub fn has_session(&self) -> bool {
        self.session_token().is_some()
    }

    /// Returns the current organization the user is associated with.
    pub fn current_organization(&self) -> Option<OrganizationId> {
        self.inner.lock().unwrap().current_organization.clone()
    }

    /// Sets the current organization the user is associated with.
    pub fn set_current_organization(&self, id: Option<&OrganizationId>) {
        self.inner.lock().unwrap().current_organization = id.cloned()
    }

    /// Sets the session token the user is associated with.
    pub fn set_session_token(&self, token: Option<SessionToken>) {
        self.inner.lock().unwrap().session_token = token;
    }

    /// Return a Future that, when resolved, logs in to the Blackfynn API.
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
            post!(self, "/account/api/session", vec![], &payload).and_then(
                move |login_response: response::ApiSession| {
                    this.inner.lock().unwrap().session_token =
                        Some(login_response.session_token().clone());
                    Ok(login_response)
                },
            ),
        )
    }

    /// Return a Future, that when, resolved returns the user
    /// associated with the session_token.
    pub fn get_user(&self) -> bf::Future<model::User> {
        get!(self, "/user/")
    }

    /// Return a Future, that when, resolved sets the current user preferred organization
    /// and returns the updated user.
    pub fn set_preferred_organization(
        &self,
        organization_id: Option<OrganizationId>,
    ) -> bf::Future<model::User> {
        let this = self.clone();
        let user = request::User {
            organization: organization_id.map(Into::into),
            ..Default::default()
        };
        into_future_trait(put!(self, "/user/", vec![], &user).and_then(
            move |user_response: model::User| {
                this.set_current_organization(user_response.preferred_organization());
                Ok(user_response)
            },
        ))
    }

    /// Return a Future, that when, resolved returns a listing of the
    /// organizations the user is a member of.
    pub fn organizations(&self) -> bf::Future<response::Organizations> {
        get!(self, "/organizations/")
    }

    /// Return a Future, that when, resolved returns the specified organization.
    pub fn organization_by_id(&self, id: OrganizationId) -> bf::Future<response::Organization> {
        get!(self, route!("/organizations/{id}", id))
    }

    /// Return a Future, that when, resolved returns a listing of the
    /// datasets the user has access to.
    pub fn datasets(&self) -> bf::Future<Vec<response::Dataset>> {
        get!(self, "/datasets/")
    }

    /// Return a Future, that when, resolved returns the specified dataset.
    pub fn dataset_by_id(&self, id: DatasetId) -> bf::Future<response::Dataset> {
        get!(self, route!("/datasets/{id}", id))
    }

    /// Return a Future, that when, resolved returns the specified package.
    pub fn package_by_id(&self, id: PackageId) -> bf::Future<response::Package> {
        get!(self, route!("/packages/{id}", id))
    }

    /// Grant temporary upload access to the specific dataset for the current session.
    pub fn grant_upload(&self, dataset_id: DatasetId) -> bf::Future<response::UploadCredential> {
        get!(
            self,
            route!("/security/user/credentials/upload/{dataset_id}", dataset_id)
        )
    }

    /// Grant temporary streaming access for the current session.
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
            })
            .collect::<Result<Vec<_>, _>>();

        let s3_files = match results {
            Ok(good) => good,
            Err(e) => return into_future_trait(future::err(e)),
        };

        post!(
            self,
            "/files/upload/preview",
            vec![param!("append", if append { "true" } else { "false" })],
            &request::UploadPreview::new(&s3_files)
        )
    }

    /// Returns a S3 uploader.
    pub fn s3_uploader(&self, creds: TemporaryCredential) -> S3Uploader {
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
    ) -> bf::Future<response::Manifest> {
        let mut params = vec![param!("append", if append { "true" } else { "false" })];
        params.push(param!("datasetId", dataset_id));
        if let Some(dest_id) = destination_id {
            params.push(param!("destinationId", dest_id.clone()));
        }

        post!(
            self,
            route!("/files/upload/complete/{import_id}", import_id),
            params
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

    use error_chain::ChainedError;

    const TEST_ENVIRONMENT: Environment = Environment::Development;
    const TEST_API_KEY: &'static str = env!("BLACKFYNN_API_KEY");
    const TEST_SECRET_KEY: &'static str = env!("BLACKFYNN_SECRET_KEY");

    // "Blackfynn"
    const FIXTURE_ORGANIZATION: &'static str =
        "N:organization:c905919f-56f5-43ae-9c2a-8d5d542c133b";

    // "Blackfynn"
    const FIXTURE_DATASET: &'static str = "N:dataset:5a6779a4-e3d8-473f-91d0-0a99f144dc44";

    // "Blackfynn"
    const FIXTURE_PACKAGE: &'static str = "N:collection:ff596451-9525-496b-9618-dccce356d4f4";

    lazy_static! {
        static ref CONFIG: Config = Config::new(TEST_ENVIRONMENT);
        static ref TEST_FILES: Vec<String> = test_data_files("/small");
        static ref TEST_DATA_DIR: String = test_data_dir("/small");
        static ref BIG_TEST_FILES: Vec<String> = test_data_files("/big");
        static ref BIG_TEST_DATA_DIR: String = test_data_dir("/big");
        static ref TEST_DATASET: DatasetId = DatasetId::new(FIXTURE_DATASET);
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
                    .and_then(move |_| bf.organizations()),
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
                    })
                    .and_then(move |bf| bf.get_user()),
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
                    .and_then(move |_| bf.organizations()),
            )
        });

        assert!(org.is_err());
    }

    #[test]
    fn fetching_organization_by_id_is_successful() {
        let org = bf().run(move |bf| {
            into_future_trait(bf.login(TEST_API_KEY, TEST_SECRET_KEY).and_then(move |_| {
                bf.organization_by_id(OrganizationId::new(FIXTURE_ORGANIZATION))
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
                    .and_then(move |_| bf.datasets()),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().display_chain().to_string());
        }
    }

    #[test]
    fn fetching_datasets_fails_if_login_fails() {
        let ds = bf().run(move |bf| into_future_trait(bf.datasets()));
        assert!(ds.is_err());
    }

    #[test]
    fn fetching_dataset_by_id_successful_if_logged_in_and_exists() {
        let ds = bf().run(move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.dataset_by_id(DatasetId::new(FIXTURE_DATASET))),
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
                    .and_then(move |_| bf.package_by_id(PackageId::new(FIXTURE_PACKAGE))),
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
                    .and_then(move |_| bf.package_by_id(PackageId::new("invalid_package_id"))),
            )
        });

        if let Err(e) = package {
            match e {
                // blackfynn api returns 403 in this case..it should really be 404 I think
                bf::error::Error(bf::error::ErrorKind::ApiError(status, _), _) => {
                    assert_eq!(status.as_u16(), 403)
                }
                _ => assert!(false),
            }
        }
    }

    #[test]
    fn fetching_dataset_by_id_fails_if_logged_in_but_doesnt_exists() {
        let ds = bf().run(move |bf| {
            into_future_trait(bf.login(TEST_API_KEY, TEST_SECRET_KEY).and_then(move |_| {
                bf.dataset_by_id(DatasetId::new(
                    "N:dataset:not-real-6803-4a67-bf20-83076774a5c7",
                ))
            }))
        });
        assert!(ds.is_err());
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
        preview: response::UploadPreview,
        upload_credential: response::UploadCredential,
    }

    // Creates a scaffold used to build further tests for uploading:
    fn create_upload_scaffold(
        test_path: String,
        test_files: Vec<String>,
        dataset_id: DatasetId,
    ) -> Box<Fn(Blackfynn) -> bf::Future<(UploadScaffold, Blackfynn)>> {
        Box::new(move |bf| {
            let dataset_id = dataset_id.clone();
            let test_path = test_path.clone();
            let test_files = test_files.clone();

            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.grant_upload(dataset_id.clone()).map(|cred| (cred, bf)))
                    .and_then(move |(creds, bf)| {
                        bf.preview_upload(test_path, &test_files, false)
                            .map(|preview| (preview, creds, bf))
                    })
                    .and_then(|(p, c, bf)| {
                        future::ok(UploadScaffold {
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
            let dataset_id = (&*TEST_DATASET).clone();
            let f = create_upload_scaffold(
                (*TEST_DATA_DIR).to_string(),
                (&*TEST_FILES).to_vec(),
                dataset_id.clone(),
            )(bf)
                .and_then(move |(scaffold, bf)| {
                let upload_credential = scaffold.upload_credential.clone();
                let uploader = bf.s3_uploader(
                    scaffold
                        .upload_credential
                        .into_inner()
                        .take_temp_credentials(),
                );
                stream::futures_unordered(scaffold.preview.into_iter().map(move |package| {
                    let upload_credential = upload_credential.clone();
                    // Simple, non-multipart uploading:
                    uploader.put_objects(
                        &*TEST_DATA_DIR,
                        package.files(),
                        package.import_id().clone(),
                        upload_credential.into(),
                    )
                })).map(move |import_id| {
                    bf.complete_upload(import_id, dataset_id.clone(), None, false)
                })
                    .collect()
            })
                .and_then(|fs| stream::futures_unordered(fs).collect());
            into_future_trait(f)
        });

        if result.is_err() {
            println!("{}", result.unwrap_err().display_chain().to_string());
            panic!();
        }

        let manifests = result.unwrap();
        let mut file_count = 0;
        assert!(manifests.len() > 0);
        for manifest in manifests {
            for entry in manifest.entries() {
                let n = entry.files().len();
                assert!(n > 0);
                file_count += n;
            }
        }
        assert_eq!(file_count, TEST_FILES.len());
    }

    #[test]
    fn multipart_file_uploading() {
        let result = bf().run(move |bf| {
            let dataset_id = (&*TEST_DATASET).clone();

            let f = create_upload_scaffold(
                (*TEST_DATA_DIR).to_string(),
                (&*TEST_FILES).to_vec(),
                dataset_id.clone(),
            )(bf)
                .and_then(move |(scaffold, bf)| {
                let cred = scaffold.upload_credential.clone();
                let uploader = bf.s3_uploader(
                    scaffold
                        .upload_credential
                        .into_inner()
                        .take_temp_credentials(),
                );
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
                    })
                    .collect()
            })
                .and_then(|fs| stream::futures_unordered(fs).collect());

            into_future_trait(f)
        });

        if result.is_err() {
            println!("{}", result.unwrap_err().display_chain().to_string());
            panic!();
        }

        let manifests = result.unwrap();
        let mut file_count = 0;
        assert!(manifests.len() > 0);
        for manifest in manifests {
            for entry in manifest.entries() {
                let n = entry.files().len();
                assert!(n > 0);
                file_count += n;
            }
        }
        assert_eq!(file_count, TEST_FILES.len());
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
            let dataset_id = (&*TEST_DATASET).clone();
            let cb = cb.clone();

            let f = create_upload_scaffold(
                (*BIG_TEST_DATA_DIR).to_string(),
                (&*BIG_TEST_FILES).to_vec(),
                dataset_id.clone(),
            )(bf)
                .and_then(|(scaffold, bf)| {
                let cred = scaffold.upload_credential.clone();
                let mut uploader = bf.s3_uploader(
                    scaffold
                        .upload_credential
                        .into_inner()
                        .take_temp_credentials(),
                );

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
                                into_future_trait(bf.complete_upload(
                                    import_id,
                                    dataset_id.clone(),
                                    None,
                                    false,
                                ).then(|r| {
                                    // wrap the results as an UploadStatus so we can return
                                    // errors as strictly value, rather something that will
                                    // affect the control flow of the future itself:
                                    match r {
                                        Ok(manifest) => Ok(UploadStatus::Completed(manifest)),
                                        Err(err) => Ok(UploadStatus::Aborted(err)),
                                    }
                                }))
                            }
                            MultipartUploadResult::Abort(originating_err, _) => into_future_trait(
                                future::ok(UploadStatus::Aborted(originating_err)),
                            ),
                        }
                    })
                    .collect()
            })
                .and_then(|fs| stream::futures_unordered(fs).collect());
            into_future_trait(f)
        });

        if result.is_err() {
            println!("{}", result.unwrap_err().display_chain().to_string());
            panic!();
        }

        if let Ok(manifests) = result {
            for entry in manifests {
                match entry {
                    UploadStatus::Completed(_) => assert!(true),
                    UploadStatus::Aborted(e) => {
                        println!("ABORTED => {:#?}", e);
                        assert!(false)
                    }
                }
            }
        }
    }
}
