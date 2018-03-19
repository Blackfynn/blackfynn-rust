// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

pub mod get;
pub mod post;

use std::path::Path;
use std::cell::RefCell;
use std::rc::Rc;

use futures::*;

use hyper;
use hyper::client::{Client, HttpConnector};
use hyper_tls::HttpsConnector;

use rusoto_core::reactor::RequestDispatcher;
use rusoto_credential::StaticProvider;
use rusoto_s3::{PutObjectRequest, S3, S3Client};

use serde;
use serde_json;

use tokio_core::reactor::{Core, Handle};

use bf::{self, model};
use bf::config::Config;
use bf::api::{request, response};
use bf::api::client::get::Get;
use bf::api::client::post::Post;
use bf::model::{ImportId, DatasetId, PackageId, SessionToken, OrganizationId, S3File, UploadCredential};

/// A custom session ID header for the Blackfynn API
header! { (XSessionId, "X-SESSION-ID") => [String] }

pub struct BlackFynnImpl {
    config: Config,
    http_client: Client<HttpsConnector<HttpConnector>>,
    session_token: Option<SessionToken>,
    current_organization: Option<OrganizationId>
}

/// The Blackfynn web client
pub struct Blackfynn {
    // See https://users.rust-lang.org/t/best-pattern-for-async-update-of-self-object/15205
    // for notes on this pattern:
    inner: Rc<RefCell<BlackFynnImpl>>
}

impl Clone for Blackfynn {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner)
        }
    }
}

// A useful alias when dealing with the fact that an endpoint does not expect
// a POST/PUT body, but a type is still expected:
type Nothing = serde_json::Value;

// === Request ================================================================

trait Request<T>: Future<Item=T, Error=bf::error::Error> {
    fn new_request(&self) -> bf::Future<T>;
}

// ============================================================================

impl Blackfynn {
    pub fn new(handle: &Handle, config: Config) -> Self {
        let http_client = Client::configure()
            .connector(HttpsConnector::new(4, handle).unwrap())
            .build(&handle);
        Self {
            inner: Rc::new(RefCell::new(BlackFynnImpl {
                config,
                http_client,
                session_token: None,
                current_organization: None
            }))
        }
    }

    /// Sets the current organization the user is associated with and returns self.
    pub fn with_current_organization(self, id: OrganizationId) -> Self {
        self.inner.borrow_mut().current_organization = Some(id);
        self
    }

    /// Sets the current organization the user is associated with and returns self.
    pub fn with_session_token(self, token: SessionToken) -> Self {
        self.inner.borrow_mut().session_token = Some(token);
        self
    }

    fn session_token(&self) -> Option<SessionToken> {
        self.inner.borrow().session_token.clone()
    }

    fn request<I, S, P, Q>
        (&self, route: S, method: hyper::Method, params: I, payload: Option<&P>) -> bf::Future<Q>
        where I: IntoIterator<Item=(String, String)>,
              P: serde::Serialize,
              Q: 'static + serde::de::DeserializeOwned,
              S: Into<String>
    {
        // Build the request url: config environment base + route:
        let mut use_url = self.inner.borrow().config.env().url().clone();
        use_url.set_path(&route.into());

        // If query parameters are provided, add them to the constructed URL:
        for (k, v) in params {
            use_url.query_pairs_mut().append_pair(k.as_str(), v.as_str());
        }

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
                // By default make every content type "application/json"
                {
                    req.headers_mut().set(hyper::header::ContentType::json());
                }
                // Make the actual request:
                bf.inner.borrow().http_client.request(req).map_err(|e| e.into())
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
                        // Finally, attempt to parse the JSON response into a typeful representation:
                        serde_json::from_slice::<Q>(&body).map_err(|e| e.into())
                    })
            })
        )
    }

    #[allow(dead_code)]
    ///
    ///# Example
    ///
    ///  ```
    ///  extern crate blackfynn;
    ///
    ///  fn main() {
    ///    use blackfynn::{Blackfynn, Config, Environment};
    ///
    ///    let config = Config::new(Environment::Development);
    ///    let result = Blackfynn::run(config, move |ref bf| {
    ///      // Not logged in
    ///      Box::new(bf.organizations())
    ///    });
    ///    assert!(result.is_err());
    ///  }
    ///  ```
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
    ///     let config = Config::new(Environment::Development);
    ///     let result = Blackfynn::run_with_core(&mut core, config, move |ref bf| {
    ///       // Not logged in
    ///       Box::new(bf.organizations())
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

    /// Returns the current organization the user is associated with.
    pub fn current_organization(&self) -> Option<OrganizationId> {
        self.inner.borrow().current_organization.clone()
    }

    /// Sets the current organization the user is associated with.
    pub fn set_current_organization(&self, id: &OrganizationId) {
        self.inner.borrow_mut().current_organization = Some(id.clone())
    }

    /// Return a Future that, when resolved, logs in to the Blackfynn API.
    /// If successful, the Blackfynn client will store the resulting session
    /// token for subsequent API calls.
    #[allow(dead_code)]
    pub fn login<S: Into<String>>(&self, email: S, password: S) -> bf::Future<response::Login> {
        let this = self.clone();
        Box::new(
            Post::<request::Login, response::Login>::new(self, "/account/login")
            .body(request::Login::new(email.into(), password.into()))
            .and_then(move |login_response: response::Login| {
                this.inner.borrow_mut().session_token = login_response.session_token.clone();
                Ok(login_response)
            })
        )
    }

    /// Return a Future, that when, resolved returns a listing of the
    /// organizations the user is a member of.
    pub fn organizations(&self) -> Get<response::Organizations> {
        Get::new(self, "/organizations/")
    }

    /// Return a Future, that when, resolved returns the specified organization.
    pub fn organization_by_id(&self, id: OrganizationId) -> Get<response::Organization> {
        Get::new(self, format!("/organizations/{id}", id=Into::<String>::into(id)))
    }

    /// Return a Future, that when, resolved returns a listing of the
    /// datasets the user has access to.
    pub fn datasets(&self) -> Get<Vec<response::Dataset>> {
        Get::new(self, "/datasets/")
    }

    /// Return a Future, that when, resolved returns the specified dataset.
    pub fn dataset_by_id(&self, id: DatasetId) -> Get<response::Dataset> {
        Get::new(self, format!("/datasets/{id}", id=Into::<String>::into(id)))
    }

    /// Grant temporary upload access to the specific dataset for the current session.
    pub fn grant_upload(&self, dataset_id: DatasetId) -> Get<response::UploadCredential> {
        Get::new(self, format!("/security/user/credentials/upload/{dataset}", dataset=Into::<String>::into(dataset_id)))
    }

    /// Grant temporary streaming access for the current session.
    pub fn grant_streaming(&self) ->Get<response::TemporaryCredential> {
        Get::new(self, format!("/security/user/credentials/streaming"))
    }

    /// Generate a preview of the files to be uploaded.
    pub fn preview_upload<P, Q>(&self, path: P, files: &Vec<Q>, append: bool) -> bf::Future<response::PreviewPackage>
        where P: AsRef<Path>,
              Q: AsRef<Path>
    {
        // Parition the S3 files into two groups, successes and failures:
        let (good, bad): (Vec<_>, Vec<_>) = files.into_iter().map(|file| model::S3File::new(path.as_ref(), file.as_ref())).partition(Result::is_ok);

        // If a filaure occurred, return immediately:
        let mut errs: Vec<bf::error::Error> = bad.into_iter().map(Result::unwrap_err).collect();
        if let Some(err) = errs.pop() {
            return Box::new(future::err(err));
        }

        let s3_files: Vec<model::S3File> = good.into_iter().map(Result::unwrap).collect();

        Box::new(Post::new(self, "/files/upload/preview")
                 .param("append", if append { "true" } else { "false" })
                 .body(request::PreviewPackage::new(&s3_files)))
    }

    /// Produces a future::stream::Stream, where each entry represents a Future
    /// that uploads a single file to the Blackfynn S3 bucket.
    pub fn upload_to_s3<P: AsRef<Path>>
        (&self, path: P, files: &Vec<S3File>, import_id: &ImportId, credentials: &UploadCredential) -> bf::Future<ImportId>
    {
        let import_id = import_id.clone();

        let temp_credentials = credentials.temp_credentials();

        let credentials_provider = StaticProvider::new(temp_credentials.access_key().clone().into(),
                                                       temp_credentials.secret_key().clone().into(),
                                                       Some(Into::<String>::into(temp_credentials.session_token().clone())),
                                                       None);

        let f = stream::futures_unordered(files.iter().map(|file: &S3File| {

            let this_config = self.clone();
            let config = &this_config.inner.borrow().config;

            let credentials_provider = credentials_provider.clone();

            let s3_server_side_encryption: String = config.s3_server_side_encryption().clone().into();
            let s3_encryption_key_id: String = credentials.encryption_key_id().clone().into();
            let s3_bucket: model::S3Bucket = credentials.s3_bucket().clone();
            let s3_upload_key: model::S3UploadKey = credentials.s3_key().to_upload_key(&import_id, file.file_name());
            let s3_key: model::S3Key = s3_upload_key.clone().into();

            file.read_contents(path.as_ref())
                .and_then(move |contents: Vec<u8>| {
                    let s3_client = S3Client::new(RequestDispatcher::default(),
                                                  credentials_provider,
                                                  Default::default());
                    let request = PutObjectRequest {
                        acl: None,
                        body: Some(contents),
                        bucket: s3_bucket.into(),
                        cache_control: None,
                        content_disposition: None,
                        content_encoding: None,
                        content_language: None,
                        content_length: None,
                        content_md5: None,
                        content_type: None,
                        expires: None,
                        grant_full_control: None,
                        grant_read: None,
                        grant_read_acp: None,
                        grant_write_acp: None,
                        key: s3_key.into(),
                        metadata: None,
                        request_payer: None,
                        sse_customer_algorithm: None,
                        sse_customer_key: None,
                        sse_customer_key_md5: None,
                        ssekms_key_id: Some(s3_encryption_key_id),
                        server_side_encryption: Some(s3_server_side_encryption),
                        storage_class: None,
                        tagging: None,
                        website_redirect_location: None
                    };
                    s3_client.put_object(&request).map_err(|e| e.into())
                })
        }))
        .into_future()
        .map_err(|(e, _)| e)
        .and_then(|_| Ok(import_id));

        Box::new(f)
    }

    /// Completes the file upload process.
    pub fn complete_upload(&self,
                           import_id: &ImportId,
                           dataset_id: &DatasetId,
                           destination_id: Option<&PackageId>,
                           append: bool) -> Post<Nothing, response::Manifest>
    {
        let mut p = Post::new(self, format!("/files/upload/complete/{import_id}", import_id=AsRef::<str>::as_ref(import_id)))
            .param("append", if append { "true" } else { "false" })
            .param("datasetId", dataset_id.as_ref());
        if let Some(dest_id) = destination_id {
            p = p.param("destinationId", dest_id.as_ref());
        }
        p
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use tokio_core::reactor::{Core};

    use bf::config::{Environment};
    use bf::util::futures::{return2, return3};

    const TEST_DATA_DIR: &'static str = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data");
    const TEST_ENVIRONMENT: Environment = Environment::Development;
    const TEST_USER_EMAIL: &'static str = env!("BLACKFYNN_RUST_API_USER");
    const TEST_PASSWORD: &'static str = env!("BLACKFYNN_RUST_API_PASSWORD");

    // "Blackfynn"
    const FIXTURE_ORGANIZATION: &'static str = "N:organization:c905919f-56f5-43ae-9c2a-8d5d542c133b";
    // "Blackfynn"
    const FIXTURE_DATASET: &'static str = "N:dataset:5a6779a4-e3d8-473f-91d0-0a99f144dc44";

    fn create_bf_client() -> (Blackfynn, Core) {
        let core = Core::new().expect("couldn't create tokio core");
        let handle = core.handle();
        let config = Config::new(TEST_ENVIRONMENT);
        let bf = Blackfynn::new(&handle, config);
        (bf, core)
    }

    // Returns a `Vec<String>` of filenames taken from the test directory
    // `TEST_DATA_DIR`.
    fn test_data_files() -> Vec<String> {
        fs::read_dir(TEST_DATA_DIR).unwrap().filter_map(Result::ok).map(|entry| {
            entry.file_name().into_string().unwrap()
        })
        .collect()
    }

    #[test]
    fn login_successfully_locally() {
        let (bf, mut core) = create_bf_client();
        let login = bf.login(TEST_USER_EMAIL, TEST_PASSWORD)
            .then(|r| {
                assert!(r.is_ok());
                future::result(r)
            });
        assert!(core.run(login).is_ok());
        assert!(bf.session_token().is_some());
    }

    #[test]
    fn login_fails_locally() {
        let (bf, mut core) = create_bf_client();
        let login = bf.login(TEST_USER_EMAIL, "this-is-a-bad-password")
            .then(|r| {
                assert!(r.is_err());
                future::result(r)
            });
        assert!(core.run(login).is_err());
        assert!(bf.session_token().is_none());
    }

    #[test]
    fn fetching_organizations_after_login_is_successful() {
        let config = Config::new(TEST_ENVIRONMENT);
        let org = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login(TEST_USER_EMAIL, TEST_PASSWORD)
                .and_then(move |_| {
                    bf.organizations()
                })
            )
        });
        assert!(org.is_ok());
    }

    #[test]
    fn fetching_organizations_fails_if_login_fails() {
        let config = Config::new(TEST_ENVIRONMENT);
        let org = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login(TEST_USER_EMAIL, "another-bad-password")
                .and_then(move |_| {
                    bf.organizations()
                })
            )
        });
        assert!(org.is_err());
    }

    #[test]
    fn fetching_organization_by_id_is_successful() {
        let config = Config::new(TEST_ENVIRONMENT);
        let org = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login(TEST_USER_EMAIL, TEST_PASSWORD)
                .and_then(move |_| {
                    bf.organization_by_id(OrganizationId::new(FIXTURE_ORGANIZATION))
                })
            )
        });
        assert!(org.is_ok());
    }

    #[test]
    fn fetching_datasets_after_login_is_successful() {
        let config = Config::new(TEST_ENVIRONMENT);
        let ds = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login(TEST_USER_EMAIL, TEST_PASSWORD)
                .and_then(move |_| {
                    bf.datasets()
                })
            )
        });
        assert!(ds.is_ok());
    }

    #[test]
    fn fetching_datasets_fails_if_login_fails() {
        let config = Config::new(TEST_ENVIRONMENT);
        let ds = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.datasets()
            )
        });
        assert!(ds.is_err());
    }

    #[test]
    fn fetching_dataset_by_id_successful_if_logged_in_and_exists() {
        let config = Config::new(TEST_ENVIRONMENT);
        let ds = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login(TEST_USER_EMAIL, TEST_PASSWORD)
                .and_then(move |_| {
                    bf.dataset_by_id(DatasetId::new(FIXTURE_DATASET))
                })
            )
        });
        assert!(ds.is_ok());
    }

    #[test]
    fn fetching_dataset_by_id_fails_if_logged_in_but_doesnt_exists() {
        let config = Config::new(TEST_ENVIRONMENT);
        let ds = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login(TEST_USER_EMAIL, TEST_PASSWORD)
                .and_then(move |_| {
                    bf.dataset_by_id(DatasetId::new("N:dataset:not-real-6803-4a67-bf20-83076774a5c7"))
                })
            )
        });
        assert!(ds.is_err());
    }

    #[test]
    fn fetching_upload_credential_granting_works() {
        let config = Config::new(TEST_ENVIRONMENT);
        let cred = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login(TEST_USER_EMAIL, TEST_PASSWORD)
                .and_then(move |_| {
                    bf.grant_upload(DatasetId::new(FIXTURE_DATASET))
                })
            )
        });
        assert!(cred.is_ok());
    }

    #[test]
    fn preview_upload_file_working() {
        let config = Config::new(TEST_ENVIRONMENT);
        let preview = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login(TEST_USER_EMAIL, TEST_PASSWORD)
                .and_then(move |_| {
                    bf.preview_upload(TEST_DATA_DIR, &test_data_files(), false)
                })
            )
        });
        assert!(preview.is_ok());
    }

    #[test]
    fn full_end_to_end_file_uploading_process_works() {
        let config = Config::new(TEST_ENVIRONMENT);
        let test_files = test_data_files();
        let num_test_files = test_files.len();
        let dataset_id = DatasetId::new(FIXTURE_DATASET);
        let result = Blackfynn::run(config, move |bf| {
            let dataset_id = dataset_id.clone();
            let test_files = test_files.clone();
            Box::new(
                bf.login(TEST_USER_EMAIL, TEST_PASSWORD)
                .and_then(move |login_response| {
                    Ok(login_response.profile.expect("missing user profile"))
                })
                .and_then(move |_user| {
                    return2(
                        bf.grant_upload(dataset_id.clone()),
                        future::ok(bf)
                    )
                    .and_then(move |(upload_credential, bf)| {
                        return3(
                            bf.preview_upload(TEST_DATA_DIR, &test_files, false),
                            future::ok(upload_credential),
                            future::ok(bf)
                        )
                    })
                    .and_then(move |(preview_package, upload_credential, bf)| {
                        let bf2 = bf.clone();
                        return2(
                            stream::futures_unordered(preview_package.packages.iter().map(move |package| {
                                bf.upload_to_s3(TEST_DATA_DIR, package.files(), package.import_id(), &upload_credential.clone().into())
                            }))
                            .collect(),
                            future::ok(bf2)
                        )
                    })
                    .and_then(move |(import_ids, bf): (Vec<ImportId>, Blackfynn)| {
                        stream::futures_unordered(import_ids.iter().map(move |import_id: &ImportId| {
                            bf.complete_upload(import_id, &dataset_id, None, false)
                        }))
                        .collect()
                    })
                })
            )
        });
        assert!(result.is_ok());
        let manifests = result.unwrap();
        assert!(manifests.len() == num_test_files);
        for entry in manifests {
            assert!(entry.bucket().is_some());
            assert!(entry.group_id().is_some());
        }
    }
}
