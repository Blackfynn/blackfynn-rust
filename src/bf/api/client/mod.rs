// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Functions to interact with the Blackfynn platform.

pub mod get;
pub mod post;
pub mod put;
pub mod s3;

// Re-export:
pub use self::get::Get;
pub use self::post::Post;
pub use self::put::Put;
pub use self::s3::{MultipartUploadResult, ProgressCallback, ProgressUpdate, S3Uploader,
                   UploadProgress, UploadProgressIter};

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use futures::*;

use hyper;
use hyper::client::{Client, HttpConnector};
use hyper_tls::HttpsConnector;

use serde;
use serde_json;

use tokio_core::reactor::{Core, Handle};

use super::{request, response};
use bf;
use bf::config::Config;
use bf::model::{self, DatasetId, ImportId, OrganizationId, PackageId, SessionToken,
                TemporaryCredential};
use bf::util::futures::into_future_trait;

/// A custom session ID header for the Blackfynn API
header! { (XSessionId, "X-SESSION-ID") => [String] }

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
    inner: Rc<RefCell<BlackFynnImpl>>,
}

impl Clone for Blackfynn {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

// A useful alias when dealing with the fact that an endpoint does not expect
// a POST/PUT body, but a type is still expected:
type Nothing = serde_json::Value;

// === Request ================================================================

trait Request<T>: Future<Item = T, Error = bf::error::Error> {
    fn new_request(&self) -> bf::Future<T>;
}

// ============================================================================

impl Blackfynn {
    pub fn new(handle: &Handle, config: Config) -> Self {
        let http_client = Client::configure()
            .connector(
                HttpsConnector::new(4, handle)
                    .expect("Blackfynn API :: couldn't create https connector"),
            )
            .build(handle);
        Self {
            inner: Rc::new(RefCell::new(BlackFynnImpl {
                config,
                http_client,
                session_token: None,
                current_organization: None,
            })),
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
        Q: 'static + serde::de::DeserializeOwned,
        S: Into<String>,
    {
        // Build the request url: config environment base + route:
        let mut use_url = self.inner.borrow().config.env().url().clone();
        use_url.set_path(&route.into());

        // If query parameters are provided, add them to the constructed URL:
        for (k, v) in params {
            use_url
                .query_pairs_mut()
                .append_pair(k.as_str(), v.as_str());
        }

        // Lift the URL into a future:
        let url = future::result(use_url.to_string().parse::<hyper::Uri>()).map_err(|e| e.into());

        // If a body payload was provided, lift it into a future:
        let body: bf::Future<Option<String>> = if let Some(data) = payload {
            into_future_trait(
                future::result(serde_json::to_string(data))
                    .map(Some)
                    .map_err(|e| e.into()),
            )
        } else {
            into_future_trait(future::ok(None))
        };

        // Lift the session token into a future:
        let maybe_token = future::ok(self.session_token().clone());

        let bf = future::ok(self.clone());

        let f = bf.join4(url, body, maybe_token)
            .and_then(
                move |(bf, url, body, token): (
                    Blackfynn,
                    hyper::Uri,
                    Option<String>,
                    Option<SessionToken>,
                )| {
                    let uri = url.to_string().parse::<hyper::Uri>()?;
                    let mut req = hyper::Request::new(method.clone(), uri);
                    // If a body was provided, set it in the outgoing request:
                    if let Some(b) = body {
                        req.set_body(b);
                    }
                    Ok((bf, req, token))
                },
            )
            .and_then(
                move |(bf, mut req, token): (Blackfynn, hyper::Request, Option<SessionToken>)| {
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
                    bf.inner
                        .borrow()
                        .http_client
                        .request(req)
                        .map_err(|e| e.into())
                },
            )
            .and_then(|response: hyper::Response| {
                // Check the status code. And 5XX code will result in the
                // future terminating with an error containing the message
                // emitted from the API:
                let status_code = response.status();
                response
                    .body()
                    .concat2()
                    .map_err(|e| e.into())
                    .and_then(move |body: hyper::Chunk| future::ok((status_code, body)))
                    .and_then(
                        move |(status_code, body): (hyper::StatusCode, hyper::Chunk)| {
                            if status_code.is_client_error() || status_code.is_server_error() {
                                return future::err(bf::error::Error::ApiError(
                                    status_code,
                                    String::from_utf8_lossy(&body).to_string(),
                                ));
                            }
                            future::ok(body)
                        },
                    )
                    .and_then(|body: hyper::Chunk| {
                        // Finally, attempt to parse the JSON response into a typeful representation:
                        serde_json::from_slice::<Q>(&body).map_err(Into::into)
                    })
            });

        into_future_trait(f)
    }

    #[allow(dead_code)]
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
    ///      Box::new(bf.organizations())
    ///    });
    ///    assert!(result.is_err());
    ///  }
    ///  ```
    ///
    fn run<F, T>(config: Config, runner: F) -> bf::Result<T>
    where
        F: Fn(Blackfynn) -> bf::Future<T>,
    {
        let mut core = Core::new().expect("couldn't create event loop");
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
    pub fn set_current_organization(&self, id: Option<&OrganizationId>) {
        self.inner.borrow_mut().current_organization = id.cloned()
    }

    /// Sets the session token the user is associated with.
    pub fn set_session_token(&self, token: Option<SessionToken>) {
        self.inner.borrow_mut().session_token = token;
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
        let this = self.clone();
        into_future_trait(
            Post::<request::ApiLogin, response::ApiSession>::new(self, "/account/api/session")
                .body(request::ApiLogin::new(api_key.into(), api_secret.into()))
                .and_then(move |login_response: response::ApiSession| {
                    this.inner.borrow_mut().session_token =
                        Some(login_response.session_token.clone());
                    Ok(login_response)
                }),
        )
    }

    /// Return a Future, that when, resolved returns the user
    /// associated with the session_token.
    pub fn get_user(&self) -> Get<model::User> {
        Get::new(self, "/user/")
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
        into_future_trait(Put::new(self, "/user/").body(user).and_then(
            move |user_response: model::User| {
                this.set_current_organization(user_response.preferred_organization());
                Ok(user_response)
            },
        ))
    }

    /// Return a Future, that when, resolved returns a listing of the
    /// organizations the user is a member of.
    pub fn organizations(&self) -> Get<response::Organizations> {
        Get::new(self, "/organizations/")
    }

    /// Return a Future, that when, resolved returns the specified organization.
    pub fn organization_by_id(&self, id: OrganizationId) -> Get<response::Organization> {
        Get::new(
            self,
            format!("/organizations/{id}", id = Into::<String>::into(id)),
        )
    }

    /// Return a Future, that when, resolved returns a listing of the
    /// datasets the user has access to.
    pub fn datasets(&self) -> Get<Vec<response::Dataset>> {
        Get::new(self, "/datasets/")
    }

    /// Return a Future, that when, resolved returns the specified dataset.
    pub fn dataset_by_id(&self, id: DatasetId) -> Get<response::Dataset> {
        Get::new(
            self,
            format!("/datasets/{id}", id = Into::<String>::into(id)),
        )
    }

    /// Return a Future, that when, resolved returns the specified package.
    pub fn package_by_id(&self, id: PackageId) -> Get<response::Package> {
        Get::new(
            self,
            format!("/packages/{id}", id = Into::<String>::into(id)),
        )
    }

    /// Grant temporary upload access to the specific dataset for the current session.
    pub fn grant_upload(&self, dataset_id: DatasetId) -> Get<response::UploadCredential> {
        Get::new(
            self,
            format!(
                "/security/user/credentials/upload/{dataset}",
                dataset = Into::<String>::into(dataset_id)
            ),
        )
    }

    /// Grant temporary streaming access for the current session.
    pub fn grant_streaming(&self) -> Get<response::TemporaryCredential> {
        Get::new(self, "/security/user/credentials/streaming".to_string())
    }

    /// Generate a preview of the files to be uploaded.
    pub fn preview_upload<P, Q>(
        &self,
        path: P,
        files: &[Q],
        append: bool,
    ) -> bf::Future<response::PreviewPackage>
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

        into_future_trait(
            Post::new(self, "/files/upload/preview")
                .param("append", if append { "true" } else { "false" })
                .body(request::PreviewPackage::new(&s3_files)),
        )
    }

    /// Returns a S3 uploader.
    pub fn s3_uploader(&self, creds: TemporaryCredential) -> S3Uploader {
        let (access_key, secret_key, session_token) = creds.take();
        S3Uploader::new(
            self.inner
                .borrow()
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
    ) -> Post<Nothing, response::Manifest> {
        let mut p = Post::new(
            self,
            format!(
                "/files/upload/complete/{import_id}",
                import_id = Into::<String>::into(import_id)
            ),
        ).param("append", if append { "true" } else { "false" })
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
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::{cell, fs, path, thread, time};

    use tokio_core::reactor::Core;

    use bf::api::client::s3::MultipartUploadResult;
    use bf::config::Environment;
    use bf::util::futures::into_future_trait;

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

    fn create_bf_client() -> (Blackfynn, Core) {
        let core = Core::new().expect("couldn't create tokio core");
        let handle = core.handle();
        let bf = Blackfynn::new(&handle, (*CONFIG).clone());
        (bf, core)
    }

    // Returns the test data directory `<project>/data/<data_dir>`:
    fn test_data_dir(data_dir: &str) -> String {
        concat!(env!("CARGO_MANIFEST_DIR"), "/test/data").to_owned() + data_dir
    }

    // Returns a `Vec<String>` of test data filenames taken from the specified
    // test data directory:
    fn test_data_files(data_dir: &str) -> Vec<String> {
        fs::read_dir(test_data_dir(data_dir))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string())
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn login_successfully_locally() {
        let (bf, mut core) = create_bf_client();
        let login = bf.login(TEST_API_KEY, TEST_SECRET_KEY).then(|r| {
            assert!(r.is_ok());
            future::result(r)
        });
        assert!(core.run(login).is_ok());
        assert!(bf.session_token().is_some());
    }

    #[test]
    fn login_fails_locally() {
        let (bf, mut core) = create_bf_client();
        let login = bf.login(TEST_API_KEY, "this-is-a-bad-secret").then(|r| {
            assert!(r.is_err());
            future::result(r)
        });
        assert!(core.run(login).is_err());
        assert!(bf.session_token().is_none());
    }

    #[test]
    fn fetching_organizations_after_login_is_successful() {
        let org = Blackfynn::run((*CONFIG).clone(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.organizations()),
            )
        });
        assert!(org.is_ok());
    }

    #[test]
    fn fetching_user_after_login_is_successful() {
        let user = Blackfynn::run((*CONFIG).clone(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_user()),
            )
        });
        assert!(user.is_ok());
    }

    #[test]
    fn updating_org_after_login_is_successful() {
        let user = Blackfynn::run((*CONFIG).clone(), move |bf| {
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
        assert!(user.is_ok());
    }

    #[test]
    fn fetching_organizations_fails_if_login_fails() {
        let org = Blackfynn::run((*CONFIG).clone(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, "another-bad-secret")
                    .and_then(move |_| bf.organizations()),
            )
        });
        assert!(org.is_err());
    }

    #[test]
    fn fetching_organization_by_id_is_successful() {
        let org = Blackfynn::run((*CONFIG).clone(), move |bf| {
            into_future_trait(bf.login(TEST_API_KEY, TEST_SECRET_KEY).and_then(move |_| {
                bf.organization_by_id(OrganizationId::new(FIXTURE_ORGANIZATION))
            }))
        });
        assert!(org.is_ok());
    }

    #[test]
    fn fetching_datasets_after_login_is_successful() {
        let ds = Blackfynn::run((*CONFIG).clone(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.datasets()),
            )
        });
        assert!(ds.is_ok());
    }

    #[test]
    fn fetching_datasets_fails_if_login_fails() {
        let ds = Blackfynn::run(
            (*CONFIG).clone(),
            move |bf| into_future_trait(bf.datasets()),
        );
        assert!(ds.is_err());
    }

    #[test]
    fn fetching_dataset_by_id_successful_if_logged_in_and_exists() {
        let ds = Blackfynn::run((*CONFIG).clone(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.dataset_by_id(DatasetId::new(FIXTURE_DATASET))),
            )
        });
        assert!(ds.is_ok());
    }

    #[test]
    fn fetching_package_by_id_successful_if_logged_in_and_exists() {
        let package = Blackfynn::run((*CONFIG).clone(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.package_by_id(PackageId::new(FIXTURE_PACKAGE))),
            )
        });
        assert!(package.is_ok());
    }

    #[test]
    fn fetching_package_by_id_invalid_if_logged_in_and_exists() {
        let config = Config::new(TEST_ENVIRONMENT);
        let package = Blackfynn::run(config, move |bf| {
            Box::new(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.package_by_id(PackageId::new("invalid_package_id"))),
            )
        });

        if let Err(e) = package {
            match e {
                // blackfynn api returns 403 in this case..it should really be 404 I think
                bf::error::Error::ApiError(status, _) => assert_eq!(status.as_u16(), 403),
                _ => assert!(false),
            }
        }
    }

    #[test]
    fn fetching_dataset_by_id_fails_if_logged_in_but_doesnt_exists() {
        let ds = Blackfynn::run((*CONFIG).clone(), move |bf| {
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
        let cred = Blackfynn::run((*CONFIG).clone(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.grant_upload(DatasetId::new(FIXTURE_DATASET))),
            )
        });
        assert!(cred.is_ok());
    }

    #[test]
    fn preview_upload_file_working() {
        let preview = Blackfynn::run((*CONFIG).clone(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.preview_upload(&*TEST_DATA_DIR, &*TEST_FILES, false)),
            )
        });
        assert!(preview.is_ok());
    }

    struct UploadScaffold {
        preview_package: response::PreviewPackage,
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
                            preview_package: p,
                            upload_credential: c,
                        }).join(future::ok(bf))
                    }),
            )
        })
    }

    #[test]
    fn simple_file_uploading() {
        let result = Blackfynn::run((*CONFIG).clone(), move |bf| {
            let dataset_id = (&*TEST_DATASET).clone();
            let f = create_upload_scaffold(
                (&*TEST_DATA_DIR).to_owned(),
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
                stream::futures_unordered(scaffold.preview_package.packages.into_iter().map(
                    move |package| {
                        let upload_credential = upload_credential.clone();
                        // Simple, non-multipart uploading:
                        uploader.put_objects(
                            &*TEST_DATA_DIR,
                            package.files(),
                            package.import_id().clone(),
                            upload_credential.into(),
                        )
                    },
                )).map(move |import_id| {
                    bf.complete_upload(import_id, dataset_id.clone(), None, false)
                })
                    .collect()
            })
                .and_then(|fs| stream::futures_unordered(fs).collect());
            into_future_trait(f)
        });

        if result.is_err() {
            println!("{:#?}", result);
        }
        assert!(result.is_ok());
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
        let result = Blackfynn::run((*CONFIG).clone(), move |bf| {
            let dataset_id = (&*TEST_DATASET).clone();

            let f = create_upload_scaffold(
                (&*TEST_DATA_DIR).to_owned(),
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
                stream::iter_ok::<_, bf::error::Error>(
                    scaffold
                        .preview_package
                        .packages
                        .into_iter()
                        .map(move |package| {
                            uploader.multipart_upload_files(
                                &*TEST_DATA_DIR,
                                package.files(),
                                package.import_id().clone(),
                                cred.clone().into(),
                            )
                        }),
                ).flatten()
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
            println!("{:#?}", result);
        }
        assert!(result.is_ok());
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
        #[derive(Clone)]
        struct ProgressIndicator(cell::Cell<bool>);

        impl ProgressIndicator {
            pub fn new() -> Self {
                ProgressIndicator(cell::Cell::new(false))
            }

            pub fn called(&self) -> bool {
                self.0.get()
            }
        }

        impl ProgressCallback for Rc<ProgressIndicator> {
            fn on_update(&self, _update: &ProgressUpdate) {
                self.0.set(true);
            }
        }

        use std::rc::Rc;

        let cb = Rc::new(ProgressIndicator::new());

        let result = Blackfynn::run((*CONFIG).clone(), |bf| {
            let dataset_id = (&*TEST_DATASET).clone();
            let cb = Rc::clone(&cb);

            let f = create_upload_scaffold(
                (&*BIG_TEST_DATA_DIR).to_owned(),
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
                        let done = RefCell::new(HashSet::<path::PathBuf>::new());
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

                stream::iter_ok::<_, bf::error::Error>(
                    scaffold
                        .preview_package
                        .packages
                        .into_iter()
                        .map(move |package| {
                            let cb = Rc::clone(&cb);
                            uploader.multipart_upload_files_cb(
                                &*BIG_TEST_DATA_DIR,
                                package.files(),
                                package.import_id().clone(),
                                cred.clone().into(),
                                cb,
                            )
                        }),
                ).flatten()
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
                                        Ok(manifest) => {
                                            future::ok(UploadStatus::Completed(manifest))
                                        }
                                        Err(err) => future::ok(UploadStatus::Aborted(err)),
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
            println!("{:#?}", result);
        }
        assert!(result.is_ok());
        let manifests = result.unwrap();
        assert!(manifests.len() > 0);
        for entry in manifests {
            match entry {
                UploadStatus::Completed(_) => assert!(true),
                UploadStatus::Aborted(_) => assert!(false),
            }
        }
    }
}
