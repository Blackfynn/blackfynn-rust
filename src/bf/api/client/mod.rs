//! Functions to interact with the Blackfynn platform.

pub mod progress;
pub mod s3;

pub use self::s3::{MultipartUploadResult, S3Uploader, UploadProgress, UploadProgressIter};

pub use self::progress::{ProgressCallback, ProgressUpdate};

use std::borrow::Borrow;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{iter, time};

use futures::{Future as _Future, Stream as _Stream, *};
use hyper;
use hyper::client::{Client, HttpConnector};
use hyper::header::{HeaderName, HeaderValue};
use hyper_tls::HttpsConnector;
use log::debug;
use serde;
use serde_json;
use tokio;

use super::request::chunked_http::ChunkedFilePayload;
use super::{request, response};
use crate::bf::config::{Config, Environment};
use crate::bf::model::upload::MultipartUploadId;
use crate::bf::model::{
    self, DatasetId, DatasetNodeId, FileUpload, ImportId, OrganizationId, PackageId, SessionToken,
    TemporaryCredential, UploadId,
};
use crate::bf::util::futures::{into_future_trait, into_stream_trait};
use crate::bf::{Error, ErrorKind, Future, Result, Stream};

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
        Self {
            inner: Arc::new(Mutex::new(BlackFynnImpl {
                config,
                http_client,
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
    ) -> Future<Q>
    where
        P: serde::Serialize,
        I: IntoIterator<Item = RequestParam> + Send,
        Q: 'static + Send + serde::de::DeserializeOwned,
        S: Into<String> + Send,
    {
        let serialized_payload = payload
            .map(|p| {
                serde_json::to_string(p)
                    .map(Into::into)
                    .map_err(Into::<Error>::into)
            })
            .unwrap_or_else(|| Ok(hyper::Body::empty()))
            .map_err(Into::into);

        match serialized_payload {
            Ok(body) => self.request_with_body(
                route,
                method,
                params,
                body,
                vec![(
                    hyper::header::CONTENT_TYPE,
                    hyper::header::HeaderValue::from_str("application/json").unwrap(),
                )],
            ),
            Err(err) => into_future_trait(futures::failed(err)),
        }
    }

    fn request_with_body<I, Q, S>(
        &self,
        route: S,
        method: hyper::Method,
        params: I,
        body: hyper::Body,
        additional_headers: Vec<(HeaderName, HeaderValue)>,
    ) -> Future<Q>
    where
        I: IntoIterator<Item = RequestParam>,
        Q: 'static + Send + serde::de::DeserializeOwned,
        S: Into<String>,
    {
        let url = self.get_url();

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
            .map_err(Into::<Error>::into)
            .into_future();

        let f = uri
            .and_then(move |uri| {
                let mut req = hyper::Request::builder()
                    .method(method.clone())
                    .uri(uri.clone())
                    .body(body)
                    .unwrap();

                // If a session token exists, use it to set the
                // "X-SESSION-ID" header to make subsequent requests,
                // and add it to the authorization header:
                if let Some(session_token) = token {
                    req.headers_mut().insert(
                        X_SESSION_ID,
                        HeaderValue::from_str(session_token.borrow()).unwrap(),
                    );
                    req.headers_mut().insert(
                        hyper::header::AUTHORIZATION,
                        HeaderValue::from_str(&format!("Bearer {}", session_token.take())).unwrap(),
                    );
                }

                for (header_name, header_value) in additional_headers {
                    req.headers_mut().insert(header_name, header_value);
                }

                let reporting_url: String = uri.to_string();
                let reporting_method: String = method.to_string();

                // Make the actual request:
                client
                    .request(req)
                    .map(|response| (reporting_url, reporting_method, response))
                    .map_err(Into::into)
            })
            .and_then(move |(reporting_url, reporting_method, response)| {
                // Check the status code. And 5XX code will result in the
                // future terminating with an error containing the message
                // emitted from the API:
                let status_code = response.status();
                response
                    .into_body()
                    .concat2()
                    .and_then(move |body: hyper::Chunk| Ok((status_code, body)))
                    .map_err(Into::<Error>::into)
                    .and_then(
                        move |(status_code, body): (hyper::StatusCode, hyper::Chunk)| {
                            if status_code.is_client_error() || status_code.is_server_error() {
                                return future::err(Error::api_error(
                                    status_code,
                                    String::from_utf8_lossy(&body),
                                ));
                            }
                            future::ok((reporting_url, reporting_method, body))
                        },
                    )
                    .and_then(move |(reporting_url, reporting_method, body)| {
                        debug!(
                            "bf:request<{method}:{url}>:serialize:payload = {payload}",
                            method = reporting_method,
                            url = reporting_url,
                            payload = Self::chunk_to_string(&body)
                        );
                        // Finally, attempt to parse the JSON response into a typeful representation:
                        serde_json::from_slice(&body).map_err(Into::into)
                    })
            });

        into_future_trait(f)
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
    ) -> Future<response::ApiSession> {
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
    pub fn get_user(&self) -> Future<model::User> {
        get!(self, "/user/")
    }

    /// Sets the preferred organization of the current user.
    pub fn set_preferred_organization(
        &self,
        organization_id: Option<OrganizationId>,
    ) -> Future<model::User> {
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
    pub fn get_organizations(&self) -> Future<response::Organizations> {
        get!(self, "/organizations/")
    }

    /// Get a specific organization.
    pub fn get_organization_by_id(&self, id: OrganizationId) -> Future<response::Organization> {
        get!(self, route!("/organizations/{id}", id))
    }

    /// Get a listing of the datasets the current user has access to.
    pub fn get_datasets(&self) -> Future<Vec<response::Dataset>> {
        get!(self, "/datasets/")
    }

    /// Create a new dataset.
    pub fn create_dataset<N: Into<String>, D: Into<String>>(
        &self,
        name: N,
        description: Option<D>,
    ) -> Future<response::Dataset> {
        post!(
            self,
            "/datasets/",
            params!(),
            payload!(request::dataset::Create::new(name, description))
        )
    }

    /// Get a specific dataset by its ID.
    pub fn get_dataset_by_id(&self, id: DatasetNodeId) -> Future<response::Dataset> {
        get!(self, route!("/datasets/{id}", id))
    }

    /// Get a specific dataset by its name.
    pub fn get_dataset_by_name<N: Into<String>>(&self, name: N) -> Future<response::Dataset> {
        let name = name.into();
        let inner = self.clone();
        into_future_trait(self.get_datasets().and_then(move |datasets| {
            datasets
                .into_iter()
                .find(|ds| {
                    let ds: &model::Dataset = ds.borrow();
                    ds.name().to_lowercase() == name.to_lowercase()
                })
                .ok_or_else(|| Error::invalid_dataset_name(name))
                .into_future()
                .and_then(move |ds| {
                    // NOTE: We must re-request the found dataset, as any dataset
                    // returned by way of the `/datasets/` route will not include
                    // child packages:
                    inner.get_dataset_by_id(ds.id().clone())
                })
        }))
    }

    /// Get a dataset by ID or by name.
    pub fn get_dataset<N: Into<String>>(&self, id_or_name: N) -> Future<response::Dataset> {
        let id_or_name = id_or_name.into();
        let id = DatasetNodeId::from(id_or_name.clone());
        let name = id_or_name.clone();

        // Definitely not a dataset ID - only try to get by name
        if !id_or_name.starts_with("N:dataset:") {
            into_future_trait(self.get_dataset_by_name(name))

        // Even if it looks like an ID it could still be a name - try both methods
        } else {
            let inner = self.clone();
            into_future_trait(
                self.get_dataset_by_id(id)
                    .or_else(move |_| inner.get_dataset_by_name(name)),
            )
        }
    }

    /// Get the user collaborators of the data set.
    pub fn get_dataset_user_collaborators(&self, id: DatasetNodeId) -> Future<Vec<model::User>> {
        get!(self, route!("/datasets/{id}/collaborators/users", id))
    }

    /// Get the team collaborators of the data set.
    pub fn get_dataset_team_collaborators(&self, id: DatasetNodeId) -> Future<Vec<model::Team>> {
        get!(self, route!("/datasets/{id}/collaborators/teams", id))
    }

    /// Get the organization role on the data set.
    pub fn get_dataset_organization_role(
        &self,
        id: DatasetNodeId,
    ) -> Future<response::OrganizationRole> {
        get!(
            self,
            route!("/datasets/{id}/collaborators/organizations", id)
        )
    }

    /// Update an existing dataset.
    pub fn update_dataset<N: Into<String>, D: Into<String>>(
        &self,
        id: DatasetNodeId,
        name: N,
        description: Option<D>,
    ) -> Future<response::Dataset> {
        put!(
            self,
            route!("/datasets/{id}", id),
            params!(),
            payload!(request::dataset::Update::new(name, description))
        )
    }

    /// Delete an existing dataset.
    pub fn delete_dataset(&self, id: DatasetNodeId) -> Future<()> {
        let f: Future<response::EmptyMap> = delete!(self, route!("/datasets/{id}", id));
        into_future_trait(f.map(|_| ()))
    }

    /// Create a new package.
    pub fn create_package<N, D, P, F>(
        &self,
        name: N,
        package_type: P,
        dataset: D,
        parent: Option<F>,
    ) -> Future<response::Package>
    where
        D: Into<DatasetNodeId>,
        N: Into<String>,
        P: Into<String>,
        F: Into<String>,
    {
        post!(
            self,
            "/packages/",
            params!(),
            payload!(request::package::Create::new(
                name,
                package_type,
                dataset,
                parent
            ))
        )
    }

    /// Get a specific package.
    pub fn get_package_by_id(&self, id: PackageId) -> Future<response::Package> {
        get!(self, route!("/packages/{id}", id))
    }

    /// Get the source files that are part of a package.
    pub fn get_package_sources(&self, id: PackageId) -> Future<response::Files> {
        get!(self, route!("/packages/{id}/sources", id))
    }

    /// Update an existing package.
    pub fn update_package<N: Into<String>>(
        &self,
        id: PackageId,
        name: N,
    ) -> Future<response::Package> {
        put!(
            self,
            route!("/packages/{id}", id),
            params!(),
            payload!(request::package::Update::new(name))
        )
    }

    /// Move several packages to a destination package.
    /// If destination is None, the package is moved to the top level of the dataset.
    pub fn mv<T: Into<PackageId>, D: Into<PackageId>>(
        &self,
        things: Vec<T>,
        destination: Option<D>,
    ) -> Future<response::MoveResponse> {
        post!(
            self,
            "/data/move",
            params!(),
            payload!(request::mv::Move::new(things, destination))
        )
    }

    /// Get the members that belong to the current users organization.
    pub fn get_members(&self) -> Future<Vec<model::User>> {
        into_future_trait(match self.current_organization() {
            Some(org) => self.get_members_by_organization(org),
            None => into_future_trait(future::err::<_, Error>(ErrorKind::NoOrganizationSet.into())),
        })
    }

    /// Get the members that belong to the specified organization.
    pub fn get_members_by_organization(&self, id: OrganizationId) -> Future<Vec<model::User>> {
        get!(self, route!("/organizations/{id}/members", id))
    }

    /// Get the members that belong to the current users organization.
    pub fn get_teams(&self) -> Future<Vec<response::Team>> {
        into_future_trait(match self.current_organization() {
            Some(org) => self.get_teams_by_organization(org),
            None => into_future_trait(future::err::<_, Error>(ErrorKind::NoOrganizationSet.into())),
        })
    }

    /// Get the teams that belong to the specified organization.
    pub fn get_teams_by_organization(&self, id: OrganizationId) -> Future<Vec<response::Team>> {
        get!(self, route!("/organizations/{id}/teams", id))
    }

    /// Grant temporary upload access to the specific dataset for the current session.
    #[deprecated(
        since = "0.4.0",
        note = "please upload using the upload service instead"
    )]
    pub fn grant_upload(&self, id: DatasetNodeId) -> Future<response::UploadCredential> {
        get!(self, route!("/security/user/credentials/upload/{id}", id))
    }

    /// Grant temporary streaming access for the current user.
    pub fn grant_streaming(&self) -> Future<response::TemporaryCredential> {
        get!(self, "/security/user/credentials/streaming")
    }

    /// Generate a preview of the files to be uploaded.
    #[deprecated(
        since = "0.4.0",
        note = "please upload using the upload service instead"
    )]
    pub fn preview_upload<P, Q>(
        &self,
        path: P,
        files: &[(UploadId, Q)],
        append: bool,
    ) -> Future<response::UploadPreview>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        let s3_files: Result<Vec<model::S3File>> = files
            .iter()
            .map(|(id, file)| FileUpload::new_non_recursive_upload(*id, path.as_ref().join(file)))
            .collect::<Result<Vec<_>>>()
            .and_then(|file_uploads| {
                file_uploads
                    .iter()
                    .map(|file_upload| file_upload.to_s3_file())
                    .collect()
            });

        let bf = self.clone();

        let post = s3_files.into_future().and_then(move |s3_files| {
            post!(
                bf,
                "/files/upload/preview",
                params!("append" => if append { "true" } else { "false" }),
                &request::UploadPreview::new(&s3_files)
            )
        });

        into_future_trait(post)
    }

    /// Get a S3 uploader.
    #[deprecated(
        since = "0.4.0",
        note = "please upload using the upload service instead"
    )]
    pub fn s3_uploader(&self, creds: TemporaryCredential) -> Result<S3Uploader> {
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
    #[deprecated(
        since = "0.4.0",
        note = "please upload using the upload service instead"
    )]
    pub fn complete_upload(
        &self,
        import_id: &ImportId,
        dataset_id: &DatasetNodeId,
        destination_id: Option<&PackageId>,
        append: bool,
        use_upload_service: bool,
    ) -> Future<response::Manifests> {
        let mut params = params!(
            "uploadService" => if use_upload_service { "true" } else { "false" },
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

    /// Generate a preview of the files to be uploaded.
    pub fn preview_upload_using_upload_service<P, Q>(
        &self,
        organization_id: &OrganizationId,
        dataset_id: &DatasetId,
        path: Option<P>,
        files: &[(UploadId, Q)],
        append: bool,
        is_directory_upload: bool,
    ) -> Future<response::UploadPreview>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        let s3_files: Result<Vec<model::S3File>> = files
            .iter()
            .map(|(upload_id, file)| {
                let path = path.as_ref();
                if is_directory_upload {
                    path.ok_or_else(|| {
                        Error::invalid_arguments(
                            "Path cannot be None when is_directory_upload is true",
                        )
                    })
                    .and_then(|path| {
                        FileUpload::new_recursive_upload(*upload_id, path, file.as_ref())
                    })
                } else if let Some(path) = path {
                    FileUpload::new_non_recursive_upload(*upload_id, path.as_ref().join(file))
                } else {
                    FileUpload::new_non_recursive_upload(*upload_id, file)
                }
            })
            .collect::<Result<Vec<_>>>()
            .and_then(|file_uploads| {
                file_uploads
                    .iter()
                    .map(|file_upload| file_upload.to_s3_file())
                    .collect()
            });

        let bf = self.clone();
        let organization_id = organization_id.clone();
        let dataset_id = dataset_id.clone();

        let post = s3_files.into_future().and_then(move |s3_files| {
            post!(
                bf,
                route!(
                    "/upload/preview/organizations/{organization_id}",
                    organization_id
                ),
                params!(
                    "append" => if append { "true" } else { "false" },
                    "dataset_id" => String::from(dataset_id)
                ),
                &request::UploadPreview::new(&s3_files)
            )
        });

        into_future_trait(post)
    }

    #[allow(clippy::too_many_arguments)]
    /// Upload a batch of files using the upload service.
    pub fn upload_file_chunks_to_upload_service<P, C>(
        &self,
        organization_id: &OrganizationId,
        import_id: &ImportId,
        path: P,
        files: Vec<model::S3File>,
        missing_parts: Option<response::FilesMissingParts>,
        progress_callback: C,
        parallelism: usize,
    ) -> Stream<ImportId>
    where
        P: 'static + AsRef<Path>,
        C: 'static + ProgressCallback + Clone,
    {
        let bf = self.clone();
        let organization_id = organization_id.clone();
        let import_id = import_id.clone();

        let fs = stream::futures_unordered(
            files
                .into_iter()
                .zip(iter::repeat(path.as_ref().to_path_buf()))
                .map(|file| future::ok::<(model::S3File, PathBuf), Error>(file.clone())),
        )
        .map(move |(file, path): (model::S3File, PathBuf)| {
            let mut file_path = path.clone();
            let file = file.clone();

            file_path.push(file.file_name());

            let file_missing_parts: Option<response::FileMissingParts> = match missing_parts {
                Some(ref mp) => mp
                    .files
                    .iter()
                    .find(|p| &p.file_name == file.file_name())
                    .cloned(),
                None => None,
            };

            let chunked_file_payload =
                if let Some(chunked_upload_properties) = file.chunked_upload() {
                    debug!(
                        "bf:upload_file_chunks<file = {file_name}> :: \
                         Chunk size received from the upload service: {chunk_size}.",
                        file_name = file.file_name(),
                        chunk_size = chunked_upload_properties.chunk_size
                    );

                    ChunkedFilePayload::new_with_chunk_size(
                        import_id.clone(),
                        file_path,
                        chunked_upload_properties.chunk_size,
                        file_missing_parts.as_ref(),
                        progress_callback.clone(),
                    )
                } else {
                    debug!(
                        "bf:upload_file_chunks<file = {file_name}> :: \
                         No chunk size received from the upload service. \
                         Falling back to default.",
                        file_name = file.file_name()
                    );
                    ChunkedFilePayload::new(
                        import_id.clone(),
                        file_path,
                        file_missing_parts.as_ref(),
                        progress_callback.clone(),
                    )
                };

            let bf = bf.clone();
            let organization_id = organization_id.clone();
            let import_id = import_id.clone();

            chunked_file_payload
                .map(move |file_chunk| {
                    if let Some(MultipartUploadId(multipart_upload_id)) = file.multipart_upload_id()
                    {
                        let import_id = import_id.clone();
                        let import_id_clone = import_id.clone();
                        let organization_id = organization_id.clone();
                        into_future_trait(
                            bf.request_with_body(
                                route!(
                                    "/upload/chunk/organizations/{organization_id}/id/{import_id}",
                                    organization_id,
                                    import_id
                                ),
                                hyper::Method::POST,
                                params!(
                                    "filename" => file.file_name().to_string(),
                                    "multipartId" => multipart_upload_id.to_string(),
                                    "chunkChecksum" => file_chunk.checksum.0,
                                    "chunkNumber" => file_chunk.chunk_number.to_string()
                                ),
                                hyper::Body::from(file_chunk.bytes),
                                vec![],
                            )
                            .and_then(
                                move |response: response::UploadResponse| {
                                    if response.success {
                                        future::ok(import_id_clone)
                                    } else {
                                        future::err(Error::upload_error(
                                            response.error.unwrap_or_else(|| {
                                                "no error message supplied".into()
                                            }),
                                        ))
                                    }
                                },
                            ),
                        )
                    } else {
                        into_future_trait(future::err(Error::upload_error(format!(
                            "no multipartId was provided for file: {}",
                            file.file_name()
                        ))))
                    }
                })
                .map_err(Into::into)
                .buffer_unordered(parallelism)
        })
        .flatten();

        into_stream_trait(fs)
    }

    /// Complete an upload to the upload service
    pub fn complete_upload_using_upload_service(
        &self,
        organization_id: &OrganizationId,
        import_id: &ImportId,
        dataset_id: &DatasetNodeId,
        destination_id: Option<&PackageId>,
        append: bool,
    ) -> Future<response::Manifests> {
        let mut params = params!(
            "datasetId" => dataset_id,
            "append" => if append { "true" } else { "false" }
        );
        if let Some(dest_id) = destination_id {
            params.push(param!("destinationId", dest_id.clone()));
        }

        post!(
            self,
            route!(
                "/upload/complete/organizations/{organization_id}/id/{import_id}",
                organization_id,
                import_id
            ),
            params
        )
    }

    /// Get the upload status using the upload service
    pub fn get_upload_status_using_upload_service(
        &self,
        organization_id: &OrganizationId,
        import_id: &ImportId,
    ) -> Future<Option<response::FilesMissingParts>> {
        get!(
            self,
            route!(
                "/upload/status/organizations/{organization_id}/id/{import_id}",
                organization_id,
                import_id
            )
        )
    }

    pub fn upload_file_chunks_to_upload_service_retries<P, C>(
        &self,
        organization_id: &OrganizationId,
        import_id: &ImportId,
        path: &P,
        files: Vec<model::S3File>,
        progress_callback: C,
        parallelism: usize,
    ) -> Stream<ImportId>
    where
        P: 'static + AsRef<Path> + Send,
        C: 'static + ProgressCallback + Clone,
    {
        #[derive(Clone)]
        struct LoopDependencies<C: ProgressCallback + Clone> {
            organization_id: OrganizationId,
            import_id: ImportId,
            path: PathBuf,
            files: Vec<model::S3File>,
            missing_parts: Option<response::FilesMissingParts>,
            result: Option<Vec<ImportId>>,
            progress_callback: C,
            try_num: usize,
            bf: Blackfynn,
            parallelism: usize,
            failed: bool,
        }
        let ld = LoopDependencies {
            organization_id: organization_id.clone(),
            import_id: import_id.clone(),
            path: path.as_ref().to_path_buf(),
            files,
            missing_parts: None,
            result: None,
            progress_callback,
            try_num: 0,
            bf: self.clone(),
            parallelism,
            failed: false,
        };

        let retry_loop = future::loop_fn(ld, |mut ld| {
            let max_retries = 10;
            let delay_millis_multiplier = 100;

            let mut ld_err = ld.clone();

            ld.bf
                .get_upload_status_using_upload_service(&ld.organization_id, &ld.import_id)
                .map(|parts| {
                    ld.missing_parts = parts;
                    ld.failed = false;
                    ld
                })
                .and_then(|mut ld| {
                    ld.bf
                        .upload_file_chunks_to_upload_service(
                            &ld.organization_id,
                            &ld.import_id,
                            ld.path.clone(),
                            ld.files.clone(),
                            ld.missing_parts.clone(),
                            ld.progress_callback.clone(),
                            ld.parallelism,
                        )
                        .collect()
                        .map(|successful_result| {
                            ld.result = Some(successful_result);
                            future::Loop::Break(ld)
                        })
                })
                .into_future()
                .or_else(move |err| {
                    if max_retries > ld_err.try_num {
                        if ld_err.failed {
                            ld_err.try_num += 1;
                        } else {
                            ld_err.try_num = 1;
                        }
                        let delay = delay_millis_multiplier * ld_err.try_num * ld_err.try_num;

                        ld_err.failed = true;

                        debug!("Upload encountered an error: {error}", error = err);
                        debug!("Waiting {millis} millis to retry...", millis = delay);

                        // delay
                        let deadline = time::Instant::now() + time::Duration::from_millis(delay as u64);
                        let continue_loop = tokio::timer::Delay::new(deadline)
                            .map_err(Into::into)
                            .map(move |_| {
                                debug!(
                                    "Attempting to resume missing parts. Attempt {try_num}/{retries})...",
                                    try_num = ld_err.try_num, retries = max_retries
                                );
                                future::Loop::Continue(ld_err)
                            });
                        into_future_trait(continue_loop)
                    } else {
                        into_future_trait(future::ok::<future::Loop<LoopDependencies<C>, LoopDependencies<C>>, Error>(
                            future::Loop::Break(ld_err),
                        ))
                    }
                })
        })
        .map(|ld| {
            match ld.result {
                Some(import_ids) => future::ok::<Stream<ImportId>, Error>(
                    into_stream_trait(stream::futures_unordered(
                        import_ids
                            .iter()
                            .map(|import_id| future::ok(import_id.clone())),
                    )),
                )
                .into_stream(),
                None => future::err(ErrorKind::RetriesExceeded.into()).into_stream(),
            }
            .flatten()
        })
        .into_stream()
        .flatten();

        into_stream_trait(retry_loop)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::{cell, fs, path, result, sync, thread, time};

    use lazy_static::lazy_static;

    use crate::bf::api::client::s3::MultipartUploadResult;
    // use bf::api::{BFChildren, BFId, BFName};
    use crate::bf::config::Environment;
    use crate::bf::util::futures::into_future_trait;
    use crate::bf::util::rand_suffix;

    const TEST_ENVIRONMENT: Environment = Environment::Development;
    const TEST_API_KEY: &str = env!("BLACKFYNN_API_KEY");
    const TEST_SECRET_KEY: &str = env!("BLACKFYNN_SECRET_KEY");

    // "Blackfynn"
    const FIXTURE_ORGANIZATION: &str = "N:organization:c905919f-56f5-43ae-9c2a-8d5d542c133b";

    // Dedicated agent email (dev)
    #[allow(dead_code)]
    const FIXTURE_EMAIL: &str = "agent-test@blackfynn.com";

    // Dedicated agent user (dev)
    #[allow(dead_code)]
    const FIXTURE_USER: &str = "N:user:6caa1955-c39e-4198-83c6-aa8fe3afbe93";

    // "AGENT-DATASET-DO-NOT-DELETE" (dev)
    const FIXTURE_DATASET: &str = "N:dataset:ef04462a-df3e-4a47-a657-f7ec80003b9e";
    const FIXTURE_DATASET_NAME: &str = "AGENT-DATASET-DO-NOT-DELETE";

    // "AGENT-TEST-PACKAGE" (dev)
    const FIXTURE_PACKAGE: &str = "N:collection:cb924124-afa9-49d8-8fdb-2135883312cf";
    const FIXTURE_PACKAGE_NAME: &str = "AGENT-TEST-PACKAGE";

    lazy_static! {
        static ref CONFIG: Config = Config::new(TEST_ENVIRONMENT);
        static ref TEST_FILES: Vec<String> = test_data_files("/small");
        static ref TEST_DATA_DIR: String = test_data_dir("/small");
        pub static ref BIG_TEST_FILES: Vec<String> = test_data_files("/big");
        pub static ref BIG_TEST_DATA_DIR: String = test_data_dir("/big");
        pub static ref MEDIUM_TEST_FILES: Vec<String> = test_data_files("/medium");
        pub static ref MEDIUM_TEST_DATA_DIR: String = test_data_dir("/medium");
    }

    /// given a 'runner' function, run the given Blackfynn instance
    /// through that function and block until completion
    fn run<F, T>(bf: &Blackfynn, runner: F) -> Result<T>
    where
        F: Fn(Blackfynn) -> Future<T>,
        T: 'static + Send,
    {
        let mut rt = tokio::runtime::Runtime::new()?;
        let result = rt.block_on(runner(bf.clone()));
        rt.shutdown_on_idle();
        result
    }

    struct Inner(sync::Mutex<bool>);

    impl Inner {
        pub fn new() -> Self {
            Inner(sync::Mutex::new(false))
        }
    }

    pub struct ProgressIndicator {
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
                .collect::<result::Result<Vec<_>, _>>()
                .unwrap(),
            Err(e) => {
                eprintln!("{:?} :: {:?}", data_dir, e);
                vec![]
            }
        }
    }

    fn add_upload_ids(file_paths: &Vec<String>) -> Vec<(UploadId, String)> {
        file_paths
            .iter()
            .enumerate()
            .map(|(id, file)| (UploadId::from(id as u64), file.to_string()))
            .collect()
    }

    #[test]
    fn login_successfully_locally() {
        let bf = bf();
        let result = run(&bf, move |bf| bf.login(TEST_API_KEY, TEST_SECRET_KEY));
        assert!(result.is_ok());
        assert!(bf.session_token().is_some());
    }

    #[test]
    fn login_fails_locally() {
        let bf = bf();
        let result = run(&bf, move |bf| {
            bf.login(TEST_API_KEY, "this-is-a-bad-secret")
        });
        assert!(result.is_err());
        assert!(bf.session_token().is_none());
    }

    #[test]
    fn fetching_organizations_after_login_is_successful() {
        let org = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_organizations()),
            )
        });

        if org.is_err() {
            panic!("{}", org.unwrap_err().to_string());
        }
    }

    #[test]
    fn fetching_user_after_login_is_successful() {
        let user = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_user()),
            )
        });

        if user.is_err() {
            panic!("{}", user.unwrap_err().to_string());
        }
    }

    #[test]
    fn updating_org_after_login_is_successful() {
        let user = run(&bf(), move |bf| {
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
            panic!("{}", user.unwrap_err().to_string());
        }
    }

    #[test]
    fn fetching_organizations_fails_if_login_fails() {
        let org = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, "another-bad-secret")
                    .and_then(move |_| bf.get_organizations()),
            )
        });

        assert!(org.is_err());
    }

    #[test]
    fn fetching_organization_by_id_is_successful() {
        let org = run(&bf(), move |bf| {
            into_future_trait(bf.login(TEST_API_KEY, TEST_SECRET_KEY).and_then(move |_| {
                bf.get_organization_by_id(OrganizationId::new(FIXTURE_ORGANIZATION))
            }))
        });

        if org.is_err() {
            panic!("{}", org.unwrap_err().to_string());
        }
    }

    #[test]
    fn fetching_datasets_after_login_is_successful() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_datasets()),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().to_string());
        }
    }

    #[test]
    fn fetching_datasets_fails_if_login_fails() {
        let ds = run(&bf(), move |bf| into_future_trait(bf.get_datasets()));
        assert!(ds.is_err());
    }

    #[test]
    fn fetching_dataset_by_id_successful_if_logged_in_and_exists() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_dataset_by_id(DatasetNodeId::new(FIXTURE_DATASET))),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().to_string());
        }
    }

    #[test]
    fn fetching_dataset_by_name_successful_if_logged_in_and_exists() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_dataset_by_name(FIXTURE_DATASET_NAME)),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().to_string());
        }
    }

    #[test]
    fn fetching_dataset_generic_works_with_name() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_dataset(FIXTURE_DATASET_NAME)),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().to_string());
        }
    }

    #[test]
    fn fetching_dataset_generic_works_with_id() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_dataset(DatasetNodeId::new(FIXTURE_DATASET))),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().to_string());
        }
    }

    #[test]
    fn fetching_child_dataset_by_id_is_successful_can_contains_child_packages_if_found_by_id() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_dataset_by_id(FIXTURE_DATASET.into())),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().to_string());
        }

        assert!(ds
            .unwrap()
            .get_package_by_id(Into::<model::PackageId>::into(FIXTURE_PACKAGE))
            .is_some());
    }

    #[test]
    fn fetching_child_dataset_by_name_is_successful_can_contains_child_packages_if_found_by_id() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_dataset_by_id(FIXTURE_DATASET.into())),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().to_string());
        }

        assert!(ds
            .unwrap()
            .get_package_by_name(FIXTURE_PACKAGE_NAME)
            .is_some());
    }

    #[test]
    fn fetching_child_dataset_by_id_is_successful_can_contains_child_packages_if_found_by_name() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_dataset_by_name(FIXTURE_DATASET_NAME)),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().to_string());
        }

        assert!(ds
            .unwrap()
            .get_package_by_id(Into::<model::PackageId>::into(FIXTURE_PACKAGE))
            .is_some());
    }

    #[test]
    fn fetching_child_dataset_by_name_is_successful_can_contains_child_packages_if_found_by_name() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_dataset_by_name(FIXTURE_DATASET_NAME)),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().to_string());
        }

        assert!(ds
            .unwrap()
            .get_package_by_name(FIXTURE_PACKAGE_NAME)
            .is_some());
    }

    #[test]
    fn fetching_child_dataset_fails_if_it_does_not_exists() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_dataset_by_name(FIXTURE_DATASET_NAME)),
            )
        });

        if ds.is_err() {
            panic!("{}", ds.unwrap_err().to_string());
        }

        assert!(ds.unwrap().get_package_by_name("doesnotexist").is_none());
    }

    #[test]
    fn fetching_dataset_by_name_fails_if_it_does_not_exist() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_dataset_by_name("doesnotexist")),
            )
        });

        assert!(ds.is_err());
    }

    #[test]
    fn fetching_package_by_id_successful_if_logged_in_and_exists() {
        let package = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_package_by_id(PackageId::new(FIXTURE_PACKAGE))),
            )
        });
        if package.is_err() {
            panic!("{}", package.unwrap_err().to_string());
        }
    }

    #[test]
    fn fetching_package_by_id_invalid_if_logged_in_and_exists() {
        let package = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_package_by_id(PackageId::new("invalid_package_id"))),
            )
        });

        if let Err(e) = package {
            match e.kind() {
                // blackfynn api returns 403 in this case..it should really be 404 I think
                ErrorKind::ApiError { status_code, .. } => assert_eq!(status_code.as_u16(), 404),
                _ => assert!(false),
            }
        }
    }

    #[test]
    fn fetching_dataset_by_id_fails_if_logged_in_but_doesnt_exists() {
        let ds = run(&bf(), move |bf| {
            into_future_trait(bf.login(TEST_API_KEY, TEST_SECRET_KEY).and_then(move |_| {
                bf.get_dataset_by_id(DatasetNodeId::new(
                    "N:dataset:not-real-6803-4a67-bf20-83076774a5c7",
                ))
            }))
        });
        assert!(ds.is_err());
    }

    #[test]
    fn fetch_dataset_user_collaborators() {
        let collaborators = run(&bf(), move |bf| {
            into_future_trait(bf.login(TEST_API_KEY, TEST_SECRET_KEY).and_then(move |_| {
                bf.get_dataset_user_collaborators(DatasetNodeId::new(FIXTURE_DATASET))
            }))
        })
        .unwrap();

        assert!(collaborators.iter().all(|c| c.role().is_some()));

        let mut collaborators: Vec<(String, String)> = collaborators
            .iter()
            .map(|u| (u.first_name().clone(), u.role().unwrap().clone()))
            .collect();
        collaborators.sort();

        let expected = vec![
            ("Agent".to_string(), "owner".to_string()),
            ("Matt".to_string(), "manager".to_string()),
            ("Michael".to_string(), "manager".to_string()),
            ("Peter".to_string(), "manager".to_string()),
        ];

        assert_eq!(collaborators, expected);
    }

    #[test]
    fn fetch_dataset_team_collaborators() {
        let collaborators = run(&bf(), move |bf| {
            into_future_trait(bf.login(TEST_API_KEY, TEST_SECRET_KEY).and_then(move |_| {
                bf.get_dataset_team_collaborators(DatasetNodeId::new(FIXTURE_DATASET))
            }))
        })
        .unwrap();
        assert!(collaborators.iter().all(|c| c.role().is_some()));

        let mut collaborators: Vec<(String, String)> = collaborators
            .iter()
            .map(|t| (t.name().clone(), t.role().unwrap().clone()))
            .collect();
        collaborators.sort();

        let expected = vec![("Matt's Team".to_string(), "editor".to_string())];

        assert_eq!(collaborators, expected);
    }

    #[test]
    fn fetch_dataset_organization_role() {
        let organization_role = run(&bf(), move |bf| {
            into_future_trait(bf.login(TEST_API_KEY, TEST_SECRET_KEY).and_then(move |_| {
                bf.get_dataset_organization_role(DatasetNodeId::new(FIXTURE_DATASET))
            }))
        })
        .unwrap();

        let organization_role = (
            organization_role.name().clone(),
            organization_role.role().clone(),
        );
        let expected = ("Blackfynn".to_string(), "manager".to_string());

        assert_eq!(organization_role, expected);
    }

    #[test]
    fn fetch_members() {
        let members = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_user().map(|user| (user, bf)))
                    .and_then(move |(user, bf)| {
                        let org = user.preferred_organization().clone();
                        bf.set_preferred_organization(org.cloned()).map(|_| bf)
                    })
                    .and_then(move |bf| bf.get_members()),
            )
        });
        assert!(members.is_ok());
    }

    #[test]
    fn fetch_teams() {
        let teams = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.get_user().map(|user| (user, bf)))
                    .and_then(move |(user, bf)| {
                        let org = user.preferred_organization().clone();
                        bf.set_preferred_organization(org.cloned()).map(|_| bf)
                    })
                    .and_then(move |bf| bf.get_teams()),
            )
        });
        assert!(teams.is_ok());
    }

    #[test]
    fn creating_then_updating_then_delete_dataset_successful() {
        let new_dataset_name = rand_suffix("$new-test-dataset".to_string());
        let result = run(&bf(), move |bf| {
            let new_dataset_name = new_dataset_name.clone();
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| {
                        let new_dataset_name = new_dataset_name.clone();
                        bf.create_dataset(
                            rand_suffix("$agent-test-dataset".to_string()),
                            Some("A test dataset created by the agent".to_string()),
                        )
                        .map(|ds| (bf, ds, new_dataset_name))
                    })
                    .and_then(move |(bf, ds, new_dataset_name)| {
                        Ok(ds.id().clone()).map(|id| (bf, id, new_dataset_name))
                    })
                    .and_then(move |(bf, id, new_dataset_name)| {
                        bf.update_dataset(
                            id.clone(),
                            new_dataset_name.clone(),
                            None as Option<String>,
                        )
                        .map(|_| (bf, id, new_dataset_name))
                    })
                    .and_then(move |(bf, id, new_dataset_name)| {
                        let id = id.clone();
                        bf.get_dataset_by_id(id.clone())
                            .and_then(move |ds| {
                                assert_eq!(ds.take().name().clone(), new_dataset_name);
                                Ok(id)
                            })
                            .map(|id| (bf, id))
                    })
                    .and_then(move |(bf, id)| bf.delete_dataset(id)),
            )
        });

        if result.is_err() {
            panic!("{}", result.unwrap_err().to_string());
        }
    }

    #[test]
    fn creating_then_updating_then_delete_package_successful() {
        let result = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| {
                        bf.create_dataset(
                            rand_suffix("$agent-test-dataset".to_string()),
                            Some("A test dataset created by the agent".to_string()),
                        )
                        .map(|ds| (bf, ds))
                    })
                    .and_then(move |(bf, ds)| Ok(ds.id().clone()).map(|id| (bf, id)))
                    .and_then(move |(bf, ds_id)| {
                        bf.create_package(
                            rand_suffix("$agent-test-package"),
                            "Text",
                            ds_id.clone(),
                            None as Option<String>,
                        )
                        .map(|pkg| (bf, ds_id, pkg))
                    })
                    .and_then(move |(bf, ds_id, pkg)| {
                        let pkg_id = pkg.take().id().clone();
                        bf.update_package(pkg_id.clone(), "new-package-name")
                            .map(|_| (bf, pkg_id, ds_id))
                    })
                    .and_then(move |(bf, pkg_id, ds_id)| {
                        bf.get_package_by_id(pkg_id).and_then(|pkg| {
                            assert_eq!(pkg.take().name().clone(), "new-package-name".to_string());
                            Ok((bf, ds_id))
                        })
                    })
                    .and_then(move |(bf, ds_id)| bf.delete_dataset(ds_id)),
            )
        });

        if result.is_err() {
            panic!("{}", result.unwrap_err().to_string());
        }
    }

    #[test]
    fn move_package_to_toplevel() {
        let result = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| {
                        bf.create_dataset(
                            rand_suffix("$agent-test-dataset".to_string()),
                            Some("A test dataset created by the agent".to_string()),
                        )
                        .map(|ds| (bf, ds))
                    })
                    .and_then(move |(bf, ds)| Ok(ds.id().clone()).map(|id| (bf, id)))
                    .and_then(move |(bf, ds_id)| {
                        bf.create_package(
                            rand_suffix("$agent-test-collection"),
                            "Collection",
                            ds_id.clone(),
                            None as Option<String>,
                        )
                        .map(|col| (bf, ds_id, col))
                    })
                    .and_then(move |(bf, ds_id, col)| {
                        bf.create_package(
                            rand_suffix("$agent-test-package"),
                            "Text",
                            ds_id.clone(),
                            Some(col.id().clone()),
                        )
                        .map(|pkg| (bf, ds_id, pkg, col))
                    })
                    .and_then(move |(bf, ds_id, pkg, col)| {
                        // Move package to top-level of dataset
                        bf.mv(vec![pkg.take().id().clone()], None as Option<PackageId>)
                            .map(|_| (bf, ds_id, col))
                    })
                    .and_then(move |(bf, ds_id, col)| {
                        bf.get_dataset_by_id(ds_id.clone()).and_then(|dataset| {
                            // Dataset now has two children ($agent-test-collection and $agent-test-package)
                            assert_eq!(dataset.children().unwrap().len(), 2);
                            Ok((bf, ds_id, col))
                        })
                    })
                    .and_then(move |(bf, ds_id, col)| {
                        bf.get_package_by_id(col.id().clone())
                            .and_then(|collection| {
                                // Collection now has no children
                                assert_eq!(collection.children().unwrap().len(), 0);
                                Ok((bf, ds_id))
                            })
                    })
                    .and_then(move |(bf, ds_id)| bf.delete_dataset(ds_id)),
            )
        });

        if result.is_err() {
            panic!("{}", result.unwrap_err().to_string());
        }
    }

    #[test]
    fn move_package_to_collection() {
        let result = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| {
                        bf.create_dataset(
                            rand_suffix("$agent-test-dataset".to_string()),
                            Some("A test dataset created by the agent".to_string()),
                        )
                        .map(|ds| (bf, ds))
                    })
                    .and_then(move |(bf, ds)| Ok(ds.id().clone()).map(|id| (bf, id)))
                    .and_then(move |(bf, ds_id)| {
                        bf.create_package(
                            rand_suffix("$agent-test-collection"),
                            "Collection",
                            ds_id.clone(),
                            None as Option<String>,
                        )
                        .map(|col| (bf, ds_id, col))
                    })
                    .and_then(move |(bf, ds_id, col)| {
                        bf.create_package(
                            rand_suffix("$agent-test-package"),
                            "Text",
                            ds_id.clone(),
                            None as Option<String>,
                        )
                        .map(|pkg| (bf, ds_id, pkg, col))
                    })
                    .and_then(move |(bf, ds_id, pkg, col)| {
                        // Move package into $agent-test-collection
                        bf.mv(vec![pkg.take().id().clone()], Some(col.id().clone()))
                            .map(|_| (bf, ds_id, col))
                    })
                    .and_then(move |(bf, ds_id, col)| {
                        bf.get_dataset_by_id(ds_id.clone()).and_then(|dataset| {
                            // Dataset now has one child
                            assert_eq!(dataset.children().unwrap().len(), 1);
                            Ok((bf, ds_id, col))
                        })
                    })
                    .and_then(move |(bf, ds_id, col)| {
                        bf.get_package_by_id(col.id().clone())
                            .and_then(|collection| {
                                // Collection now has one child
                                assert_eq!(collection.children().unwrap().len(), 1);
                                Ok((bf, ds_id))
                            })
                    })
                    .and_then(move |(bf, ds_id)| bf.delete_dataset(ds_id)),
            )
        });

        if result.is_err() {
            panic!("{}", result.unwrap_err().to_string());
        }
    }

    #[test]
    fn fetching_upload_credential_granting_works() {
        let cred = run(&bf(), move |bf| {
            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| bf.grant_upload(DatasetNodeId::new(FIXTURE_DATASET))),
            )
        });
        if cred.is_err() {
            panic!("{}", cred.unwrap_err().to_string());
        }
    }

    #[test]
    fn preview_upload_file_working() {
        let preview =
            run(&bf(), move |bf| {
                let enumerated_files = add_upload_ids(&*TEST_FILES);
                into_future_trait(bf.login(TEST_API_KEY, TEST_SECRET_KEY).and_then(move |_| {
                    bf.preview_upload(&*TEST_DATA_DIR, &enumerated_files, false)
                }))
            });
        if preview.is_err() {
            panic!("{}", preview.unwrap_err().to_string());
        }
    }

    struct UploadScaffold {
        dataset_id: DatasetNodeId,
        preview: response::UploadPreview,
        upload_credential: response::UploadCredential,
    }

    // Creates a scaffold used to build further tests for uploading:
    fn create_upload_scaffold(
        test_path: String,
        test_files: Vec<String>,
    ) -> Box<dyn Fn(Blackfynn) -> Future<(UploadScaffold, Blackfynn)>> {
        Box::new(move |bf| {
            let test_path = test_path.clone();
            let test_files = add_upload_ids(&test_files);

            into_future_trait(
                bf.login(TEST_API_KEY, TEST_SECRET_KEY)
                    .and_then(move |_| {
                        bf.create_dataset(
                            rand_suffix("$agent-test-dataset".to_string()),
                            Some("A test dataset created by the agent".to_string()),
                        )
                        .map(move |ds| (bf, ds))
                    })
                    .and_then(move |(bf, ds)| {
                        let id = ds.id().clone();
                        bf.grant_upload(id.clone())
                            .map(move |cred| (bf, id.clone(), cred))
                    })
                    .and_then(move |(bf, dataset_id, creds)| {
                        bf.preview_upload(test_path, &test_files, false)
                            .map(|preview| (bf, dataset_id, preview, creds))
                    })
                    .and_then(|(bf, dataset_id, preview, upload_credential)| {
                        future::ok(UploadScaffold {
                            dataset_id,
                            preview,
                            upload_credential,
                        })
                        .join(Ok(bf))
                    }),
            )
        })
    }

    #[test]
    fn simple_file_uploading() {
        let result = run(&bf(), move |bf| {
            let f =
                create_upload_scaffold((*TEST_DATA_DIR).to_string(), (&*TEST_FILES).to_vec())(bf)
                    .and_then(move |(scaffold, bf)| {
                        let bf_clone = bf.clone();
                        let upload_credential = scaffold.upload_credential.clone();
                        let uploader = bf
                            .s3_uploader(scaffold.upload_credential.take().take_temp_credentials())
                            .unwrap();
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
                                    )
                                    .map(move |import_id| (dataset_id, import_id))
                            },
                        ))
                        .map(move |(dataset_id, import_id)| {
                            bf.complete_upload(&import_id, &dataset_id.clone(), None, false, false)
                        })
                        .collect()
                        .map(move |fs| (bf_clone, outer_dataset_id, fs))
                    })
                    .and_then(|(bf, dataset_id, fs)| {
                        stream::futures_unordered(fs)
                            .collect()
                            .map(|manifests| (bf, dataset_id, manifests))
                    })
                    .and_then(|(bf, dataset_id, manifests)| {
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
                    })
                    .and_then(move |(bf, dataset_id)| bf.delete_dataset(dataset_id));

            into_future_trait(f)
        });

        if result.is_err() {
            println!("{}", result.unwrap_err().to_string());
            panic!();
        }
    }

    #[test]
    fn multipart_file_uploading() {
        let result =
            run(&bf(), move |bf| {
                let bf_clone = bf.clone();
                let f = create_upload_scaffold(
                    (*TEST_DATA_DIR).to_string(),
                    (&*TEST_FILES).to_vec(),
                )(bf)
                .and_then(move |(scaffold, bf)| {
                    let dataset_id = scaffold.dataset_id.clone();
                    let dataset_id_inner = scaffold.dataset_id.clone();
                    let cred = scaffold.upload_credential.clone();
                    let uploader = bf
                        .s3_uploader(scaffold.upload_credential.take().take_temp_credentials())
                        .unwrap();
                    stream::iter_ok::<_, Error>(scaffold.preview.into_iter().map(move |package| {
                        uploader.multipart_upload_files(
                            &*TEST_DATA_DIR,
                            package.files(),
                            package.import_id().clone(),
                            cred.clone().into(),
                        )
                    }))
                    .flatten()
                    .filter_map(move |result| match result {
                        MultipartUploadResult::Complete(import_id, _) => Some(bf.complete_upload(
                            &import_id,
                            &dataset_id.clone(),
                            None,
                            false,
                            false,
                        )),
                        _ => None,
                    })
                    .collect()
                    .map(|fs| (fs, dataset_id_inner))
                })
                .and_then(|(fs, dataset_id)| {
                    stream::futures_unordered(fs)
                        .collect()
                        .map(|manifests| (dataset_id, manifests))
                })
                .and_then(|(dataset_id, manifests)| {
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
                })
                .and_then(move |dataset_id| bf_clone.delete_dataset(dataset_id));

                into_future_trait(f)
            });

        if result.is_err() {
            println!("{}", result.unwrap_err().to_string());
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
        let cb = ProgressIndicator::new();

        let result = run(&bf(), move |bf| {
            let cb = cb.clone();

            let f = create_upload_scaffold(
                (*BIG_TEST_DATA_DIR).to_string(),
                (&*BIG_TEST_FILES).to_vec(),
            )(bf)
            .and_then(|(scaffold, bf)| {
                let bf_clone = bf.clone();
                let cred = scaffold.upload_credential.clone();
                let dataset_id = scaffold.dataset_id.clone();
                let dataset_id_outer = dataset_id.clone();
                let mut uploader = bf
                    .s3_uploader(scaffold.upload_credential.take().take_temp_credentials())
                    .unwrap();
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

                stream::iter_ok::<_, Error>(scaffold.preview.into_iter().map(move |package| {
                    let cb = cb.clone();
                    uploader.multipart_upload_files_cb(
                        &*BIG_TEST_DATA_DIR,
                        package.files(),
                        package.import_id().clone(),
                        cred.clone().into(),
                        cb,
                    )
                }))
                .flatten()
                .map(move |result| {
                    match result {
                        MultipartUploadResult::Complete(import_id, _) => {
                            into_future_trait(
                                bf.complete_upload(&import_id, &dataset_id, None, false, false)
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
                })
                .collect()
                .map(|fs| (bf_clone, fs, dataset_id_outer))
            })
            .and_then(|(bf, fs, dataset_id)| {
                stream::futures_unordered(fs)
                    .collect()
                    .map(|manifests| (bf, dataset_id, manifests))
            })
            .and_then(|(bf, dataset_id, manifests)| {
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
            })
            .and_then(move |(bf, dataset_id)| bf.delete_dataset(dataset_id).map(|_| ()));

            into_future_trait(f)
        });

        if result.is_err() {
            println!("{}", result.unwrap_err().to_string());
            panic!();
        }
    }

    #[test]
    fn upload_using_upload_service() {
        // create upload
        let result = run(&bf(), move |bf| {
            let f = bf
                .login(TEST_API_KEY, TEST_SECRET_KEY)
                .and_then(move |_| {
                    bf.create_dataset(
                        rand_suffix("$agent-test-dataset".to_string()),
                        Some("A test dataset created by the agent".to_string()),
                    )
                    .map(move |ds| (bf, ds.id().clone(), ds.int_id().clone()))
                })
                .and_then(|(bf, dataset_id, dataset_int_id)| {
                    bf.get_user().map(|user| {
                        (
                            bf,
                            dataset_id,
                            user.preferred_organization().unwrap().clone(),
                            dataset_int_id,
                        )
                    })
                })
                .and_then(move |(bf, dataset_id, organization_id, dataset_int_id)| {
                    let files: Vec<(UploadId, String)> = add_upload_ids(&*TEST_FILES)
                        .iter()
                        .map(|(id, file)| (*id, format!("{}/{}", *TEST_DATA_DIR, file)))
                        .collect();
                    bf.preview_upload_using_upload_service(
                        &organization_id,
                        &dataset_int_id,
                        None as Option<String>,
                        &files,
                        false,
                        false,
                    )
                    .map(|preview| (bf, dataset_id, organization_id, preview))
                })
                .and_then(move |(bf, dataset_id, organization_id, preview)| {
                    let bf = bf.clone();
                    let bf_clone = bf.clone();
                    let dataset_id = dataset_id.clone();
                    let dataset_id_clone = dataset_id.clone();

                    let upload_futures = preview.into_iter().map(move |package| {
                        let import_id = package.import_id().clone();
                        let bf = bf.clone();
                        let bf_clone = bf.clone();
                        let organization_id = organization_id.clone();

                        let dataset_id = dataset_id.clone();
                        let package = package.clone();

                        let file_path = path::Path::new(&TEST_DATA_DIR.to_string())
                            .to_path_buf()
                            .canonicalize()
                            .unwrap();

                        let progress_indicator = ProgressIndicator::new();

                        bf.upload_file_chunks_to_upload_service(
                            &organization_id,
                            &import_id,
                            file_path,
                            package.files().to_vec(),
                            None,
                            progress_indicator,
                            1,
                        )
                        .collect()
                        .map(|_| (bf_clone, dataset_id))
                        .and_then(move |(bf, dataset_id)| {
                            bf.complete_upload_using_upload_service(
                                &organization_id,
                                &import_id,
                                &dataset_id,
                                None,
                                false,
                            )
                        })
                    });

                    futures::future::join_all(upload_futures).map(|_| (bf_clone, dataset_id_clone))
                })
                .and_then(move |(bf, dataset_id)| bf.delete_dataset(dataset_id));

            into_future_trait(f)
        });

        // check result
        if result.is_err() {
            println!("{}", result.unwrap_err().to_string());
            panic!();
        }
    }

    #[test]
    fn upload_missing_parts_using_upload_service() {
        // create upload
        let result = run(&bf(), move |bf| {
            let f = bf
                .login(TEST_API_KEY, TEST_SECRET_KEY)
                .and_then(move |_| {
                    bf.create_dataset(
                        rand_suffix("$agent-test-dataset".to_string()),
                        Some("A test dataset created by the agent".to_string()),
                    )
                    .map(move |ds| (bf, ds.id().clone(), ds.int_id().clone()))
                })
                .and_then(|(bf, dataset_id, dataset_int_id)| {
                    bf.get_user().map(|user| {
                        (
                            bf,
                            dataset_id,
                            user.preferred_organization().unwrap().clone(),
                            dataset_int_id,
                        )
                    })
                })
                .and_then(move |(bf, dataset_id, organization_id, dataset_int_id)| {
                    let enumerated_files = add_upload_ids(&*MEDIUM_TEST_FILES);
                    bf.preview_upload_using_upload_service(
                        &organization_id,
                        &dataset_int_id,
                        Some((*MEDIUM_TEST_DATA_DIR).to_string()),
                        &enumerated_files,
                        false,
                        false,
                    )
                    .map(|preview| (bf, dataset_id, organization_id, preview))
                })
                .and_then(move |(bf, dataset_id, organization_id, preview)| {
                    let bf = bf.clone();
                    let bf_clone = bf.clone();
                    let dataset_id = dataset_id.clone();
                    let dataset_id_clone = dataset_id.clone();

                    let upload_futures = preview.into_iter().map(move |package| {
                        let import_id = package.import_id().clone();
                        let bf = bf.clone();
                        let bf_clone = bf.clone();
                        let organization_id = organization_id.clone();

                        let dataset_id = dataset_id.clone();
                        let package = package.clone();

                        let file_path = path::Path::new(&MEDIUM_TEST_DATA_DIR.to_string())
                            .to_path_buf()
                            .canonicalize()
                            .unwrap();

                        let progress_indicator = ProgressIndicator::new();

                        // only upload the first chunk
                        bf.upload_file_chunks_to_upload_service(
                            &organization_id,
                            &import_id,
                            file_path.clone(),
                            package.files().to_vec(),
                            Some(response::FilesMissingParts {
                                files: package
                                    .files()
                                    .to_vec()
                                    .iter()
                                    .map(|file| response::FileMissingParts {
                                        file_name: file.file_name().to_string(),
                                        missing_parts: vec![1],
                                        expected_total_parts: 2,
                                    })
                                    .collect(),
                            }),
                            progress_indicator.clone(),
                            1,
                        )
                        .collect()
                        .map(|_| (bf_clone, dataset_id))
                        .and_then(move |(bf, dataset_id)| {
                            bf.get_upload_status_using_upload_service(&organization_id, &import_id)
                                .map(|status| (bf, dataset_id, organization_id, import_id, status))
                        })
                        .and_then(
                            move |(bf, dataset_id, organization_id, import_id, status)| {
                                // upload the rest of the chunks based on the status response
                                bf.upload_file_chunks_to_upload_service(
                                    &organization_id,
                                    &import_id,
                                    file_path,
                                    package.files().to_vec(),
                                    status,
                                    progress_indicator,
                                    1,
                                )
                                .collect()
                                .map(|_| (bf, dataset_id, organization_id, import_id))
                            },
                        )
                        .and_then(
                            move |(bf, dataset_id, organization_id, import_id)| {
                                bf.complete_upload_using_upload_service(
                                    &organization_id,
                                    &import_id,
                                    &dataset_id,
                                    None,
                                    false,
                                )
                            },
                        )
                    });

                    futures::future::join_all(upload_futures).map(|_| (bf_clone, dataset_id_clone))
                })
                .and_then(move |(bf, dataset_id)| bf.delete_dataset(dataset_id));

            into_future_trait(f)
        });

        // check result
        if result.is_err() {
            println!("{}", result.unwrap_err().to_string());
            panic!();
        }
    }

    #[test]
    fn upload_to_upload_service_with_retries() {
        // create upload
        let result = run(&bf(), move |bf| {
            let f = bf
                .login(TEST_API_KEY, TEST_SECRET_KEY)
                .and_then(move |_| {
                    bf.create_dataset(
                        rand_suffix("$agent-test-dataset".to_string()),
                        Some("A test dataset created by the agent".to_string()),
                    )
                    .map(move |ds| (bf, ds.id().clone(), ds.int_id().clone()))
                })
                .and_then(|(bf, dataset_id, dataset_int_id)| {
                    bf.get_user().map(|user| {
                        (
                            bf,
                            dataset_id,
                            user.preferred_organization().unwrap().clone(),
                            dataset_int_id,
                        )
                    })
                })
                .and_then(move |(bf, dataset_id, organization_id, dataset_int_id)| {
                    let enumerated_files = add_upload_ids(&*MEDIUM_TEST_FILES);
                    bf.preview_upload_using_upload_service(
                        &organization_id.clone(),
                        &dataset_int_id,
                        Some((*MEDIUM_TEST_DATA_DIR).to_string()),
                        &enumerated_files,
                        false,
                        false,
                    )
                    .map(|preview| (bf, dataset_id, organization_id, preview))
                })
                .and_then(move |(bf, dataset_id, organization_id, preview)| {
                    let bf = bf.clone();
                    let bf_clone = bf.clone();
                    let dataset_id = dataset_id.clone();
                    let dataset_id_clone = dataset_id.clone();

                    let upload_futures = preview.into_iter().map(move |package| {
                        let import_id = package.import_id().clone();
                        let bf = bf.clone();
                        let bf_clone = bf.clone();
                        let organization_id = organization_id.clone();

                        let dataset_id = dataset_id.clone();
                        let package = package.clone();

                        let file_path = path::Path::new(&MEDIUM_TEST_DATA_DIR.to_string())
                            .to_path_buf()
                            .canonicalize()
                            .unwrap();

                        let progress_indicator = ProgressIndicator::new();

                        // upload using the retries function
                        bf.upload_file_chunks_to_upload_service_retries(
                            &organization_id,
                            &import_id,
                            &file_path,
                            package.files().to_vec(),
                            progress_indicator.clone(),
                            1,
                        )
                        .collect()
                        .map(|_| (bf_clone, dataset_id))
                        .and_then(move |(bf, dataset_id)| {
                            bf.complete_upload_using_upload_service(
                                &organization_id,
                                &import_id,
                                &dataset_id,
                                None,
                                false,
                            )
                        })
                    });

                    futures::future::join_all(upload_futures).map(|_| (bf_clone, dataset_id_clone))
                })
                .and_then(move |(bf, dataset_id)| bf.delete_dataset(dataset_id));

            into_future_trait(f)
        });

        // check result
        if result.is_err() {
            println!("{}", result.unwrap_err().to_string());
            panic!();
        }
    }

    #[test]
    fn upload_directory() {
        // preview upload and verify that it contains previewPath
        let result = run(&bf(), move |bf| {
            let upload_f = bf
                .login(TEST_API_KEY, TEST_SECRET_KEY)
                .and_then(move |_| {
                    bf.create_dataset(
                        rand_suffix("$agent-test-dataset".to_string()),
                        Some("A test dataset created by the agent".to_string()),
                    )
                    .map(move |ds| (bf, ds.id().clone(), ds.int_id().clone()))
                })
                .and_then(|(bf, dataset_id, dataset_int_id)| {
                    bf.get_user().map(|user| {
                        (
                            bf,
                            dataset_id,
                            user.preferred_organization().unwrap().clone(),
                            dataset_int_id,
                        )
                    })
                })
                .and_then(move |(bf, dataset_id, organization_id, dataset_int_id)| {
                    let files_with_path: Vec<String> = MEDIUM_TEST_FILES
                        .iter()
                        .map(|filename| format!("medium/{}", filename))
                        .collect();
                    let enumerated_files = add_upload_ids(&files_with_path);
                    bf.preview_upload_using_upload_service(
                        &organization_id,
                        &dataset_int_id,
                        Some((*MEDIUM_TEST_DATA_DIR).to_string()),
                        &enumerated_files,
                        false,
                        true,
                    )
                    .map(|preview| (bf, dataset_id, organization_id, preview))
                })
                .and_then(move |(bf, dataset_id, organization_id, preview)| {
                    let bf = bf.clone();
                    let bf_clone = bf.clone();
                    let dataset_id = dataset_id.clone();
                    let dataset_id_clone = dataset_id.clone();

                    let upload_futures = preview.into_iter().map(move |package| {
                        let package_copy = package.clone();
                        // perview path should be expected uploaded directory
                        assert_eq!(package.preview_path(), Some("medium".to_string()));

                        let import_id = package_copy.import_id().clone();
                        let bf = bf.clone();
                        let bf_clone = bf.clone();
                        let organization_id = organization_id.clone();

                        let dataset_id = dataset_id.clone();

                        let file_path = path::Path::new(&MEDIUM_TEST_DATA_DIR.to_string())
                            .to_path_buf()
                            .canonicalize()
                            .unwrap();

                        let progress_indicator = ProgressIndicator::new();

                        // upload using the retries function
                        bf.upload_file_chunks_to_upload_service_retries(
                            &organization_id,
                            &import_id,
                            &file_path,
                            package_copy.files().to_vec(),
                            progress_indicator.clone(),
                            1,
                        )
                        .collect()
                        .map(|_| (bf_clone, dataset_id))
                        .and_then(move |(bf, dataset_id)| {
                            bf.complete_upload_using_upload_service(
                                &organization_id,
                                &import_id,
                                &dataset_id,
                                None,
                                false,
                            )
                        })
                    });

                    futures::future::join_all(upload_futures).map(|_| (bf_clone, dataset_id_clone))
                })
                .and_then(move |(bf, dataset_id)| bf.delete_dataset(dataset_id));
            into_future_trait(upload_f)
        });

        // check result
        if result.is_err() {
            println!("{}", result.unwrap_err().to_string());
            panic!();
        }
    }

}
