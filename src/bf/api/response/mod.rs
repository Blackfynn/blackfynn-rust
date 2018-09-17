// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Client response types to the Blackfynn API.

mod account;
mod channel;
mod dataset;
mod file;
mod organization;
mod package;
mod security;
mod upload;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmptyMap {}

// Re-export
pub use self::account::ApiSession;
pub use self::channel::Channel;
pub use self::dataset::{
    ChangeResponse, CollaboratorChanges, CollaboratorCounts, Collaborators, Dataset,
};
pub use self::file::{File, Files};
pub use self::organization::{Organization, Organizations};
pub use self::package::Package;
pub use self::security::{TemporaryCredential, UploadCredential};
pub use self::upload::{Manifests, UploadPreview};
