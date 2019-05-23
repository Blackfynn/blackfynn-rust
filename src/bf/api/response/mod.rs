// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! Client response types to the Blackfynn API.

mod account;
mod channel;
mod dataset;
mod file;
mod mv;
mod organization;
mod package;
mod security;
mod team;
mod upload;

use serde_derive::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmptyMap {}

// Re-export
pub use self::account::ApiSession;
pub use self::channel::Channel;
pub use self::dataset::{ChangeResponse, CollaboratorCounts, Collaborators, Dataset};
pub use self::file::{File, Files};
pub use self::mv::MoveResponse;
pub use self::organization::{Organization, OrganizationRole, Organizations};
pub use self::package::Package;
pub use self::security::{TemporaryCredential, UploadCredential};
pub use self::team::Team;
pub use self::upload::{
    FileHash, FileMissingParts, FilesMissingParts, Manifests, UploadPreview, UploadResponse,
};
