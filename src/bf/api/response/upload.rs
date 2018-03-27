// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// A type representing an API file upload preview response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewPackage {
    pub packages: Vec<model::upload::PackagePreview>
}

// Currently, the response type for `files/upload/complete/{importId}` just
// maps to a manifest directly:
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest(model::Manifest);

impl From<Manifest> for model::Manifest {
    fn from(manifest: Manifest) -> Self {
        manifest.0
    }
}

impl AsRef<model::Manifest> for Manifest {
    fn as_ref(&self) -> &model::Manifest {
        &self.0
    }
}
