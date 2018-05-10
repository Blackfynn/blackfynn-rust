// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model::{self, PackagePreview};

/// A file upload preview response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewPackage {
    pub packages: Vec<PackagePreview>,
}

/// A manifest of files uploaded to the Blackfynn platform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest(Vec<model::ManifestEntry>);

impl Manifest {
    pub fn entries(&self) -> &Vec<model::ManifestEntry> {
        &self.0
    }
}
