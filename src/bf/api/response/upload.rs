// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::slice;
use std::vec;

use bf::model;

/// A file upload preview response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadPreview {
    packages: Vec<model::PackagePreview>,
}

impl UploadPreview {
    /// Unwraps the value.
    pub fn into_inner(self) -> Vec<model::PackagePreview> {
        self.packages
    }

    pub fn packages(&self) -> &Vec<model::PackagePreview> {
        &self.packages
    }

    pub fn package_count(&self) -> usize {
        self.packages.len()
    }

    pub fn file_count(&self) -> usize {
        self.packages.iter().map(|p| p.file_count()).sum()
    }

    pub fn iter(&self) -> slice::Iter<model::PackagePreview> {
        self.packages.iter()
    }
}

impl IntoIterator for UploadPreview {
    type Item = model::PackagePreview;
    type IntoIter = vec::IntoIter<model::PackagePreview>;

    fn into_iter(self) -> Self::IntoIter {
        self.packages.into_iter()
    }
}

/// A manifest of files uploaded to the Blackfynn platform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifests(Vec<model::ManifestEntry>);

impl Manifests {
    /// Unwraps the value.
    pub fn into_inner(self) -> Vec<model::ManifestEntry> {
        self.0
    }

    pub fn entries(&self) -> &Vec<model::ManifestEntry> {
        &self.0
    }

    pub fn iter(&self) -> slice::Iter<model::ManifestEntry> {
        self.0.iter()
    }
}

impl IntoIterator for Manifests {
    type Item = model::ManifestEntry;
    type IntoIter = vec::IntoIter<model::ManifestEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// A file upload preview response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadResponse {
    pub success: bool,
    pub error: Option<String>
}
