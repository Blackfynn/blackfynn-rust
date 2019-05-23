// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};

use crate::bf::api::{BFId, BFName};
use crate::bf::model;

/// An identifier for a package on the Blackfynn platform.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct PackageId(String);

impl PackageId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        PackageId(id.into())
    }

    /// Unwraps the value.
    pub fn take(self) -> String {
        self.0
    }
}

impl Borrow<String> for PackageId {
    fn borrow(&self) -> &String {
        &self.0
    }
}

impl Borrow<str> for PackageId {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for PackageId {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PackageId> for String {
    fn from(id: PackageId) -> String {
        id.0
    }
}

impl<'a> From<&'a PackageId> for String {
    fn from(id: &'a PackageId) -> String {
        id.0.to_string()
    }
}

impl From<String> for PackageId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl<'a> From<&'a str> for PackageId {
    fn from(id: &'a str) -> Self {
        Self::new(String::from(id))
    }
}

impl fmt::Display for PackageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A "package" representation on the Blackfynn platform.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Package {
    id: PackageId,
    name: String,
    dataset_id: model::DatasetNodeId,
    state: Option<String>,
    package_type: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl BFId for Package {
    type Id = PackageId;
    fn id(&self) -> &Self::Id {
        self.id()
    }
}

impl BFName for Package {
    fn name(&self) -> &String {
        self.name()
    }
}

impl Package {
    pub fn id(&self) -> &PackageId {
        &self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    #[allow(dead_code)]
    pub fn dataset_id(&self) -> &model::DatasetNodeId {
        &self.dataset_id
    }

    #[allow(dead_code)]
    pub fn state(&self) -> Option<&String> {
        self.state.as_ref()
    }

    #[allow(dead_code)]
    pub fn package_type(&self) -> Option<&String> {
        self.package_type.as_ref()
    }

    #[allow(dead_code)]
    pub fn create_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    #[allow(dead_code)]
    pub fn updated_at(&self) -> &DateTime<Utc> {
        &self.updated_at
    }
}
