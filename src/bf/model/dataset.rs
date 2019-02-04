// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;
use std::ops::Deref;

use bf::api::{BFId, BFName};
use bf::model;
use chrono::{DateTime, Utc};
use std::fmt;

/// An identifier for a Blackfynn dataset.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct DatasetId(String);

impl DatasetId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        DatasetId(id.into())
    }

    /// Unwraps the value.
    pub fn take(self) -> String {
        self.0
    }
}

impl Borrow<String> for DatasetId {
    fn borrow(&self) -> &String {
        &self.0
    }
}

impl Borrow<str> for DatasetId {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for DatasetId {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<DatasetId> for String {
    fn from(id: DatasetId) -> Self {
        id.0
    }
}

impl<'a> From<&'a DatasetId> for String {
    fn from(id: &'a DatasetId) -> Self {
        id.0.to_string()
    }
}

impl From<String> for DatasetId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl<'a> From<&'a str> for DatasetId {
    fn from(id: &'a str) -> Self {
        Self::new(String::from(id))
    }
}

impl fmt::Display for DatasetId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The representation type of a `model::Dataset`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DatasetStatus {
    NoStatus,
    WorkInProgress,
    Completed,
    InReview,
}

/// A Blackfynn dataset.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    id: DatasetId,
    name: String,
    state: Option<model::PackageState>,
    description: Option<String>,
    #[serde(deserialize_with = "model::PackageType::deserialize")]
    package_type: Option<model::PackageType>,
    status: DatasetStatus,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl BFId for Dataset {
    type Id = DatasetId;
    fn id(&self) -> &Self::Id {
        self.id()
    }
}

impl BFName for Dataset {
    fn name(&self) -> &String {
        self.name()
    }
}

impl Dataset {
    pub fn id(&self) -> &DatasetId {
        &self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    #[allow(dead_code)]
    pub fn state(&self) -> Option<&model::PackageState> {
        self.state.as_ref()
    }

    #[allow(dead_code)]
    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }

    #[allow(dead_code)]
    pub fn package_type(&self) -> Option<&model::PackageType> {
        self.package_type.as_ref()
    }

    #[allow(dead_code)]
    pub fn status(&self) -> &DatasetStatus {
        &self.status
    }

    #[allow(dead_code)]
    pub fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    #[allow(dead_code)]
    pub fn updated_at(&self) -> &DateTime<Utc> {
        &self.updated_at
    }
}
