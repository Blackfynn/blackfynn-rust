// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use chrono::{DateTime, Utc};
use bf::model;

/// An identifier for a Blackfynn dataset.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DatasetId(String);

impl DatasetId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        DatasetId(id.into())
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<String> for DatasetId {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for DatasetId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<DatasetId> for String {
    fn from(id: DatasetId) -> Self {
        id.0
    }
}

impl From<String> for DatasetId {
    fn from(id: String) -> Self {
        DatasetId::new(id)
    }
}

/// A Blackfynn dataset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    id: DatasetId,
    name: String,
    state: Option<model::PackageState>,
    description: Option<String>,
    #[serde(deserialize_with = "model::PackageType::deserialize")]
    package_type: Option<model::PackageType>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>
}

impl Dataset {
    #[allow(dead_code)]
    pub fn id(&self) -> &DatasetId {
        &self.id
    }

    #[allow(dead_code)]
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
    pub fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    #[allow(dead_code)]
    pub fn updated_at(&self) -> &DateTime<Utc> {
        &self.updated_at
    }
}
