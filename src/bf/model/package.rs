// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;
use chrono::{DateTime, Utc};
use serde::{de, Deserialize, Deserializer};
use std::fmt;

/// An identifier for a package on the Blackfynn platform.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct PackageId(String);

impl PackageId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        PackageId(id.into())
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<String> for PackageId {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for PackageId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A package's processing state.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PackageState {
    Deleting,
    Error,
    Failed,
    Pending,
    Ready,
    Runnable,
    Running,
    Starting,
    Submitted,
    Succeeded,
    Unavailable,
}

/// A package's type.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PackageType {
    Collection,
    DataSet,
    CSV,
    Image,
    MRI,
    MSWord,
    PDF,
    Slide,
    Tabular,
    Text,
    TimeSeries,
    Unknown,
    Unsupported,
    Video,
}

impl Default for PackageType {
    fn default() -> Self {
        PackageType::Unknown
    }
}

impl PackageType {
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<PackageType>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Option<String> = Option::deserialize(deserializer)?;
        match s {
            Some(t) => match t.to_lowercase().as_ref() {
                "collection" => Ok(Some(PackageType::Collection)),
                // TODO: Remove API support for dataset package type
                "dataset" => Ok(Some(PackageType::DataSet)),
                "csv" => Ok(Some(PackageType::CSV)),
                "image" => Ok(Some(PackageType::Image)),
                "mri" => Ok(Some(PackageType::MRI)),
                "msword" => Ok(Some(PackageType::MSWord)),
                "pdf" => Ok(Some(PackageType::PDF)),
                "slide" => Ok(Some(PackageType::Slide)),
                "tabular" => Ok(Some(PackageType::Tabular)),
                "text" => Ok(Some(PackageType::Text)),
                "timeseries" => Ok(Some(PackageType::TimeSeries)),
                "unknown" => Ok(Some(PackageType::Unknown)),
                "unsupported" => Ok(Some(PackageType::Unsupported)),
                "video" => Ok(Some(PackageType::Video)),
                _ => Err(de::Error::custom(format!("Invalid package type: {}", t))),
            },
            None => Ok(None),
        }
    }
}

/// A "package" representation on the Blackfynn platform.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Package {
    id: PackageId,
    name: String,
    dataset_id: model::DatasetId,
    package_state: Option<PackageState>,
    #[serde(deserialize_with = "PackageType::deserialize")]
    package_type: Option<PackageType>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Package {
    #[allow(dead_code)]
    pub fn id(&self) -> &model::PackageId {
        &self.id
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &String {
        &self.name
    }

    #[allow(dead_code)]
    pub fn dataset_id(&self) -> &model::DatasetId {
        &self.dataset_id
    }

    #[allow(dead_code)]
    pub fn package_state(&self) -> Option<&PackageState> {
        self.package_state.as_ref()
    }

    #[allow(dead_code)]
    pub fn package_type(&self) -> Option<&PackageType> {
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
