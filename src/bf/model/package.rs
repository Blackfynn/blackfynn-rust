// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use serde::{de, Deserialize, Deserializer};

use chrono::{DateTime, Utc};

use bf::model;

/// An identifier for a package on the Blackfynn platform.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

impl From<String> for PackageId {
    fn from(id: String) -> Self {
        PackageId::new(id)
    }
}

/// A package's processing state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize)]
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
    Video,
}

impl PackageType {
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<PackageType>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Option<String> = Option::deserialize(deserializer)?;
        match s {
            Some(t) => match t.as_ref() {
                "Collection" | "collection" | "COLLECTION" => Ok(Some(PackageType::Collection)),
                "DataSet" | "dataset" | "DATASET" => Ok(Some(PackageType::DataSet)),
                "CSV" | "csv" => Ok(Some(PackageType::CSV)),
                "Image" | "image" | "IMAGE" => Ok(Some(PackageType::Image)),
                "MRI" | "mri" => Ok(Some(PackageType::MRI)),
                "MsWord" | "MSWord" | "msword" | "MSWORD" => Ok(Some(PackageType::MSWord)),
                "Pdf" | "pdf" | "PDF" => Ok(Some(PackageType::PDF)),
                "Slide" | "slide" | "SLIDE" => Ok(Some(PackageType::Slide)),
                "Tabular" | "tabular" | "TABULAR" => Ok(Some(PackageType::Tabular)),
                "Text" | "text" | "TEXT" => Ok(Some(PackageType::Text)),
                "TimeSeries" | "timeseries" | "TIMESERIES" => Ok(Some(PackageType::TimeSeries)),
                "Unknown" | "unknown" | "UNKNOWN" => Ok(Some(PackageType::Unknown)),
                "Video" | "video" | "VIDEO" => Ok(Some(PackageType::Video)),
                _ => Err(de::Error::custom(format!("Invalid package type: {}", t))),
            },
            None => Ok(None),
        }
    }
}

/// A "package" representation on the Blackfynn platform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
