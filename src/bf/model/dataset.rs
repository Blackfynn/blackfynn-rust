// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};

use crate::bf::api::{BFId, BFName};

/// An node identifier for a Blackfynn dataset (ex. N:dataset:c905919f-56f5-43ae-9c2a-8d5d542c133b).
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct DatasetNodeId(String);

impl DatasetNodeId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        DatasetNodeId(id.into())
    }

    /// Unwraps the value.
    pub fn take(self) -> String {
        self.0
    }
}

impl Borrow<String> for DatasetNodeId {
    fn borrow(&self) -> &String {
        &self.0
    }
}

impl Borrow<str> for DatasetNodeId {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for DatasetNodeId {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<DatasetNodeId> for String {
    fn from(id: DatasetNodeId) -> Self {
        id.0
    }
}

impl<'a> From<&'a DatasetNodeId> for String {
    fn from(id: &'a DatasetNodeId) -> Self {
        id.0.to_string()
    }
}

impl From<String> for DatasetNodeId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl<'a> From<&'a str> for DatasetNodeId {
    fn from(id: &'a str) -> Self {
        Self::new(String::from(id))
    }
}

impl fmt::Display for DatasetNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An integer identifier for a Blackfynn dataset
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct DatasetId(u32);

impl DatasetId {
    #[allow(dead_code)]
    pub fn new(id: u32) -> Self {
        DatasetId(id)
    }

    /// Unwraps the value.
    #[allow(dead_code)]
    pub fn take(self) -> u32 {
        self.0
    }
}

impl Deref for DatasetId {
    type Target = u32;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<DatasetId> for u32 {
    fn from(id: DatasetId) -> Self {
        id.0
    }
}

impl From<u32> for DatasetId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<DatasetId> for String {
    fn from(id: DatasetId) -> Self {
        id.0.to_string()
    }
}

impl fmt::Display for DatasetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A Blackfynn dataset.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    id: DatasetNodeId,
    name: String,
    // -----------------------
    // Existing package states
    // -----------------------
    // * DELETING
    // * ERROR
    // * EXPORTING
    // * EXPORT_FAILED
    // * FAILED
    // * IMPORTING
    // * IMPORT_FAILED
    // * PENDING
    // * READY
    // * RUNNABLE
    // * RUNNING
    // * STARTING
    // * SUBMITTED
    // * SUCCEEDED
    // * UNAVAILABLE
    state: Option<String>,
    description: Option<String>,
    // ----------------------
    // Existing package types
    // ----------------------
    // * CSV
    // * Collection
    // * Image
    // * MRI
    // * MSWord
    // * PDF
    // * Slide
    // * Tabular
    // * Text
    // * TimeSeries
    // * Unknown
    // * Unsupported
    // * Video
    package_type: Option<String>,
    status: String,
    automatically_process_packages: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    int_id: DatasetId,
}

impl BFId for Dataset {
    type Id = DatasetNodeId;
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
    pub fn id(&self) -> &DatasetNodeId {
        &self.id
    }

    pub fn int_id(&self) -> &DatasetId {
        &self.int_id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    #[allow(dead_code)]
    pub fn state(&self) -> Option<&String> {
        self.state.as_ref()
    }

    #[allow(dead_code)]
    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }

    #[allow(dead_code)]
    pub fn package_type(&self) -> Option<&String> {
        self.package_type.as_ref()
    }

    #[allow(dead_code)]
    pub fn status(&self) -> &String {
        &self.status
    }

    #[allow(dead_code)]
    pub fn automatically_process_packages(&self) -> &bool {
        &self.automatically_process_packages
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
