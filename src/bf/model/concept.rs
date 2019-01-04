// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

use chrono::{DateTime, Utc};

use bf::api::{BFId, BFName};

/// An identifier for a Blackfynn model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ModelId(String);

impl ModelId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        ModelId(id.into())
    }

    /// Unwraps the value.
    pub fn take(self) -> String {
        self.0
    }
}

impl Borrow<String> for ModelId {
    fn borrow(&self) -> &String {
        &self.0
    }
}

impl Borrow<str> for ModelId {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for ModelId {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<ModelId> for String {
    fn from(id: ModelId) -> Self {
        id.0
    }
}

impl<'a> From<&'a ModelId> for String {
    fn from(id: &'a ModelId) -> Self {
        id.0.to_string()
    }
}

impl From<String> for ModelId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl<'a> From<&'a str> for ModelId {
    fn from(id: &'a str) -> Self {
        Self::new(String::from(id))
    }
}

impl fmt::Display for ModelId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A Blackfynn model (formerly `concept`).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Model {
    id: ModelId,
    name: String,
    display_name: String,
    description: String,
    locked: bool,
    count: i64,
    property_count: i64,
    template_id: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl BFId for Model {
    type Id = ModelId;
    fn id(&self) -> &Self::Id {
        self.id()
    }
}

impl BFName for Model {
    fn name(&self) -> &String {
        self.name()
    }
}

impl Model {
    pub fn id(&self) -> &ModelId {
        &self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    #[allow(dead_code)]
    pub fn display_name(&self) -> &String {
        &self.display_name
    }

    #[allow(dead_code)]
    pub fn locked(&self) -> bool {
        self.locked
    }

    #[allow(dead_code)]
    pub fn count(&self) -> i64 {
        self.count
    }

    #[allow(dead_code)]
    pub fn property_count(&self) -> i64 {
        self.count
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

/// Data attached to a record (formerly `InstanceDatum`)
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordDatum {
    name: String,
    display_name: String,
    value: Option<String>,
    required: bool,
    locked: bool,
    default: bool,
    #[serde(rename = "name")]
    is_title: bool,
}

impl BFName for RecordDatum {
    fn name(&self) -> &String {
        self.name()
    }
}

impl RecordDatum {
    pub fn name(&self) -> &String {
        &self.name
    }

    #[allow(dead_code)]
    pub fn display_name(&self) -> &String {
        &self.display_name
    }

    #[allow(dead_code)]
    pub fn value(&self) -> Option<&String> {
        self.value.as_ref()
    }

    #[allow(dead_code)]
    pub fn locked(&self) -> bool {
        self.locked
    }

    #[allow(dead_code)]
    pub fn required(&self) -> bool {
        self.required
    }

    #[allow(dead_code)]
    pub fn default(&self) -> bool {
        self.default
    }

    #[allow(dead_code)]
    pub fn is_title(&self) -> bool {
        self.is_title
    }
}

/// An identifier for a Blackfynn record.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct RecordId(String);

impl RecordId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        RecordId(id.into())
    }

    /// Unwraps the value.
    pub fn take(self) -> String {
        self.0
    }
}

impl Borrow<String> for RecordId {
    fn borrow(&self) -> &String {
        &self.0
    }
}

impl Borrow<str> for RecordId {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for RecordId {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<RecordId> for String {
    fn from(id: RecordId) -> Self {
        id.0
    }
}

impl<'a> From<&'a RecordId> for String {
    fn from(id: &'a RecordId) -> Self {
        id.0.to_string()
    }
}

impl From<String> for RecordId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl<'a> From<&'a str> for RecordId {
    fn from(id: &'a str) -> Self {
        Self::new(String::from(id))
    }
}

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An data instance of a Blackfynn model (formerly `concept instance`).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    id: RecordId,
    #[serde(rename = "type")]
    type_: String,
    values: Vec<RecordDatum>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl BFId for Record {
    type Id = RecordId;
    fn id(&self) -> &Self::Id {
        self.id()
    }
}

impl IntoIterator for Record {
    type Item = RecordDatum;
    type IntoIter = ::std::vec::IntoIter<RecordDatum>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl Record {
    pub fn id(&self) -> &RecordId {
        &self.id
    }

    #[allow(dead_code)]
    pub fn type_(&self) -> &String {
        &self.type_
    }

    #[allow(dead_code)]
    pub fn values(&self) -> &Vec<RecordDatum> {
        &self.values
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[allow(dead_code)]
    pub fn take_values(self) -> Vec<RecordDatum> {
        self.values
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
