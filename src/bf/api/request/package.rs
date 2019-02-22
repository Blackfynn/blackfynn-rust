// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model::{DatasetNodeId, Property};

pub use bf::model::PackageType;

#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Create {
    name: String,
    package_type: PackageType,
    properties: Vec<Property>,
    dataset: DatasetNodeId,
}

impl Create {
    pub fn new<P, Q>(name: P, package_type: PackageType, dataset: Q) -> Self
    where
        P: Into<String>,
        Q: Into<DatasetNodeId>,
    {
        Self {
            name: name.into(),
            package_type,
            properties: vec![],
            dataset: dataset.into(),
        }
    }
}

#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Update {
    name: String,
}

impl Update {
    pub fn new<P>(name: P) -> Self
    where
        P: Into<String>,
    {
        Self { name: name.into() }
    }
}
