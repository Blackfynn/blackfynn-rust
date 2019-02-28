// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model::{DatasetNodeId, Property};

#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Create {
    name: String,
    package_type: String,
    properties: Vec<Property>,
    dataset: DatasetNodeId,
}

impl Create {
    pub fn new<D, N, P>(name: N, package_type: P, dataset: D) -> Self
    where
        D: Into<DatasetNodeId>,
        N: Into<String>,
        P: Into<String>,
    {
        Self {
            name: name.into(),
            package_type: package_type.into(),
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
