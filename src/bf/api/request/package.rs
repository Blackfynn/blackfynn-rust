// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use serde_derive::Serialize;

use crate::bf::model::{DatasetNodeId, Property};

#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Create {
    name: String,
    package_type: String,
    properties: Vec<Property>,
    dataset: DatasetNodeId,
    parent: Option<String>,
}

impl Create {
    pub fn new<D, N, P, F>(name: N, package_type: P, dataset: D, parent: Option<F>) -> Self
    where
        D: Into<DatasetNodeId>,
        N: Into<String>,
        P: Into<String>,
        F: Into<String>,
    {
        Self {
            name: name.into(),
            package_type: package_type.into(),
            properties: vec![],
            dataset: dataset.into(),
            parent: parent.map(Into::into),
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
