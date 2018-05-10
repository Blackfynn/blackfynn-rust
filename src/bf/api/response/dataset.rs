// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::api::response::package::Package;
use bf::model;

/// A response wrapping a `model::Dataset`, along with and related metadata.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub organization: String,
    pub owner: String,
    pub children: Option<Vec<Package>>,
    pub content: model::Dataset,
}
