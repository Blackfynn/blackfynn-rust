// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;
use bf::api::response;

// This corresponds to the `objects` map that is returned from `/packages/{:id}`
// when the `include=` parameter is provided.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMap {
    pub source: Option<Vec<response::file::File>>,
    pub file: Option<Vec<response::file::File>>,
    pub view: Option<Vec<response::file::File>>
}

/// A type representing an API response containing a package.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Package {
    pub channels: Option<Vec<response::channel::Channel>>,
    pub content: model::Package,
    pub children: Option<Vec<Package>>,
    pub objects: Option<ObjectMap>,
}
