// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// A response wrapping a `model::File`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct File {
    pub content: model::File
}
