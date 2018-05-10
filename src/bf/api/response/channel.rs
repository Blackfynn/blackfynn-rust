// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// A response wrapping a timeseries `model::Channel`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Channel {
    pub content: model::Channel,
}
