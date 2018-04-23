// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// A response wrapping a timeseries `model::Channel`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Channel {
    content: model::Channel,
}

impl Channel {
    pub fn into_inner(self) -> model::Channel {
        self.content
    }
}

impl AsRef<model::Channel> for Channel {
    fn as_ref(&self) -> &model::Channel {
        &self.content
    }
}
