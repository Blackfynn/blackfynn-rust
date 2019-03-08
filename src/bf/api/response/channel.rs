// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;

use serde_derive::Deserialize;

use crate::bf::model;

/// A response wrapping a timeseries `model::Channel`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Channel {
    content: model::Channel,
}

impl Channel {
    pub fn take(self) -> model::Channel {
        self.content
    }
}

impl Borrow<model::Channel> for Channel {
    fn borrow(&self) -> &model::Channel {
        &self.content
    }
}
