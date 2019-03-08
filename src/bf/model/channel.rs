// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use serde_derive::{Deserialize, Serialize};

use crate::bf::api::BFName;

/// A Blackfynn timeseries channel.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Channel {
    name: String,
    rate: f64,
    start: i64,
    end: i64,
    unit: String,
    spike_duration: Option<i64>,
    channel_type: String,
    group: Option<String>,
}

impl BFName for Channel {
    fn name(&self) -> &String {
        &self.name
    }
}

impl Channel {
    pub fn rate(&self) -> f64 {
        self.rate
    }

    pub fn start(&self) -> i64 {
        self.start
    }

    pub fn end(&self) -> i64 {
        self.end
    }

    pub fn spike_duration(&self) -> Option<i64> {
        self.spike_duration
    }

    pub fn channel_type(&self) -> &String {
        &self.channel_type
    }

    pub fn group(&self) -> Option<&String> {
        self.group.as_ref()
    }
}
