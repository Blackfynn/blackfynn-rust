// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use serde_derive::Deserialize;

use crate::bf::model;

/// A response wrapping a `model::Team`, along with additional metadata.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Team {
    team: model::Team,
    administrators: Vec<model::User>,
    is_admin: bool,
    member_count: i32,
}

impl Team {
    pub fn take(self) -> model::Team {
        self.team
    }

    pub fn administrators(&self) -> &Vec<model::User> {
        self.administrators.as_ref()
    }

    pub fn is_admin(&self) -> bool {
        self.is_admin
    }

    pub fn member_count(&self) -> i32 {
        self.member_count
    }
}
