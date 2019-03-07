// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use serde_derive::Deserialize;

use crate::bf::model;

/// The result of a successful login.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
pub struct ApiSession {
    session_token: model::SessionToken,
    organization: String,
    expires_in: i32,
}

impl ApiSession {
    pub fn session_token(&self) -> &model::SessionToken {
        &self.session_token
    }

    pub fn organization(&self) -> &String {
        &self.organization
    }

    pub fn expires_in(&self) -> i32 {
        self.expires_in
    }
}
