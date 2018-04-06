// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// The result of a successful login.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
pub struct ApiSession {
    pub session_token: model::SessionToken,
    pub organization: String,
    pub expires_in: i32,
}
