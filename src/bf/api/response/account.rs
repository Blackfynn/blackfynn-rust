// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// A type representing the result of a successful login
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Login {
    pub session_token: Option<model::SessionToken>,
    pub organization: Option<String>,
    pub profile: Option<model::User>,
    pub message: Option<String>
}
