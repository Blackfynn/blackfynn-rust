// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

/// A Blackfynn platform login request.
#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiLogin {
    pub token_id: String,
    pub secret: String
}

impl ApiLogin {
    pub fn new(token_id: String, secret: String) -> Self {
        Self {
            token_id,
            secret
        }
    }
}
