// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use serde_derive::Serialize;

/// A Blackfynn platform login request.
#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiLogin {
    token_id: String,
    secret: String,
}

impl ApiLogin {
    pub fn new<P, Q>(token_id: P, secret: Q) -> Self
    where
        P: Into<String>,
        Q: Into<String>,
    {
        Self {
            token_id: token_id.into(),
            secret: secret.into(),
        }
    }
}
