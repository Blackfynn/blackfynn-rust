// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

/// A type representing a login request
#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Login {
    pub email: String,
    pub password: String
}

impl Login {
    pub fn new(email: String, password: String) -> Self {
        Self {
            email,
            password
        }
    }
}
