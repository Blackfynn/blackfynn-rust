// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

/// A user HTTP `PUT` request.
#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub organization: Option<String>,
    pub email: Option<String>,
    pub url: Option<String>,
    pub color: Option<String>,
    pub last_name: Option<String>,
    pub first_name: Option<String>,
    pub credential: Option<String>,
}

impl Default for User {
    fn default() -> Self {
        Self {
            organization: None,
            email: None,
            url: None,
            color: None,
            last_name: None,
            first_name: None,
            credential: None,
        }
    }
}
