// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::fmt;

use serde_derive::Serialize;

/// A Blackfynn platform login request.
#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Property {
    key: String,
    value: String,
}

impl Property {
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }
}

impl fmt::Display for Property {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.key, self.value)
    }
}
