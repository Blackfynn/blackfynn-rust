// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;

use serde_derive::{Deserialize, Serialize};

/// A Blackfynn platform session token.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SessionToken(String);

impl SessionToken {
    pub fn new(token: String) -> Self {
        SessionToken(token)
    }

    /// Unwraps the value.
    pub fn take(self) -> String {
        self.0
    }
}

impl Borrow<String> for SessionToken {
    fn borrow(&self) -> &String {
        &self.0
    }
}

impl Borrow<str> for SessionToken {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for SessionToken {
    fn from(token: String) -> Self {
        SessionToken::new(token)
    }
}

impl From<SessionToken> for String {
    fn from(token: SessionToken) -> Self {
        token.0
    }
}

impl<'a> From<&'a SessionToken> for String {
    fn from(token: &'a SessionToken) -> Self {
        token.0.to_string()
    }
}
