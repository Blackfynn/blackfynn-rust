// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

/// A Blackfynn platform session token.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionToken(String);

impl SessionToken {
    pub fn new(token: String) -> Self {
        SessionToken(token)
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<String> for SessionToken {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for SessionToken {
    fn as_ref(&self) -> &str {
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
