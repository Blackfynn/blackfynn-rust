// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

/// A type representing a Blackfynn session token.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionToken(String);

impl SessionToken {
    pub fn new(token: String) -> Self {
        SessionToken(token)
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
