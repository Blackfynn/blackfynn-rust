/// Common API type definitions.

use std::convert::From;

/// A type representing a Blackfynn session token
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionToken(pub String);

impl From<SessionToken> for String {
    fn from(token: SessionToken) -> String {
        token.0
    }
}

/// A type representing a Blackfynn API token
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiToken(pub String);


impl From<ApiToken> for String {
    fn from(token: ApiToken) -> String {
        token.0
    }
}

/// A type representing a Blackfynn secret token
#[derive(Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretToken(pub String);

impl From<SecretToken> for String {
    fn from(token: SecretToken) -> String {
        token.0
    }
}

