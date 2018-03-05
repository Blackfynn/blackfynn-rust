use bf::api::types::{SessionToken};
use bf::model::{User};

/// A type representing the result of a successful login
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Login {
    pub session_token: Option<SessionToken>,
    pub organization: Option<String>,
    pub profile: Option<User>,
    pub message: Option<String>
}
