// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

/// An identifier for a team on the Blackfynn platform.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct TeamId(String);

impl TeamId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        TeamId(id.into())
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<String> for TeamId {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for TeamId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<TeamId> for String {
    #[allow(dead_code)]
    fn from(id: TeamId) -> Self {
        id.0
    }
}

impl<'a> From<&'a TeamId> for String {
    #[allow(dead_code)]
    fn from(id: &'a TeamId) -> Self {
        id.0.to_string()
    }
}

impl From<String> for TeamId {
    fn from(id: String) -> Self {
        TeamId::new(id)
    }
}

/// A Team.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Team {
    id: TeamId,
    name: String,
}

impl Team {
    pub fn id(&self) -> &TeamId {
        &self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}
