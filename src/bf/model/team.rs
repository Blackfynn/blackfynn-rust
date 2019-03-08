// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;
use std::ops::Deref;

use serde_derive::{Deserialize, Serialize};

use crate::bf::api::{BFId, BFName};

/// An identifier for a team on the Blackfynn platform.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct TeamId(String);

impl TeamId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        TeamId(id.into())
    }

    /// Unwraps the value.
    pub fn take(self) -> String {
        self.0
    }
}

impl Borrow<String> for TeamId {
    fn borrow(&self) -> &String {
        &self.0
    }
}

impl Borrow<str> for TeamId {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for TeamId {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
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
    role: Option<String>,
}

impl Team {
    pub fn id(&self) -> &TeamId {
        &self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn role(&self) -> Option<&String> {
        self.role.as_ref()
    }
}

impl BFId for Team {
    type Id = TeamId;
    fn id(&self) -> &Self::Id {
        self.id()
    }
}

impl BFName for Team {
    fn name(&self) -> &String {
        self.name()
    }
}
