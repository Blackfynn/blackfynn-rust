// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// An identifier for a user on the Blackfynn platform.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct UserId(String);

impl UserId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        UserId(id.into())
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<String> for UserId {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for UserId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<UserId> for String {
    #[allow(dead_code)]
    fn from(id: UserId) -> Self {
        id.0
    }
}

impl<'a> From<&'a UserId> for String {
    #[allow(dead_code)]
    fn from(id: &'a UserId) -> Self {
        id.0.to_string()
    }
}

impl From<String> for UserId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl<'a> From<&'a str> for UserId {
    fn from(id: &'a str) -> Self {
        Self::new(String::from(id))
    }
}

/// A user.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    id: UserId,
    first_name: String,
    last_name: String,
    email: String,
    preferred_organization: Option<model::OrganizationId>,
}

impl User {
    pub fn id(&self) -> &UserId {
        &self.id
    }

    pub fn first_name(&self) -> &String {
        &self.first_name
    }

    pub fn last_name(&self) -> &String {
        &self.last_name
    }

    pub fn email(&self) -> &String {
        &self.email
    }

    pub fn preferred_organization(&self) -> Option<&model::OrganizationId> {
        self.preferred_organization.as_ref()
    }
}
