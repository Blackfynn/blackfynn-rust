// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;

use serde_derive::{Deserialize, Serialize};

use crate::bf::api::BFId;
use crate::bf::model;

/// An identifier for a user on the Blackfynn platform.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct UserId(String);

impl UserId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        UserId(id.into())
    }

    /// Unwraps the value.
    pub fn take(self) -> String {
        self.0
    }
}

impl Borrow<String> for UserId {
    fn borrow(&self) -> &String {
        &self.0
    }
}

impl Borrow<str> for UserId {
    fn borrow(&self) -> &str {
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
    role: Option<String>,
}

impl BFId for User {
    type Id = UserId;
    fn id(&self) -> &Self::Id {
        self.id()
    }
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

    pub fn role(&self) -> Option<&String> {
        self.role.as_ref()
    }
}
