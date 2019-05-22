// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::slice;
use std::vec;

use serde_derive::Deserialize;

use crate::bf::model;

/// A response wrapping a `model::Organization`, along with related metadata.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Organization {
    is_admin: bool,
    is_owner: bool,
    owners: Vec<model::User>,
    administrators: Vec<model::User>,
    organization: model::Organization,
}

impl Organization {
    pub fn is_admin(&self) -> bool {
        self.is_admin
    }

    pub fn is_owner(&self) -> bool {
        self.is_owner
    }

    pub fn owners(&self) -> &Vec<model::User> {
        &self.owners
    }

    pub fn administrators(&self) -> &Vec<model::User> {
        &self.administrators
    }

    pub fn organization(&self) -> &model::Organization {
        &self.organization
    }
}

impl From<model::Organization> for Organization {
    #[allow(dead_code)]
    fn from(organization: model::Organization) -> Self {
        Self {
            is_admin: false,
            is_owner: false,
            owners: vec![],
            administrators: vec![],
            organization,
        }
    }
}

/// A listing of organizations a user is a member of.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Organizations {
    organizations: Vec<Organization>,
}

impl Organizations {
    pub fn len(&self) -> usize {
        self.organizations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> slice::Iter<'_, Organization> {
        self.organizations.iter()
    }
}

impl From<Vec<model::Organization>> for Organizations {
    #[allow(dead_code)]
    fn from(organizations: Vec<model::Organization>) -> Self {
        Self {
            organizations: organizations
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>(),
        }
    }
}

impl IntoIterator for Organizations {
    type Item = Organization;
    type IntoIter = vec::IntoIter<Organization>;

    fn into_iter(self) -> Self::IntoIter {
        self.organizations.into_iter()
    }
}

/// An organization role.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationRole {
    id: String,
    name: String,
    role: Option<String>,
}

impl OrganizationRole {
    pub fn id(&self) -> &String {
        &self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn role(&self) -> Option<&String> {
        self.role.as_ref()
    }
}
