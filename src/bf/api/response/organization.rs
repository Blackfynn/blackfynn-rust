// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::slice;
use std::vec;

use bf::model;

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

/// A listing of organizations a user is a member of.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Organizations {
    organizations: Vec<Organization>,
}

impl Organizations {
    pub fn iter(&self) -> slice::Iter<Organization> {
        self.organizations.iter()
    }
}

impl IntoIterator for Organizations {
    type Item = Organization;
    type IntoIter = vec::IntoIter<Organization>;

    fn into_iter(self) -> Self::IntoIter {
        self.organizations.into_iter()
    }
}
