// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;
use std::ops::Deref;

use serde_derive::Deserialize;

use crate::bf::api::response::package::Package;
use crate::bf::api::BFChildren;
use crate::bf::model;

/// A response wrapping a `model::Dataset`, along with and related metadata.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    organization: String,
    owner: String,
    children: Option<Vec<Package>>,
    content: model::Dataset,
}

impl BFChildren for Dataset {
    type Child = Package;
    fn children(&self) -> Option<&Vec<Self::Child>> {
        self.children.as_ref()
    }
}

impl Borrow<model::Dataset> for Dataset {
    fn borrow(&self) -> &model::Dataset {
        &self.content
    }
}

impl Deref for Dataset {
    type Target = model::Dataset;
    fn deref(&self) -> &Self::Target {
        &self.content
    }
}

impl Dataset {
    /// Get the organization associated with this dataset.
    pub fn organization(&self) -> &String {
        &self.organization
    }

    /// Get the owner of the dataset.
    pub fn owner(&self) -> &String {
        &self.owner
    }

    // Get the child packages contained in this dataset.
    pub fn children(&self) -> Option<&Vec<Package>> {
        self.children.as_ref()
    }

    /// Take ownership of the dataset wrapped by this response object.
    pub fn take(self) -> model::Dataset {
        self.content
    }

    /// Fetch a package from a dataset by package ID.
    pub fn get_package_by_id(&self, package_id: model::PackageId) -> Option<model::Package> {
        self.get_child_by_id(package_id).map(|p| p.clone().take())
    }

    /// Fetch a package from a dataset by package name.
    pub fn get_package_by_name<N: Into<String>>(&self, package_name: N) -> Option<model::Package> {
        self.get_child_by_name(package_name)
            .map(|p| p.clone().take())
    }
}

/// A response wrapping a `model::Collaborators`, along with and related metadata.
#[derive(Debug, Clone, Eq, Hash, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Collaborators {
    users: Vec<model::User>,
    organizations: Vec<model::Organization>,
    teams: Vec<model::Team>,
}

impl Collaborators {
    /// Get the user collaborators.
    #[allow(dead_code)]
    pub fn users(&self) -> &Vec<model::User> {
        &self.users
    }

    /// Get the number of user collaborators.
    #[allow(dead_code)]
    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    /// Get the organization collaborators.
    #[allow(dead_code)]
    pub fn organizations(&self) -> &Vec<model::Organization> {
        &self.organizations
    }

    /// Get the number of organization collaborators.
    #[allow(dead_code)]
    pub fn organization_count(&self) -> usize {
        self.organizations.len()
    }

    /// Get the team collaborators.
    #[allow(dead_code)]
    pub fn teams(&self) -> &Vec<model::Team> {
        &self.teams
    }

    /// Get the number of team collaborators.
    #[allow(dead_code)]
    pub fn team_count(&self) -> usize {
        self.teams.len()
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaboratorCounts {
    users: u32,
    organizations: u32,
    teams: u32,
}

impl CollaboratorCounts {
    /// Get the number of user collaborators.
    #[allow(dead_code)]
    pub fn users(&self) -> u32 {
        self.users
    }

    /// Get the number of organization collaborators.
    #[allow(dead_code)]
    pub fn organizations(&self) -> u32 {
        self.organizations
    }

    /// Get the number of team collaborators.
    #[allow(dead_code)]
    pub fn teams(&self) -> u32 {
        self.teams
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeResponse {
    success: bool,
    message: Option<String>,
}

impl ChangeResponse {
    /// Test if the operation was successful.
    #[allow(dead_code)]
    pub fn success(&self) -> bool {
        self.success
    }

    /// Get a message associated with the change.
    #[allow(dead_code)]
    pub fn message(&self) -> Option<&String> {
        self.message.as_ref()
    }
}
