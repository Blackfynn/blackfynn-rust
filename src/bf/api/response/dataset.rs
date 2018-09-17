// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::api::response::package::Package;
use bf::model;
use itertools::join;
use std::collections::HashMap;

/// A response wrapping a `model::Dataset`, along with and related metadata.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    organization: String,
    owner: String,
    children: Option<Vec<Package>>,
    content: model::Dataset,
}

impl Dataset {
    /// Get the associated organization.
    pub fn organization(&self) -> &String {
        &self.organization
    }

    /// Get the owner.
    pub fn owner(&self) -> &String {
        &self.owner
    }

    pub fn children(&self) -> Option<&Vec<Package>> {
        self.children.as_ref()
    }

    /// Unwrap the response into the contained model object.
    pub fn into_inner(self) -> model::Dataset {
        self.content
    }
}

impl AsRef<model::Dataset> for Dataset {
    fn as_ref(&self) -> &model::Dataset {
        &self.content
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

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaboratorChanges {
    changes: HashMap<String, ChangeResponse>,
    counts: CollaboratorCounts,
}

impl CollaboratorChanges {
    /// Get all the changes that occurred due to the share action.
    #[allow(dead_code)]
    pub fn changes(&self) -> &HashMap<String, ChangeResponse> {
        &self.changes
    }

    /// Get a count of all the changes that occurred due to the share action.
    #[allow(dead_code)]
    pub fn counts(&self) -> &CollaboratorCounts {
        &self.counts
    }

    /// Get a summary of the changes that occurred.
    #[allow(dead_code)]
    pub fn summary(&self) -> String {
        fn pluralize<'a>(s: &'a str, count: u32) -> String {
            let mut t = String::from(s);
            if count == 0 || count > 1 {
                t.push_str("s");
            }
            t
        }

        fn make_entry<'a>(thing: &'a str, count: u32) -> String {
            format!(
                "{count} {thing}",
                count = count,
                thing = pluralize(thing, count)
            )
        }

        let counts = self.counts();
        let users_count = counts.users();
        let orgs_count = counts.organizations();
        let teams_count = counts.teams();

        if users_count > 0 || orgs_count > 0 || teams_count > 0 {
            join(
                vec![
                    make_entry("user", users_count),
                    make_entry("organization", orgs_count),
                    make_entry("team", teams_count),
                    "changed.".to_string(),
                ],
                ", ",
            )
        } else {
            "No changes".to_owned()
        }
    }
}
