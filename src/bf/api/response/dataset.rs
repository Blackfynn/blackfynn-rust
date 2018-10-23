// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::api::response::package::Package;
use bf::model;
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
        let mut text = String::new();
        let n = self.changes.len();
        for (i, (ref entity_id, ref change_response)) in self.changes.iter().enumerate() {
            let line = format!("{entity_id}: ", entity_id = entity_id);
            text.push_str(&line);
            if change_response.success() {
                text.push_str("OK");
            } else {
                let error = "[Something went wrong]".to_string();
                let blurb = change_response.message().unwrap_or(&error);
                text.push_str(blurb.as_str());
            }
            if (i + 1) < n {
                text.push_str("\n");
            }
        }
        text
    }
}
