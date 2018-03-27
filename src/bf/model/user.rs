// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// A user, as defined by the Blackfynn API
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    id: String,
    first_name: String,
    last_name: String,
    email: String,
    preferred_organization: Option<model::OrganizationId>,
}

impl User {
    pub fn id(&self) -> &String {
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
