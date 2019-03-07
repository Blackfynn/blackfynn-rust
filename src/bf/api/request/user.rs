// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use serde_derive::Serialize;

use crate::bf::model::OrganizationId;

/// A user HTTP `PUT` request.
#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    organization: Option<OrganizationId>,
    email: Option<String>,
    url: Option<String>,
    color: Option<String>,
    last_name: Option<String>,
    first_name: Option<String>,
    credential: Option<String>,
}

impl User {
    pub fn new(
        organization: Option<OrganizationId>,
        email: Option<String>,
        last_name: Option<String>,
        first_name: Option<String>,
    ) -> Self {
        Self {
            organization,
            email,
            url: None,
            color: None,
            last_name,
            first_name,
            credential: None,
        }
    }

    pub fn with_organization(organization: Option<OrganizationId>) -> Self {
        Self {
            organization,
            ..Default::default()
        }
    }
}

impl Default for User {
    fn default() -> Self {
        Self {
            organization: None,
            email: None,
            url: None,
            color: None,
            last_name: None,
            first_name: None,
            credential: None,
        }
    }
}
