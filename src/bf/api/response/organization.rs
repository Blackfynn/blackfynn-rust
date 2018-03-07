// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Organization {
    pub is_admin: bool,
    pub is_owner: bool,
    pub owners: Vec<model::User>,
    pub administrators: Vec<model::User>,
    pub organization: model::Organization
}

/// A type representing a listing of organizations a user is a member of
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Organizations {
    pub organizations: Vec<Organization>
}
