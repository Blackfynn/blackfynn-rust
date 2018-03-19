// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

/// A user, as defined by the Blackfynn API
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    id: String,
    first_name: String,
    last_name: String,
    email: String,
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
}
