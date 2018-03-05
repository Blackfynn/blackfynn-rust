// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

/// An organization, as defined by the Blackfynn API

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Organization {
    id: String,
    name: String
}

impl Organization {
    pub fn name(&self) -> &String {
        &self.name
    }
}
