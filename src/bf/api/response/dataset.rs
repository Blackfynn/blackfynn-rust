// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::api::response::package::Package;
use bf::model;

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
    pub fn organization(&self) -> &String {
        &self.organization
    }

    pub fn owner(&self) -> &String {
        &self.owner
    }

    pub fn children(&self) -> Option<&Vec<Package>> {
        self.children.as_ref()
    }

    pub fn into_inner(self) -> model::Dataset {
        self.content
    }
}

impl AsRef<model::Dataset> for Dataset {
    fn as_ref(&self) -> &model::Dataset {
        &self.content
    }
}
