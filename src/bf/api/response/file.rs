// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// A response wrapping a `model::File`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct File {
    content: model::File,
}

impl File {
    pub fn into_inner(self) -> model::File {
        self.content
    }
}

impl AsRef<model::File> for File {
    fn as_ref(&self) -> &model::File {
        &self.content
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Files(Vec<File>);

impl Files {
    pub fn into_inner(self) -> Vec<model::File> {
        self.0.into_iter().map(|file| file.into_inner()).collect()
    }
}
