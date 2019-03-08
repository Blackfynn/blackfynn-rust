// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use serde_derive::Deserialize;

use crate::bf::model;

/// A response wrapping a `model::File`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct File {
    content: model::File,
}

impl File {
    pub fn take(self) -> model::File {
        self.content
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Files(Vec<File>);

impl Files {
    pub fn take(self) -> Vec<model::File> {
        self.0.into_iter().map(|file| file.take()).collect()
    }
}
