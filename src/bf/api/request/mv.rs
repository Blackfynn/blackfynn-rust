// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.
use serde_derive::Serialize;

use crate::bf::model::PackageId;

#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Move {
    things: Vec<PackageId>,
    destination: Option<PackageId>,
}
impl Move {
    pub fn new<D, T>(things: Vec<T>, destination: Option<D>) -> Self
    where
        T: Into<PackageId>,
        D: Into<PackageId>,
    {
        Self {
            things: things.into_iter().map(Into::into).collect::<Vec<_>>(),
            destination: destination.map(Into::into),
        }
    }
}
