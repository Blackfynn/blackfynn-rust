// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Create {
    name: String,
    description: Option<String>,
}

impl Create {
    pub fn new<P, Q>(name: P, description: Option<Q>) -> Self
    where
        P: Into<String>,
        Q: Into<String>,
    {
        Self {
            name: name.into(),
            description: description.map(Into::into),
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Update {
    name: String,
    description: Option<String>,
}

impl Update {
    pub fn new<P, Q>(name: P, description: Option<Q>) -> Self
    where
        P: Into<String>,
        Q: Into<String>,
    {
        Self {
            name: name.into(),
            description: description.map(Into::into),
        }
    }
}
