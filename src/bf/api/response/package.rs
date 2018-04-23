// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::api::response;
use bf::model;

// This corresponds to the `objects` map that is returned from `/packages/{:id}`
// when the `include=` parameter is provided.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Objects {
    source: Option<Vec<response::file::File>>,
    file: Option<Vec<response::file::File>>,
    view: Option<Vec<response::file::File>>,
}

/// A response wrapping a `model::Package`, along with additional metadata.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Package {
    channels: Option<Vec<response::channel::Channel>>,
    content: model::Package,
    children: Option<Vec<Package>>,
    objects: Option<Objects>,
}

impl Package {
    pub fn into_inner(self) -> model::Package {
        self.content
    }

    pub fn channels(&self) -> Option<&Vec<response::channel::Channel>> {
        self.channels.as_ref()
    }

    pub fn children(&self) -> Option<&Vec<Package>> {
        self.children.as_ref()
    }

    pub fn source(&self) -> Option<&Vec<response::file::File>> {
        match self.objects {
            Some(ref o) => o.source.as_ref(),
            None => None,
        }
    }

    pub fn file(&self) -> Option<&Vec<response::file::File>> {
        match self.objects {
            Some(ref o) => o.file.as_ref(),
            None => None,
        }
    }

    pub fn view(&self) -> Option<&Vec<response::file::File>> {
        match self.objects {
            Some(ref o) => o.view.as_ref(),
            None => None,
        }
    }
}
