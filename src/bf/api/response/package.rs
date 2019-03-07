// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use std::borrow::Borrow;
use std::ops::Deref;

use serde_derive::Deserialize;

use crate::bf::api::{response, BFChildren, BFId, BFName};
use crate::bf::model;

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

impl Borrow<model::Package> for Package {
    fn borrow(&self) -> &model::Package {
        &self.content
    }
}

impl Deref for Package {
    type Target = model::Package;
    fn deref(&self) -> &Self::Target {
        &self.content
    }
}

impl BFId for Package {
    type Id = model::PackageId;
    fn id(&self) -> &Self::Id {
        self.id()
    }
}

impl BFName for Package {
    fn name(&self) -> &String {
        self.name()
    }
}

impl BFChildren for Package {
    type Child = Self;
    fn children(&self) -> Option<&Vec<Self::Child>> {
        self.children()
    }
}

impl Package {
    /// Gets the ID of the package.
    pub fn id(&self) -> &model::PackageId {
        let p: &model::Package = self.borrow();
        p.id()
    }

    /// Gets the name of the package.
    pub fn name(&self) -> &String {
        let p: &model::Package = self.borrow();
        p.name()
    }

    /// Take ownership of the package wrapped by this response object.
    pub fn take(self) -> model::Package {
        self.content
    }

    /// Get the child packages contained in this package.
    pub fn children(&self) -> Option<&Vec<Self>> {
        self.children.as_ref()
    }

    /// Gets a collection of channels associated with this package.
    pub fn channels(&self) -> Option<&Vec<response::channel::Channel>> {
        self.channels.as_ref()
    }

    /// Gets the raw file sources backing this package.
    pub fn source(&self) -> Option<&Vec<response::file::File>> {
        match self.objects {
            Some(ref o) => o.source.as_ref(),
            None => None,
        }
    }

    /// Gets the processed files backing this package.
    pub fn file(&self) -> Option<&Vec<response::file::File>> {
        match self.objects {
            Some(ref o) => o.file.as_ref(),
            None => None,
        }
    }

    /// Gets the view files backing this package.
    pub fn view(&self) -> Option<&Vec<response::file::File>> {
        match self.objects {
            Some(ref o) => o.view.as_ref(),
            None => None,
        }
    }

    /// Fetch a package from a dataset by package ID.
    pub fn get_package_by_id(&self, package_id: model::PackageId) -> Option<model::Package> {
        self.get_child_by_id(package_id).map(|p| p.clone().take())
    }

    /// Fetch a package from a dataset by package name.
    pub fn get_package_by_name<N: Into<String>>(&self, package_name: N) -> Option<model::Package> {
        self.get_child_by_name(package_name)
            .map(|p| p.clone().take())
    }
}
