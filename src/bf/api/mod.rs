// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

//! The Blackfynn platform API.

mod client;
pub mod request;
pub mod response;

use std::borrow::Borrow;

pub use self::client::progress::{ProgressCallback, ProgressUpdate};

pub use self::client::Blackfynn;

/// Objects with a Blackfynn identifier implement this trait.
pub trait BFId {
    type Id: Clone + PartialEq + Into<String>;

    /// Gets the Blackfynn ID.
    fn id(&self) -> &Self::Id;
}

/// Objects with a Blackfynn-designated name implement this trait.
pub trait BFName {
    /// Gets the Blackfynn-designated name.
    fn name(&self) -> &String;
}

/// Objects that contain child objects implement this trait.
pub trait BFChildren {
    type Child: BFId + BFName;

    /// Get the child objects contained in the parent.
    fn children(&self) -> Option<&Vec<Self::Child>>;

    fn get_child_by_id<I>(&self, child_id: I) -> Option<&Self::Child>
    where
        I: Into<<<Self as BFChildren>::Child as BFId>::Id>,
    {
        let child_id: <<Self as BFChildren>::Child as BFId>::Id = child_id.into();
        let children: Option<&Vec<Self::Child>> = self.children();
        children.and_then(|children: &Vec<Self::Child>| {
            children.iter().map(|child| child.borrow()).find(|child| {
                let id: <<Self as BFChildren>::Child as BFId>::Id = child.id().clone();
                id == child_id
            })
        })
    }

    fn get_child_by_name<N: Into<String>>(&self, name: N) -> Option<&Self::Child> {
        let child_name: String = name.into();
        let children: Option<&Vec<Self::Child>> = self.children();
        children.and_then(|children: &Vec<Self::Child>| {
            children
                .iter()
                .map(|child| child.borrow())
                .find(|child| child.name() == &child_name)
        })
    }
}
