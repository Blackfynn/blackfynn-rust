use bf::model;
use bf::api::response::channel::Channel;
use bf::api::response::file::File;

/// A type representing an API response containing a package.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Package {
    pub channels: Option<Vec<Channel>>,
    pub content: model::Package,
    pub objects: Vec<File>
}
