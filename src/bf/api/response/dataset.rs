use bf::model;
use bf::api::response::package::Package;

/// A type representing an API response containing a dataset
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub organization: String,
    pub owner: String,
    pub children: Option<Vec<Package>>,
    pub content: model::Dataset
}
