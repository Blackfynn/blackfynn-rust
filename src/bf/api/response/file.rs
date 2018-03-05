use bf::model;

/// A type representing an API response containing a file
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct File {
    pub content: model::File
}
