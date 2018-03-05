use bf::model;

/// A type representing an API response containing a channel
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Channel {
    pub content: model::Channel
}
