/// Representation of a Blackfynn API package

/// A type encoding package state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PackageState {
    DELETING,
    ERROR,
    FAILED,
    PENDING,
    READY,
    RUNNABLE,
    RUNNING,
    STARTING,
    SUBMITTED,
    SUCCEEDED,
    UNAVAILABLE,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Package {
    name: String,
    dataset_id: String,
    package_state: PackageState,
    package_type: String   // TODO: convert to a typed representation
}
