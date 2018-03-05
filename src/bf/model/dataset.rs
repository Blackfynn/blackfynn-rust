use bf::model;

/// Representation of a Blackfynn API dataset

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    name: String,
    state: model::package::PackageState,
    description: Option<String>,
    package_type: String
}
