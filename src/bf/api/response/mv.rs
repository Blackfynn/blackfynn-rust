use serde_derive::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveResponse {
    success: Vec<String>,
    failures: Vec<MoveFailure>,
    destination: Option<String>,
}

impl MoveResponse {
    pub fn success(&self) -> &Vec<String> {
        self.success.as_ref()
    }

    pub fn failures(&self) -> &Vec<MoveFailure> {
        self.failures.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveFailure {
    id: String,
    error: String,
}

impl MoveFailure {
    pub fn id(&self) -> &String {
        &self.id
    }

    pub fn error(&self) -> &String {
        &self.error
    }
}
