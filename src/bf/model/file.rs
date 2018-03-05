/// Representation of a Blackfynn API file

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FileObjectType {
    File,
    View,
    Source
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct File {
    name: String,
    file_type: String, //TODO Make this typed
    s3bucket: String,
    s3key: String,
    object_type: FileObjectType,
    size: usize
}

impl File {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn file_type(&self) -> &String {
        &self.file_type
    }

    pub fn s3_bucket(&self) -> &String {
        &self.s3bucket
    }

    pub fn s3_key(&self) -> &String {
        &self.s3key
    }

    pub fn object_type(&self) -> &FileObjectType {
        &self.object_type
    }

    pub fn size(&self) -> usize {
        self.size
    }
}
