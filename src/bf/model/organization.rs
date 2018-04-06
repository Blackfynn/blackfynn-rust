// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

use bf::model;

/// An identifier for an organization on the Blackfynn platform.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrganizationId(String);

impl OrganizationId {
    #[allow(dead_code)]
    pub fn new<S: Into<String>>(id: S) -> Self {
        OrganizationId(id.into())
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<String> for OrganizationId {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl AsRef<str> for OrganizationId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<OrganizationId> for String {
    #[allow(dead_code)]
    fn from(id: OrganizationId) -> Self {
        id.0
    }
}

impl From<String> for OrganizationId {
    fn from(id: String) -> Self {
        OrganizationId::new(id)
    }
}

/// An organization.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Organization {
    id: OrganizationId,
    name: String,
    slug: String,
    encryption_key_id: model::S3EncryptionKeyId
}

impl Organization {
    #[allow(dead_code)]
    pub fn id(&self) -> &OrganizationId {
        &self.id
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &String {
        &self.name
    }

    #[allow(dead_code)]
    pub fn slug(&self) -> &String {
        &self.slug
    }

    #[allow(dead_code)]
    pub fn encryption_key_id(&self) -> &model::S3EncryptionKeyId {
        &self.encryption_key_id
    }
}
