// Copyright (c) 2018 Blackfynn, Inc. All Rights Reserved.

#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
// aka ConceptPayload
pub struct CreateModel {
    name: String,
    display_name: String,
    description: String,
    locked: bool,
    template_id: Option<String>,
}

impl CreateModel {
    #[allow(dead_code)]
    pub fn new(name: String, display_name: String) -> Self {
        Self {
            name,
            display_name,
            description: "".into(),
            locked: false,
            template_id: None,
        }
    }

    #[allow(dead_code)]
    pub fn set_description(mut self, description: String) -> Self {
        self.description = description;
        self
    }

    #[allow(dead_code)]
    pub fn set_locked(mut self, locked: bool) -> Self {
        self.locked = locked;
        self
    }

    #[allow(dead_code)]
    pub fn set_template_id(mut self, template_id: String) -> Self {
        self.template_id = Some(template_id);
        self
    }
}

pub type UpdateModel = CreateModel;

#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
// aka InstanceDatumPayload
pub struct CreateRecordDatum {
    name: String,
    value: Option<String>,
}

impl CreateRecordDatum {
    #[allow(dead_code)]
    pub fn empty(name: String) -> Self {
        Self { name, value: None }
    }

    #[allow(dead_code)]
    pub fn new(name: String, value: String) -> Self {
        Self {
            name,
            value: Some(value),
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
// aka InstanceDataPayloadWrapper
pub struct CreateRecord {
    values: Vec<CreateRecordDatum>,
}

impl CreateRecord {
    #[allow(dead_code)]
    pub fn empty() -> Self {
        Self { values: vec![] }
    }

    #[allow(dead_code)]
    pub fn new(values: Vec<CreateRecordDatum>) -> Self {
        Self { values }
    }

    #[allow(dead_code)]
    pub fn append(mut self, datum: CreateRecordDatum) -> Self {
        self.values.push(datum);
        self
    }
}
