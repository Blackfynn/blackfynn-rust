/// Representation of a Blackfynn API channel

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Channel {
    name: String,
    rate: f64,
    start: i64,
    end: i64,
    unit: String,
    spike_duration: i64,
    channel_type: String,
    group: String
}

impl Channel {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn rate(&self) -> f64 {
        self.rate
    }

    pub fn start(&self) -> i64 {
        self.start
    }

    pub fn end(&self) -> i64 {
        self.end
    }

    pub fn spike_duration(&self) -> i64 {
        self.spike_duration
    }

    pub fn channel_type(&self) -> &String {
        &self.channel_type
    }

    pub fn group(&self) -> &String {
        &self.group
    }
}
