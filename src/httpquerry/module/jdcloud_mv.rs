use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct JdcloudRequest<T: Serialize> {
    pub ak: String,
    pub sk: String,
    pub service_name: String,
    pub region: String,
    pub parameters: Option<T>,
}

#[derive(Deserialize, Serialize)]
pub struct InstanceId {
    pub instance_id: String,
}

impl JdcloudRequest<InstanceId> {}