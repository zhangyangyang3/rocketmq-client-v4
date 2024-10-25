use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct BrokerData {
    pub cluster: String,
    pub brokerName: String,
    pub brokerAddrs: HashMap<String, String>,
}
