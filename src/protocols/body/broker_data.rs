use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct BrokerData {
    pub cluster: String,
    pub brokerName: String,
    pub brokerAddrs: HashMap<String, String>
}
