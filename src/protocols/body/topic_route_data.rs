use crate::protocols::body::broker_data::BrokerData;
use crate::protocols::body::queue_data::QueueData;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct TopicRouteData {
    pub orderTopicConf: Option<String>,
    pub queueDatas: Vec<QueueData>,
    pub brokerDatas: Vec<BrokerData>,
    pub filterServerTable: HashMap<String, Vec<String>>,
    //HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;
}
