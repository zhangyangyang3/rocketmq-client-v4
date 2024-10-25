use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct QueueData {
    pub brokerName: String,
    pub readQueueNums: i32,
    pub writeQueueNums: i32,
    pub perm: i32,
    pub topicSysFlag: i32,
}
