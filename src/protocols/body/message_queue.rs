use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
#[allow(non_snake_case)]
pub struct MessageQueue {
    pub topic: String,
    pub brokerName: String,
    pub queueId: i32
}

impl MessageQueue {
    pub fn new(topic: String, broker_name: String, queue_id: i32) -> Self {
        Self {
            topic,
            brokerName: broker_name,
            queueId: queue_id
        }
    }

    pub fn new_from_ref(ref_mq: &MessageQueue) -> Self {
        Self {
            topic: ref_mq.topic.clone(),
            brokerName: ref_mq.brokerName.clone(),
            queueId: ref_mq.queueId
        }
    }

}