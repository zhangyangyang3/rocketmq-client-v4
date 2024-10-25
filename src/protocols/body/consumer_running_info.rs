use crate::consumer::pull_consumer::MqConsumer;
use crate::consumer::pull_consumer_v2::PullConsumer;
use crate::protocols::body::consumer_data;
use crate::protocols::body::message_queue::MessageQueue;
use crate::protocols::body::subscription_data::SubscriptionData;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::{mq_command, response_code};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
pub struct ProcessQueueInfo {
    pub commitOffset: i64,
    pub cachedMsgMinOffset: i64,
    pub cachedMsgMaxOffset: i64,
    pub cachedMsgCount: i32,
    pub cachedMsgSizeInMiB: i32,
    pub transactionMsgMinOffset: i64,
    pub transactionMsgMaxOffset: i64,
    pub transactionMsgCount: i64,
    pub locked: bool,
    pub tryUnlockTimes: i64,
    pub lastLockTimestamp: i64,
    pub droped: bool,
    pub lastPullTimestamp: i64,
    pub lastConsumeTimestamp: i64,
}

impl ProcessQueueInfo {
    pub fn new() -> Self {
        Self {
            commitOffset: 0,
            cachedMsgMinOffset: 0,
            cachedMsgMaxOffset: 0,
            cachedMsgCount: 0,
            cachedMsgSizeInMiB: 0,
            transactionMsgMinOffset: 0,
            transactionMsgMaxOffset: 0,
            transactionMsgCount: 0,
            locked: false,
            tryUnlockTimes: 0,
            lastLockTimestamp: 0,
            droped: false,
            lastPullTimestamp: 0,
            lastConsumeTimestamp: 0,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ConsumerRunningInfo {
    pub properties: HashMap<String, String>,
    pub subscription_set: HashSet<SubscriptionData>,
    pub jstack: String,
    pub mq_table: HashMap<MessageQueue, ProcessQueueInfo>,
}

impl ConsumerRunningInfo {
    pub fn build_pull_consumer_running_info(
        consumer: &PullConsumer,
        message_queues: &Vec<MessageQueue>,
    ) -> Self {
        let sub = SubscriptionData::simple_new(consumer.topic.clone());
        let mut set = HashSet::new();
        set.insert(sub);
        let mut properties = HashMap::new();
        properties.insert(
            "PROP_CONSUMER_START_TIMESTAMP".to_string(),
            consumer.start_time.to_string(),
        );
        properties.insert(
            "PROP_NAMESERVER_ADDR".to_string(),
            consumer.name_server_addr.clone(),
        );
        properties.insert(
            "PROP_CLIENT_VERSION".to_string(),
            mq_command::VERSION_FLAG.to_string(),
        );
        properties.insert(
            "PROP_CONSUME_TYPE".to_string(),
            consumer_data::CONSUME_TYPE_PUSH.to_string(),
        );
        properties.insert("PROP_THREADPOOL_CORE_SIZE".to_string(), "1".to_string());
        let mut mq_table = HashMap::new();
        for x in message_queues {
            mq_table.insert(x.clone(), ProcessQueueInfo::new());
        }
        ConsumerRunningInfo {
            properties,
            subscription_set: set,
            jstack: "".to_string(),
            mq_table,
        }
    }

    pub fn build_consumer_running_info(consumer: MqConsumer) -> Self {
        let sub = SubscriptionData::simple_new(consumer.topic.clone());
        let mut set = HashSet::new();
        set.insert(sub);
        let mut properties = HashMap::new();
        properties.insert(
            "PROP_CONSUMER_START_TIMESTAMP".to_string(),
            consumer.start_time.to_string(),
        );
        properties.insert(
            "PROP_NAMESERVER_ADDR".to_string(),
            consumer.name_server_addr.clone(),
        );
        properties.insert(
            "PROP_CLIENT_VERSION".to_string(),
            mq_command::VERSION_FLAG.to_string(),
        );
        properties.insert(
            "PROP_CONSUME_TYPE".to_string(),
            consumer_data::CONSUME_TYPE_PUSH.to_string(),
        );
        properties.insert("PROP_THREADPOOL_CORE_SIZE".to_string(), "1".to_string());
        let mut mq_table = HashMap::new();
        for x in &consumer.message_queues {
            mq_table.insert(x.clone(), ProcessQueueInfo::new());
        }
        ConsumerRunningInfo {
            properties,
            subscription_set: set,
            jstack: "".to_string(),
            mq_table,
        }
    }

    pub fn to_command(&self, opaque: i32) -> MqCommand {
        let json = self.to_json();
        let mut cmd =
            MqCommand::new_with_body(response_code::SUCCESS, vec![], vec![], Vec::from(json));
        cmd.request_flag = 1;
        cmd.opaque = opaque;
        cmd
    }

    fn to_json(&self) -> String {
        let mut json = "".to_string();
        json.push_str("{");

        json.push_str("\"properties\":");
        let prop = serde_json::to_string(&self.properties).unwrap();
        json.push_str(&prop);
        json.push_str(",");

        json.push_str("\"subscriptionSet\":");
        let sub = serde_json::to_string(&self.subscription_set).unwrap();
        json.push_str(&sub);
        json.push_str(",");

        json.push_str("\"jstack\":\"{}\",");

        // mq_table
        json.push_str("\"mqTable\":{");
        for (k, v) in &self.mq_table {
            let k_json = serde_json::to_string(k).unwrap();
            let v_json = serde_json::to_string(v).unwrap();
            json.push_str(&k_json);
            json.push_str(":");
            json.push_str(&v_json);
            json.push_str(",");
        }
        if json.ends_with(",") {
            json.remove(json.len() - 1);
        }
        // end mq_table
        json.push_str("}");

        // end
        json.push_str("}");
        json
    }
}

#[cfg(test)]
mod test {
    use crate::consumer::pull_consumer::MqConsumer;
    use crate::protocols::body::consumer_running_info::ConsumerRunningInfo;
    use crate::protocols::body::message_queue::MessageQueue;

    #[test]
    fn json_test() {
        let topic = "test_topic";
        let broker_name = "broker_name";
        let mut consumer = MqConsumer::new_consumer(
            "127.0.0.1:9876".to_string(),
            "test_group".to_string(),
            topic.to_string(),
        );
        let mq1 = MessageQueue::new(topic.to_string(), broker_name.to_string(), 1);
        let mq2 = MessageQueue::new(topic.to_string(), broker_name.to_string(), 2);
        consumer.message_queues.push(mq1);
        consumer.message_queues.push(mq2);

        let running_info = ConsumerRunningInfo::build_consumer_running_info(consumer);
        println!("{}", running_info.to_json());
    }
}
