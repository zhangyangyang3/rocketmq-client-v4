use std::collections::HashMap;
use std::time::Duration;
use local_ip_address::local_ip;
use log::{debug, warn};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc};
use tokio::sync::mpsc::Receiver;
use tokio::time::sleep;
use crate::connection::MqConnection;
use crate::protocols::body::heartbeat_data::HeartbeatData;
use crate::protocols::body::message_queue::MessageQueue;
use crate::protocols::body::mq_message::MqMessage;
use crate::protocols::header::send_message_request_header::{SendMessageRequestHeader, SendMessageRequestHeaderV2};
use crate::protocols::{get_current_time_millis, PermName, request_code, SerializeDeserialize};
use crate::protocols::mq_command::MqCommand;

pub struct  Producer {

    pub group_name: String,
    pub client_id: String,
    pub message_queue: i32,
    pub name_server: String,
    pub message_queue_map: HashMap<String, Vec<MessageQueue>>,
    pub tx: mpsc::Sender<MqCommand>,
}

impl Producer {
    pub async fn new(group_name: String, name_server: String) -> Producer {
        let ip = local_ip().unwrap();
        let pid = sysinfo::get_current_pid().unwrap().as_u32();
        let client_id = format!("{}@{}", ip, pid);
        let (tx,rx) = mpsc::channel(1024);
        Self::init(rx, &name_server, tx.clone(), &client_id, &group_name).await;
        Producer {
            group_name,
            client_id,
            message_queue: 1,
            name_server,
            message_queue_map: HashMap::new(),
            tx,
        }
    }


    pub async fn init( mut rx: Receiver<MqCommand>, name_server: &str, tx: mpsc::Sender<MqCommand>
    , client_id: &str, producer_group: &str
    ) {

        let cluster = MqConnection::get_cluster_info(name_server).await;
        let (mut broker_stream, _) = MqConnection::get_broker_tcp_stream(&cluster).await;

        tokio::spawn(async move {
            loop {
                let cmd: MqCommand = rx.recv().await.unwrap();
                let send = broker_stream.write_all(&cmd.to_bytes()).await;
                if send.is_err() {
                    warn!("send command to mq failed. req cmd:{}, response error:{:?}", cmd.req_code, send.err());
                    continue;
                }
                let resp = MqCommand::read_from_stream(&mut broker_stream).await;
                if resp.req_code != 0 {
                    warn!("send command to mq failed. req cmd:{}, response code:{}, remark{:?}", cmd.req_code, resp.req_code, String::from_utf8(resp.r_body));
                } else {
                    debug!("read from mq success. req cmd:{}, resp remark:{:?}, ext field:{:?}", cmd.req_code, String::from_utf8(resp.r_body), String::from_utf8(resp.e_body));
                }
            }
        });

        let client = client_id.to_string();
        let producer_group = producer_group.to_string();
        tokio::spawn(async move {
            loop {
                let heart_beat = HeartbeatData::new_producer_data(client.clone(), producer_group.clone());
                let body = heart_beat.to_json_bytes();
                let body = MqCommand::new_with_body(request_code::HEART_BEAT, vec![], vec![], body);
                tx.send(body).await.unwrap();
                sleep(Duration::from_secs(5)).await;
            }
        });

    }
    pub async fn send_message(&mut self, topic: String, message: Vec<u8>, key: String) -> Result<(), std::io::Error> {
        let mut properties = HashMap::new();
        properties.insert("KEYS".to_string(), key);
        properties.insert("WAIT".to_string(), "true".to_string());
        self.send_with_properties(topic,  properties, message).await
    }

    pub async fn send_message_with_tag(&mut self, topic: String, tag: String, message: Vec<u8>, key: String)-> Result<(), std::io::Error> {
        let mut properties = HashMap::new();
        properties.insert("TAGS".to_string(), tag);
        properties.insert("KEYS".to_string(), key);
        properties.insert("WAIT".to_string(), "true".to_string());

        self.send_with_properties(topic, properties, message).await
    }

    pub async fn send_with_properties(&mut self, topic: String, properties: HashMap<String, String>, message: Vec<u8>) -> Result<(), std::io::Error> {
        let mut message = MqMessage::new(topic.clone(), message);
        message.properties = properties;

        if !self.message_queue_map.contains_key(&topic) {
            let mq_list = Self::fetch_message_write_queue(&self.name_server, &topic).await;
            self.message_queue_map.insert(topic.clone(), mq_list);
        }
        let queue_id: i32 = (get_current_time_millis() % self.message_queue_map.get(&topic).unwrap().len() as i64) as i32;

        let header = SendMessageRequestHeader::new(self.group_name.clone(), topic.clone(), queue_id, &message.properties);
        let header_bytes = SendMessageRequestHeaderV2::new(header).to_bytes_1();
        let body = message.encode_message();
        let cmd = MqCommand::new_with_body(request_code::SEND_BATCH_MESSAGE, vec![], header_bytes, body);
        let tx = self.tx.clone();
        tx.send(cmd).await.unwrap();
        Ok(())
    }


    async fn fetch_message_write_queue(name_server: &str, topic: &str) -> Vec<MessageQueue> {
        let topic_route_data = MqConnection::get_topic_route_data(name_server, topic).await;
        if topic_route_data.is_none() {
            warn!("no topic_route_data. may by you should create topic first");
            panic!("no topic_route_data. you should create topic first");
        }
        let topic_route_data = topic_route_data.unwrap();
        let mut qds = topic_route_data.queueDatas;
        qds.sort_by_key(|k| k.brokerName.clone());

        let mut mq_list = vec![];

        // this is subscribed message queue
        for qd in qds.iter_mut() {
            if PermName::is_writeable(qd.perm) {
                for i in 0..qd.writeQueueNums {
                    let mq = MessageQueue::new(topic.to_string(), qd.brokerName.clone(), i);
                    mq_list.push(mq);
                }
            }
        }
        mq_list
    }

}

#[cfg(test)]
mod send_test {
    use std::time::Duration;
    use log::LevelFilter;
    use time::UtcOffset;
    use uuid::Uuid;
    use crate::producer::Producer;

    #[tokio::test]
    async fn send_message_test() {

        let offset = UtcOffset::from_hms(8, 0, 0).unwrap();
        simple_logger::SimpleLogger::new().with_utc_offset(offset).with_level(LevelFilter::Debug).env().init().unwrap();

        let message_body = r#"{"id":"3910000000000056508"}"#;
        let body = message_body.as_bytes().to_vec();

        let name_addr = "192.168.3.49:9876".to_string();
        let topic = "topic_test_007".to_string();

        let mut producer = Producer::new("rust_send_group_1".to_string(), name_addr.clone()).await;
        for _i in 0..10 {
            let uid = Uuid::new_v4();
            producer.send_message(topic.clone(), body.clone(), uid.to_string()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }


    #[test]
    fn to_string_test () {
        let arr:[u8;166] = [ 0, 1, 97, 0, 0, 0, 17, 114, 117, 115, 116, 95, 115, 101, 110, 100, 95, 103, 114, 111, 117, 112, 95, 49, 0, 1, 98, 0, 0, 0, 14, 116, 111, 112, 105, 99, 95, 116, 101, 115, 116, 95, 48, 48, 55, 0, 1, 99, 0, 0, 0, 6, 84, 66, 87, 49, 48, 50, 0, 1, 100, 0, 0, 0, 4, 49, 48, 48, 48, 0, 1, 101, 0, 0, 0, 1, 54, 0, 1, 102, 0, 0, 0, 1, 48, 0, 1, 103, 0, 0, 0, 13, 49, 55, 50, 55, 49, 52, 51, 52, 52, 52, 57, 57, 48, 0, 1, 104, 0, 0, 0, 1, 48, 0, 1, 105, 0, 0, 0, 6, 75, 69, 89, 83, 49, 48, 0, 1, 106, 0, 0, 0, 1, 48, 0, 1, 107, 0, 0, 0, 5, 102, 97, 108, 115, 101, 0, 1, 108, 0, 0, 0, 1, 48, 0, 1, 109, 0, 0, 0, 5, 102, 97, 108, 115, 101,];
        let s = String::from_utf8_lossy(&arr);
        println!("{:?}", s);

        let arr:[u8;12] = [49, 55, 50, 55, 49, 52, 55, 51, 52, 49, 55, 49];
        let s = String::from_utf8_lossy(&arr);
        println!("{:?}", s);
    }
}