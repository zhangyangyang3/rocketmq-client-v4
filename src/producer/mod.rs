use std::collections::HashMap;
use local_ip_address::local_ip;
use log::{ warn};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
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
    pub broker_stream: TcpStream,
    pub broker_id: i64,
    pub message_queue: i32,
    pub name_server: String,
    pub message_queue_map: HashMap<String, Vec<MessageQueue>>,
}

impl Producer {
    pub fn new(group_name: String, broker_stream: TcpStream, broker_id: i64, name_server: String) -> Producer {
        let ip = local_ip().unwrap();
        let pid = sysinfo::get_current_pid().unwrap().as_u32();
        let client_id = format!("{}@{}", ip, pid);
        Producer {
            group_name,
            client_id,
            broker_stream,
            broker_id,
            message_queue: 1,
            name_server,
            message_queue_map: HashMap::new(),
        }
    }


    pub async fn init(&mut self) {

        let stream = &mut self.broker_stream;
        let client_id = &self.client_id;
        let group_name = &self.group_name;
        let heart = HeartbeatData::new_producer_data(client_id.clone(), group_name.clone());
        heart.send_heartbeat(stream).await;

        // tokio::spawn(async move {
        //     loop {
        //         debug!("send producer heart beat: producer group{:?}", group_name.clone());
        //         let heart = HeartbeatData::new_producer_data(client_id.clone(), group_name.clone());
        //         heart.send_heartbeat(&mut stream).await;
        //         sleep(Duration::from_secs(5)).await;
        //     }
        // });

    }
    pub async fn send_message(&mut self, topic: String, message: Vec<u8>, key: String) -> Result<(), std::io::Error> {
        let mut properties = HashMap::new();
        properties.insert("KEYS".to_string(), key);
        self.send_with_properties(topic,  properties, message).await
    }

    pub async fn send_message_with_tag(&mut self, topic: String, tag: String, message: Vec<u8>, key: String)-> Result<(), std::io::Error> {
        let mut properties = HashMap::new();
        properties.insert("TAGS".to_string(), tag);
        properties.insert("KEYS".to_string(), key);
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
        let body = message.body;
        let cmd = MqCommand::new_with_body(request_code::SEND_MESSAGE_V2, vec![], header_bytes, body);
        let send = self.broker_stream.write_all(&cmd.to_bytes()).await;
        if send.is_err() {
            warn!("send message failed:{:?}", send);
            return send;
        }
        let _ = MqCommand::read_from_stream_with_opaque(&mut self.broker_stream, cmd.opaque).await;
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
    use crate::connection::MqConnection;

    #[tokio::test]
    async fn send_message_test() {
        let message_body = r#"{"id":"3910000000000056508","parentId":null,"senderId":"4296638931631415296","receiverId":null,"groupId":"4296643037620133888","type":"File","chatType":"Group","content":"{\"suffix\":\"\",\"url\":\"https://im-rc.s3.ap-southeast-1.amazonaws.com/public/dd2554235243522435.zip\",\"name\":\"im-patch.zip\",\"size\":\"60.23KB\"}","serverReceiveTime":"2024/09/23 09:37:47","createTime":"2024/09/23 09:37:44","displayContent":""}"#;
        let body = message_body.as_bytes().to_vec();

        let name_addr = "192.168.3.49:9876".to_string();
        let topic = "pushNoticeMessage_To".to_string();

        let cluster = MqConnection::get_cluster_info(&name_addr).await;
        let (tcp_stream, id) = MqConnection::get_broker_tcp_stream(&cluster).await;

        let mut producer = crate::producer::Producer::new("rust_send_group_1".to_string(),tcp_stream, id, name_addr.clone());
        producer.init().await;
        producer.send_message_with_tag(topic.clone(), "TAG1".to_string(), body, "3910000000000056508".to_string()).await.unwrap();
    }
}