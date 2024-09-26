use std::sync::Arc;
use log::{debug, info, warn};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use crate::connection::{get_client_ip, MqConnection};
use crate::consumer::message_handler::MessageHandler;
use crate::protocols::body::consumer_data::{CONSUME_FROM_LAST_OFFSET, MESSAGE_MODEL_CLUSTER};
use crate::protocols::body::heartbeat_data::HeartbeatData;
use crate::protocols::body::message_body::MessageBody;
use crate::protocols::body::message_queue::MessageQueue;
use crate::protocols::body::subscription_data::SubscriptionData;
use crate::protocols::header::pull_message_request_header::PullMessageRequestHeader;
use crate::protocols::header::pull_message_response_header::PullMessageResponseHeader;
use crate::protocols::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use crate::protocols::header::update_consumer_offset_request_header::UpdateConsumerOffsetRequestHeader;
use crate::protocols::{PermName, response_code};
use crate::protocols::header::get_consumer_list_by_group_request_header::GetConsumerListByGroupRequestHeader;
use crate::protocols::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;

pub struct MqConsumer {
    pub name_server_addr: String,
    pub consume_group: String,
    pub client_id: String,
    pub topic: String,
    pub broadcast: bool,
    pub client_addr: String,
    pub broker_stream: TcpStream,
    pub broker_id: i64,
}

impl MqConsumer {

    pub fn new_consumer(name_server_addr: String, consume_group: String, topic: String, broker_stream: TcpStream, broker_id: i64) -> Self {

        let client_addr = get_client_ip();
        let mut client_id = String::from(&client_addr);
        client_id.push_str("@");
        client_id.push_str(sysinfo::get_current_pid().unwrap().as_u32().to_string().as_str());
        Self {
            name_server_addr,
            consume_group,
            client_id,
            topic,
            broadcast: false,
            client_addr,
            broker_stream,
            broker_id,
        }
    }


    pub async fn pull_message(&mut self, do_consume: Arc<impl MessageHandler>, rw_lock: Arc<RwLock<bool>>) {

        let heartbeat_data = HeartbeatData::new_pull_consumer_data(self.client_id.clone(), self.consume_group.clone(),
                                                                   CONSUME_FROM_LAST_OFFSET, SubscriptionData::simple_new(self.topic.clone()),
                                                                   MESSAGE_MODEL_CLUSTER.to_string());
        heartbeat_data.send_heartbeat(&mut self.broker_stream).await;

        loop {

            let mq_list = self.re_balance().await;
            if mq_list.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }

            let mut consumer_status = GetConsumerStatusRequestHeader::new(self.topic.clone(), self.consume_group.clone(), self.client_id.clone());
            consumer_status.send_request(&mut self.broker_stream, &mq_list).await;
            for mq in &mq_list {
                // 选择从message queue 中获取消息
                let mut offset = QueryConsumerOffsetRequestHeader::new(self.consume_group.clone(),mq.topic.clone(), mq.queueId)
                    .send_request(&mut self.broker_stream).await;
                info!("consumer offset:{:?}", offset);
                let cmd = PullMessageRequestHeader::new(self.consume_group.clone(), mq.topic.clone(), mq.queueId,
                                                        offset, 0)
                    .send_request(&mut self.broker_stream).await;
                if cmd.is_none() {
                    Self::sleep(100).await;
                    continue;
                }
                let cmd = cmd.unwrap();
                let response_header = PullMessageResponseHeader::bytes_to_header(cmd.header_serialize_method, cmd.e_body);
                info!("pull message response header:{:?}", &response_header);
                if response_header.is_some() {
                    let response_header = response_header.unwrap();
                    offset = if response_header.nextBeginOffset.is_none() {
                        response_header.offset.unwrap()
                    } else {
                        response_header.nextBeginOffset.unwrap()
                    };
                }
                match cmd.req_code {
                    response_code::SUCCESS => {
                        let r_body = String::from_utf8(cmd.r_body.clone()).unwrap();
                        match r_body.as_str() {
                            "FOUND" => {
                                let bodies = MessageBody::decode_from_bytes(cmd.body);
                                debug!("message count:{}", bodies.len());
                                for m in bodies {
                                    do_consume.handle(&m).await;
                                }
                                UpdateConsumerOffsetRequestHeader::new(self.consume_group.clone(), self.topic.clone(), mq.queueId, offset)
                                    .send_update_consumer_offset(&mut self.broker_stream).await;
                            }
                            _ => {
                                debug!("does not get message:{}", r_body);
                                Self::sleep(100).await;
                            }
                        }

                    }
                    _ => {
                        warn!("not support response code:{}, message:{:?}", cmd.req_code, String::from_utf8(cmd.r_body));
                    }
                }
                Self::sleep(100).await;
            }
            heartbeat_data.send_heartbeat(&mut self.broker_stream).await;

            {
                let read = rw_lock.read().await;
                if !*read {
                    info!("监听到中止服务，不再处理mq消息");
                    break;
                }
            }
        }


    }

    pub async fn fetch_message_queue(&mut self) -> Vec<MessageQueue> {
        let topic_route_data = MqConnection::get_topic_route_data(&self.name_server_addr, &self.topic).await;
        if topic_route_data.is_none() {
            warn!("no topic_route_data. may by you should create topic first");
            panic!("no topic_route_data.");
        }
        let topic_route_data = topic_route_data.unwrap();
        let mut qds = topic_route_data.queueDatas;
        qds.sort_by_key(|k| k.brokerName.clone());

        let mut mq_list = vec![];

        // this is subscribed message queue
        for qd in qds.iter_mut() {
            if PermName::is_readable(qd.perm) {
                for i in 0..qd.writeQueueNums {
                    let mq = MessageQueue::new(self.topic.clone(), qd.brokerName.clone(), i);
                    mq_list.push(mq);
                }
            }
        }

        // this is publish message queue
        // for qd in qds.iter_mut() {
        //     if PermName::is_writeable(qd.perm) {
        //         let mut broker_data = None;
        //         for bd in topic_route_data.brokerDatas.iter() {
        //             if bd.brokerName == qd.brokerName {
        //                 broker_data = Some(bd);
        //                 break;
        //             }
        //         }
        //         if broker_data.is_none() {
        //             continue;
        //         }
        //         let broker_data = broker_data.unwrap();
        //         //MixAll.MASTER_ID
        //         if !broker_data.brokerAddrs.contains_key("0") {
        //             continue;
        //         }
        //         for i in 0..qd.writeQueueNums {
        //             let mq = MessageQueue::new(self.topic.clone(), qd.brokerName.clone(), i);
        //             mq_list.push(mq);
        //         }
        //     }
        // }
        mq_list
    }


    // 返回需要处理的消费队列
    pub async fn re_balance(&mut self) -> Vec<MessageQueue> {
        let total_message_queue = self.fetch_message_queue().await;
        if total_message_queue.is_empty() {
            warn!("consumer list is empty");
            return vec![];
        }
        let stream = &mut self.broker_stream;
        let consumer_list =GetConsumerListByGroupRequestHeader::new(self.consume_group.clone())
            .query_consumer_list_by_group(stream).await;
        if !consumer_list.contains(&self.client_id) {
            warn!("current client id not in consumer list, current client id:{}, total list:{:?}", &self.client_id, &consumer_list);
            return vec![];
        }
        // average strategy
        //int index = cidAll.indexOf(currentCID);
        //         int mod = mqAll.size() % cidAll.size();
        //         int averageSize =
        //             mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
        //                 + 1 : mqAll.size() / cidAll.size());
        //         int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        //         int range = Math.min(averageSize, mqAll.size() - startIndex);
        //         for (int i = 0; i < range; i++) {
        //             result.add(mqAll.get((startIndex + i) % mqAll.size()));
        //         }
        //         return result;
        let mut idx = 0;
        for i in 0..consumer_list.len() {
            if consumer_list[i] == self.client_id {
                idx = i;
            }
        }
        let mod_val = total_message_queue.len() % consumer_list.len();
        let avg_size = if total_message_queue.len() <= consumer_list.len() {
            1
        } else if mod_val > 0 && idx < mod_val {
            total_message_queue.len() / consumer_list.len() + 1
        } else {
            total_message_queue.len() / consumer_list.len()
        };
        let start_idx = if mod_val > 0 && idx < mod_val {
            idx * avg_size
        } else {
            idx * avg_size + mod_val
        };

        let range = if avg_size < total_message_queue.len() - start_idx {
            avg_size
        } else {
            total_message_queue.len() - start_idx
        };

        let mut used_mqs: Vec<MessageQueue> = vec![];

        for i in 0..range {
            let t = &total_message_queue[(start_idx + i)%total_message_queue.len()];

            used_mqs.push(MessageQueue::new_from_ref(t));
        }

        info!("used consumer list:{:?}", &used_mqs);
        used_mqs
    }

    async fn sleep(ms: u64) {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }


}


#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::time::Duration;
    use log::{ info, LevelFilter};
    use time::UtcOffset;
    use tokio::sync::RwLock;
    use crate::connection::MqConnection;
    use crate::consumer::pull_consumer::MqConsumer;

    pub fn init_logger() {
        let offset = UtcOffset::from_hms(8, 0, 0).unwrap();
        simple_logger::SimpleLogger::new().with_utc_offset(offset).with_level(LevelFilter::Debug).env().init().unwrap();
    }
    #[tokio::test]
    async fn test_fetch_message_queue() {
        init_logger();
        let name_addr = "192.168.3.49:9876".to_string();
        let topic = "pushNoticeMessage_To".to_string();

        let cluster = MqConnection::get_cluster_info(&name_addr).await;
        info!("cluster info:{:?}", cluster);
        let (tcp_stream, id) = MqConnection::get_broker_tcp_stream(&cluster).await;

        info!("tcp stream:{:?}", tcp_stream);
        let mut consumer = MqConsumer::new_consumer(name_addr, "test_group".to_string(), topic, tcp_stream, id);
        let mq_list = consumer.fetch_message_queue().await;

        info!("message queue list:{:?}", mq_list);
    }


    struct Handler {}
    impl super::MessageHandler for Handler {
        async fn handle(&self, message: &super::MessageBody){
            info!("message body:{:?}", String::from_utf8(message.body.clone()));
        }
    }

    #[tokio::test]
    async fn pull_message_test() {
        init_logger();
        let name_addr = "192.168.3.49:9876".to_string();
        let topic = "pushNoticeMessage_To".to_string();

        let cluster = MqConnection::get_cluster_info(&name_addr).await;
        let (tcp_stream, id) = MqConnection::get_broker_tcp_stream(&cluster).await;

        info!("tcp stream:{:?}, id:{}", tcp_stream, id);
        // let mut consumer = MqConsumer::new_consumer(name_addr, "T_PushNoticeMessage_group".to_string(), topic, tcp_stream, id);
        let mut consumer = MqConsumer::new_consumer(name_addr, "consume_pushNoticeMessage_test_1".to_string(), topic, tcp_stream, id);
        let lock = Arc::new(RwLock::new(true));
        let lock1 = lock.clone();
        let handle = Arc::new(Handler {});
        tokio::spawn(async move {consumer.pull_message(handle, lock).await;});
        tokio::time::sleep(Duration::from_secs(60)).await;
        *lock1.write().await = false;

    }


}