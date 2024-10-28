use crate::connection::{get_client_ip, MqConnection};
use crate::consumer::message_handler::MessageHandler;
use crate::protocols::body::consumer_data::{CONSUME_FROM_LAST_OFFSET, MESSAGE_MODEL_CLUSTER};
use crate::protocols::body::consumer_running_info::ConsumerRunningInfo;
use crate::protocols::body::heartbeat_data::HeartbeatData;
use crate::protocols::body::message_body::MessageBody;
use crate::protocols::body::message_queue::MessageQueue;
use crate::protocols::body::subscription_data::SubscriptionData;
use crate::protocols::header::get_consumer_list_by_group_request_header::GetConsumerListByGroupRequestHeader;
use crate::protocols::header::get_consumer_running_info_requestheader::GetConsumerRunningInfoRequestHeader;
use crate::protocols::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
use crate::protocols::header::pull_message_request_header::PullMessageRequestHeader;
use crate::protocols::header::pull_message_response_header::PullMessageResponseHeader;
use crate::protocols::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use crate::protocols::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use crate::protocols::header::update_consumer_offset_request_header::UpdateConsumerOffsetRequestHeader;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::{get_current_time_millis, PermName, request_code, response_code, SerializeDeserialize};
use dashmap::DashMap;
use log::{debug, info, warn};
use std::ops::Deref;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use crate::protocols::body::topic_route_data::TopicRouteData;

#[derive(Debug, Clone)]
pub struct PullConsumer {
    pub name_server_addr: String,
    pub consume_group: String,
    pub client_id: String,
    pub topic: String,
    pub broadcast: bool,
    pub client_addr: String,
    pub start_time: i64,
}

impl PullConsumer {
    pub fn new(name_server_addr: String, consume_group: String, topic: String) -> PullConsumer {
        let client_addr = get_client_ip();
        let mut client_id = String::from(&client_addr);
        client_id.push_str("@");
        client_id.push_str(
            sysinfo::get_current_pid()
                .unwrap()
                .as_u32()
                .to_string()
                .as_str(),
        );
        Self {
            name_server_addr,
            consume_group,
            client_id,
            topic,
            broadcast: false,
            client_addr,
            start_time: get_current_time_millis(),
        }
    }
    pub async fn start_consume(
        &self,
        do_consume: Arc<impl MessageHandler + Send + Sync + 'static>,
        run: Arc<RwLock<bool>>,
    ) {
        let cluster_info = MqConnection::get_cluster_info(self.name_server_addr.as_str()).await;
        let mut brokers = MqConnection::get_all_master_broker_stream(&cluster_info).await;
        let consumer = Arc::new(self.clone());
        let len = brokers.len();
        for _ in 0..len {
            let broker = brokers.pop().unwrap();
            let (read_half, write) = broker.into_split();
            let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<MqCommand>(1024);
            let (msg_tx, msg_rx) = tokio::sync::mpsc::channel::<MessageBody>(1024);
            let cmd_map: Arc<DashMap<i32, MqCommand>> = Arc::new(DashMap::new());
            let queue_list: Arc<RwLock<Vec<MessageQueue>>> = Arc::new(RwLock::new(vec![]));
            let queue_offset_map = Arc::new(DashMap::<i32, i64>::new());
            let consumer = consumer.clone();
            let run = run.clone();
            Self::send_heartbeat(cmd_tx.clone(), consumer.clone()).await;
            Self::read_cmd(
                read_half,
                cmd_tx.clone(),
                cmd_map.clone(),
                queue_offset_map.clone(),
                queue_list.clone(),
                msg_tx.clone(),
                consumer.clone(),
                run.clone(),
            )
            .await;

            Self::write_cmd(cmd_rx, write, cmd_map.clone(), run.clone()).await;

            Self::do_pull_message(
                cmd_tx.clone(),
                consumer.clone(),
                queue_list.clone(),
                queue_offset_map.clone(),
                run.clone(),
            )
            .await;

            let do_consume = do_consume.clone();
            Self::consume_rx(do_consume, msg_rx, run.clone()).await;
        }
    }

    async fn write_cmd(
        mut cmd_rx: Receiver<MqCommand>,
        mut write: OwnedWriteHalf,
        cmd_map: Arc<DashMap<i32, MqCommand>>,
        run: Arc<RwLock<bool>>,
    ) {
        tokio::spawn(async move {
            loop {
                if !*run.read().await {
                    info!("stop write mq cmd to server");
                    let _ = write.shutdown().await;
                    break;
                }
                let cmd = cmd_rx.recv().await;
                match cmd {
                    None => {
                        break;
                    }
                    Some(real_cmd) => {
                        Self::write_cmd_to_mq(real_cmd, &mut write, cmd_map.clone()).await;
                    }
                }
            }
        });
    }

    async fn read_cmd(
        mut read_half: OwnedReadHalf,
        cmd_tx: Sender<MqCommand>,
        cmd_map: Arc<DashMap<i32, MqCommand>>,
        queue_offset_map: Arc<DashMap<i32, i64>>,
        mut queue_list: Arc<RwLock<Vec<MessageQueue>>>,
        msg_tx: Sender<MessageBody>,
        consumer: Arc<PullConsumer>,
        run: Arc<RwLock<bool>>,
    ) {
        tokio::spawn(async move {
            loop {
                if !*run.read().await {
                    info!("stop read and pull mq cmd");
                    break;
                }

                let readable = read_half.readable().await;
                if readable.is_err() {
                    warn!("mq broker stream broken. remote addr:{:?}", read_half.peer_addr());
                    break;
                }
                let broker_addr = read_half.peer_addr().unwrap().to_string();

                // read cmd from mq
                let server_cmd = MqCommand::read_from_read_half(&mut read_half).await;
                if server_cmd.is_none() {
                    warn!("read cmd from mq failed, remote addr:{:?}", read_half.peer_addr());
                    break;
                }
                let server_cmd = server_cmd.unwrap();
                // debug!(
                //     "read cmd from server,:{:?}opaque:{}, req_code:{}, flag:{}",
                //     read_half.local_addr(),
                //     server_cmd.opaque,
                //     server_cmd.req_code,
                //     server_cmd.request_flag
                // );

                // server send request
                match server_cmd.req_code {
                    request_code::NOTIFY_CONSUMER_IDS_CHANGED => {
                        // re balance
                        let header =
                            NotifyConsumerIdsChangedRequestHeader::convert_from_cmd(&server_cmd);
                        let consume_group = header.consumerGroup.as_str();

                        let req =
                            GetConsumerListByGroupRequestHeader::new(consume_group.to_string())
                                .to_command();
                        cmd_tx.send(req).await.unwrap();
                        info!("NOTIFY_CONSUMER_IDS_CHANGED, do re balance");
                    }

                    response_code::TOPIC_NOT_EXIST => {
                        warn!(
                            "topic not exits:{:?}",
                            String::from_utf8(server_cmd.r_body.clone())
                        );
                    }

                    request_code::GET_CONSUMER_RUNNING_INFO => {
                        let header =
                            GetConsumerRunningInfoRequestHeader::convert_from_command(&server_cmd);
                        if header.consumerGroup != consumer.consume_group
                            || header.clientId != consumer.client_id
                        {
                            warn!(
                                "GET_CONSUMER_RUNNING_INFO, server req:{:?}, current consumer:{:?}",
                                header, consumer
                            );
                            return;
                        }
                        let queues = queue_list.read().await;
                        let run_info = ConsumerRunningInfo::build_pull_consumer_running_info(
                            &consumer,
                            queues.deref(),
                        );
                        let cmd = run_info.to_command(server_cmd.opaque);
                        cmd_tx.send(cmd).await.unwrap();
                    }

                    _ => {
                        let req_cmd = cmd_map.remove(&server_cmd.opaque);
                        if req_cmd.is_none() {
                            warn!(
                                "can not find request cmd for opaque:{}, req_cmd:{:?}",
                                server_cmd.opaque, req_cmd
                            );
                            return;
                        }
                        let req_cmd = req_cmd.unwrap().1;
                        match req_cmd.req_code {
                            request_code::PULL_MESSAGE => {
                                Self::do_consume_message(
                                    server_cmd,
                                    msg_tx.clone(),
                                    cmd_tx.clone(),
                                    queue_offset_map.clone(),
                                    &consumer,
                                )
                                .await;
                            }
                            request_code::HEART_BEAT => {
                                debug!(
                                    "send heart beat. resp code:{:?}. remark:{:?}",
                                    server_cmd.req_code,
                                    String::from_utf8(server_cmd.r_body.clone())
                                );
                            }
                            request_code::QUERY_CONSUMER_OFFSET => {
                                let req =
                                    QueryConsumerOffsetRequestHeader::convert_from_cmd(&req_cmd);
                                let offset =
                                    QueryConsumerOffsetResponseHeader::convert_from_command(
                                        server_cmd,
                                    );
                                let offset = match offset {
                                    None => -1,
                                    Some(o) => o.offset,
                                };
                                if offset >= 0 {
                                    queue_offset_map.insert(req.queueId, offset);
                                }
                            }
                            request_code::GET_CONSUMER_LIST_BY_GROUP => {
                                Self::do_balance(&req_cmd, &server_cmd, &consumer, &mut queue_list, broker_addr.as_str())
                                    .await;
                            }
                            request_code::UPDATE_CONSUMER_OFFSET => {
                                // do nothing
                                // update offset
                                // let header = UpdateConsumerOffsetRequestHeader::convert_from_command(&req_cmd);
                                // set_local_consumer_offset(header.topic.as_str(), header.queueId, header.commitOffset).await;
                            }
                            response_code::SUCCESS => {
                                info!("get server resp. req cmd:{:?}, opaque:{:?},remark:{:?}, extend:{:?}, body:{:?}"
                                            , req_cmd.req_code, req_cmd.opaque, String::from_utf8(server_cmd.r_body.clone())
                                            , String::from_utf8(server_cmd.e_body.clone()), String::from_utf8(server_cmd.body));
                            }

                            code => {
                                warn!("get an not support request code:{:?}", code);
                            }
                        }
                    }
                }
            }
        });
    }

    async fn write_cmd_to_mq(
        cmd: MqCommand,
        write: &mut OwnedWriteHalf,
        cmd_map: Arc<DashMap<i32, MqCommand>>,
    ) {
        // send cmd to mq
        // debug!(
        //     "write cmd to server:{:?}, req code:{}, opaque:{}",
        //     write.local_addr(),
        //     cmd.req_code,
        //     cmd.opaque
        // );
        let req_code = cmd.req_code;
        let opaque = cmd.opaque;
        let bytes = cmd.to_bytes();
        if cmd.request_flag == 0 {
            cmd_map.insert(opaque, cmd);
        }
        let w = write.write_all(&bytes).await;
        if w.is_err() {
            warn!(
                "write to rocketmq failed, req_code:{}, opaque:{}, err:{:?}",
                req_code,
                opaque,
                w.err()
            )
        }
    }

    pub async fn do_balance(
        req_cmd: &MqCommand,
        resp_cmd: &MqCommand,
        consumer: &Arc<PullConsumer>,
        queue_list: &mut Arc<RwLock<Vec<MessageQueue>>>,
        broker_addr: &str
    ) {
        let consumer_list = GetConsumerListByGroupRequestHeader::build_consumer_list(&resp_cmd);
        let req_header = GetConsumerListByGroupRequestHeader::build_from_cmd(&req_cmd);
        let consumer_group = req_header.consumerGroup.as_str();
        if consumer.consume_group == consumer_group {
            if !consumer_list.contains(&consumer.client_id) {
                warn!(
                    "current client id not in consumer list, current client id:{}, total list:{:?}",
                    consumer.client_id, &consumer_list
                );
                return;
            }
            let used_mqs: Vec<MessageQueue> =
                Self::calc_used_queues(&consumer_list, consumer, broker_addr).await;
            info!("used consumer list:{:?}", &used_mqs);
            let mut write_lock = queue_list.write().await;
            *write_lock = used_mqs;
        } else {
            warn!("current consumer group:{}, server request consumer group:{}, not equal. can not do re balance", consumer.consume_group, consumer_group);
        }
    }

    async fn do_consume_message(
        server_cmd: MqCommand,
        msg_sender: Sender<MessageBody>,
        cmd_sender: Sender<MqCommand>,
        queue_offset_map: Arc<DashMap<i32, i64>>,
        consumer: &Arc<PullConsumer>,
    ) {
        let response_header = PullMessageResponseHeader::bytes_to_header(
            server_cmd.header_serialize_method,
            server_cmd.e_body,
        );
        // debug!("pull message response header:{:?}", &response_header);
        let mut offset = 0;
        if response_header.is_some() {
            let response_header = response_header.unwrap();
            offset = response_header.nextBeginOffset.unwrap();
        }

        match server_cmd.req_code {
            response_code::SUCCESS => {
                let r_body = String::from_utf8(server_cmd.r_body.clone()).unwrap();
                match r_body.as_str() {
                    "FOUND" => {
                        let bodies = MessageBody::decode_from_bytes(server_cmd.body);

                        debug!("message count:{}", bodies.len());
                        let temp = bodies.get(0).unwrap();
                        let old_offset = match queue_offset_map.get(&temp.queue_id) {
                            Some(v) => v.value().clone(),
                            None => -1,
                        };
                        if old_offset >= offset {
                            debug!(
                                "topic:{}, queue:{}, offset:{} already pulled.",
                                temp.topic.as_str(),
                                temp.queue_id,
                                offset
                            );
                            return;
                        }

                        for m in &bodies {
                            debug!(
                                "send msg to consume:{},{},{}",
                                m.topic.as_str(),
                                m.queue_id,
                                m.msg_id.as_str()
                            );

                            let send = msg_sender.send(m.clone()).await;
                            if send.is_err() {
                                warn!("consume message failed:{:?}", send.err());
                                // todo should re consume
                            }
                        }

                        queue_offset_map.insert(temp.queue_id, offset);
                        let update_offset = UpdateConsumerOffsetRequestHeader::new(
                            consumer.consume_group.clone(),
                            consumer.topic.clone(),
                            temp.queue_id,
                            offset,
                        )
                        .command();
                        cmd_sender.send(update_offset).await.unwrap();
                    }
                    _ => {
                        debug!("does not get message:{}", r_body);
                        // sleep(10).await;
                    }
                }
            }
            response_code::PULL_NOT_FOUND => {
                // debug!("no message found");
                // sleep(10).await;
            }
            _ => {
                warn!(
                    "not support response code:{}, message:{:?}",
                    server_cmd.req_code,
                    String::from_utf8(server_cmd.r_body)
                );
            }
        }
    }

    async fn send_heartbeat(tx: Sender<MqCommand>, consumer: Arc<PullConsumer>) {
        tokio::spawn(async move {
            let heartbeat_data = HeartbeatData::new_push_consumer_data(
                consumer.client_id.clone(),
                consumer.consume_group.clone(),
                CONSUME_FROM_LAST_OFFSET,
                SubscriptionData::simple_new(consumer.topic.clone()),
                MESSAGE_MODEL_CLUSTER.to_string(),
            );
            let heartbeat_data = heartbeat_data.to_json_bytes();
            let heartbeat_cmd =
                MqCommand::new_with_body(request_code::HEART_BEAT, vec![], vec![], heartbeat_data);
            tx.send(heartbeat_cmd).await.unwrap();
        });
    }

    async fn do_pull_message(
        tx: Sender<MqCommand>,
        consumer: Arc<PullConsumer>,
        message_queues: Arc<RwLock<Vec<MessageQueue>>>,
        queue_offset_map: Arc<DashMap<i32, i64>>,
        run: Arc<RwLock<bool>>,
    ) {
        tokio::spawn(async move {
            loop {
                if !*run.read().await {
                    info!("stop pull message");
                    break;
                }

                let read_lock = message_queues.read().await;
                if (*read_lock).is_empty() {
                    info!("fetch consumer message queue info:{:?}", consumer);
                    let cmd =
                        GetConsumerListByGroupRequestHeader::new(consumer.consume_group.clone())
                            .to_command();
                    let send = tx.send(cmd).await;
                    match send {
                        Ok(_) => {}
                        Err(err) => {
                            warn!(
                                "send GetConsumerListByGroupRequestHeader to channel failed:{:?}",
                                err
                            );
                        }
                    }
                    Self::sleep(100).await;
                    continue;
                }
                for queue in read_lock.iter() {
                    let offset = match queue_offset_map.get(&queue.queueId) {
                        None => -1,
                        Some(kv) => kv.value().clone(),
                    };

                    if offset < 0 {
                        info!("fetch queue offset. queue:{:?}", queue);
                        let cmd = QueryConsumerOffsetRequestHeader::new(
                            consumer.consume_group.clone(),
                            consumer.topic.clone(),
                            queue.queueId,
                        )
                        .to_command();
                        let opaque = cmd.opaque;
                        debug!("send fetch queue offset, opaque:{}", opaque);
                        tx.send(cmd).await.unwrap();
                        Self::sleep(10).await;
                        continue;
                    }

                    let cmd = PullMessageRequestHeader::new(
                        consumer.consume_group.clone(),
                        consumer.topic.clone(),
                        queue.queueId,
                        offset,
                        0,
                    )
                    .to_command();
                    tx.send(cmd).await.unwrap();
                }
                Self::sleep(100).await;
            }
        });
    }

    pub async fn consume_rx(
        do_consume: Arc<impl MessageHandler + Send + Sync + 'static>,
        mut msg_rx: Receiver<MessageBody>,
        run: Arc<RwLock<bool>>,
    ) {
        tokio::spawn(async move {
            loop {
                if !*run.read().await {
                    info!("stop consume message");
                    break;
                }
                let body = msg_rx.recv().await;
                match body {
                    None => {
                        debug!("channel closed");
                        break;
                    }
                    Some(msg_body) => {
                        debug!(
                        "do consume msg:{},{},{}",
                        msg_body.topic.as_str(),
                        msg_body.queue_id,
                        msg_body.msg_id
                    );
                        do_consume.handle(&msg_body).await;
                    }
                }
            }
        });
    }

    async fn calc_used_queues(
        consumer_list: &Vec<String>,
        consumer: &Arc<PullConsumer>,
        broker_addr: &str
    ) -> Vec<MessageQueue> {
        let mut used_mqs: Vec<MessageQueue> = vec![];
        if !consumer_list.contains(&consumer.client_id) {
            warn!(
                "current client id not in consumer list, current client id:{}, total list:{:?}",
                &consumer.client_id, &consumer_list
            );
            return used_mqs;
        }

        let total_message_queue =
            Self::fetch_message_queue(consumer.name_server_addr.as_str(), consumer.topic.as_str(), broker_addr).await;
        if total_message_queue.is_empty() {
            warn!("consumer list is empty");
            return used_mqs;
        }
        let mut idx = 0;
        for i in 0..consumer_list.len() {
            if consumer_list[i] == consumer.client_id {
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
        for i in 0..range {
            let t = &total_message_queue[(start_idx + i) % total_message_queue.len()];

            used_mqs.push(MessageQueue::new_from_ref(t));
        }

        used_mqs
    }

    async fn sleep(ms: u64) {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }


    pub async fn fetch_message_queue(name_server: &str, topic: &str, broker_addr: &str) -> Vec<MessageQueue> {
        let topic_route_data = MqConnection::get_topic_route_data(name_server, topic).await;
        if topic_route_data.is_none() {
            warn!("no topic_route_data. may by you should create topic first");
            panic!("no topic_route_data.");
        }

        let topic_route_data = topic_route_data.unwrap();
        let broker_name = Self::get_broker_name_by_addr(&topic_route_data, broker_addr);
        let mut qds = topic_route_data.queueDatas;
        qds.sort_by_key(|k| k.brokerName.clone());

        let mut mq_list = vec![];

        // this is subscribed message queue
        for qd in qds.iter_mut() {
            if PermName::is_readable(qd.perm) && qd.brokerName == broker_name {
                for i in 0..qd.readQueueNums {
                    let mq = MessageQueue::new(topic.to_string(), qd.brokerName.clone(), i);
                    mq_list.push(mq);
                }
            }
        }
        mq_list
    }

    fn get_broker_name_by_addr(topic_rote_data: &TopicRouteData, broker_addr: &str) -> String {
        for x in &topic_rote_data.brokerDatas {
            for v in x.brokerAddrs.values() {
                if v == broker_addr {
                    return x.brokerName.clone();
                }
            }
        }
        panic!("no broker addr : {} found in route data:{:?}", broker_addr, topic_rote_data);
    }
}

#[cfg(test)]
mod test {
    use crate::consumer::pull_consumer_v2::PullConsumer;
    use log::{info, LevelFilter};
    use std::sync::Arc;
    use std::time::Duration;
    use time::UtcOffset;
    use tokio::sync::RwLock;

    struct Handler {}

    unsafe impl Send for Handler {

    }

    unsafe impl Sync for Handler {}
    impl super::MessageHandler for Handler {
        async fn handle(&self, message: &super::MessageBody) {
            info!(
                "message body:{:?}",
                String::from_utf8(message.body.clone()).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn pull_message_test() {
        let offset = UtcOffset::from_hms(8, 0, 0).unwrap();
        simple_logger::SimpleLogger::new()
            .with_utc_offset(offset)
            .with_level(LevelFilter::Debug)
            .env()
            .init()
            .unwrap();

        let name_addr = "192.168.3.49:9876".to_string();
        let topic = "pushNoticeMessage_To".to_string();
        let consume_group = "consume_pushNoticeMessage_test_2".to_string();
        let consumer = PullConsumer::new(name_addr, consume_group, topic);

        let handle = Arc::new(Handler {});
        let lock = Arc::new(RwLock::new(true));
        let run = lock.clone();
        tokio::spawn(async move {
            consumer.start_consume(handle, run).await;
        });
        tokio::time::sleep(Duration::from_secs(60)).await;
        let mut run = lock.write().await;
        *run = false;
        tokio::time::sleep(Duration::from_secs(2)).await;
        info!("quit the test")
    }
}
