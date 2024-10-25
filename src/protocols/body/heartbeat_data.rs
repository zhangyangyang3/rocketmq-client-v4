use crate::protocols::body::consumer_data::{ConsumerData, CONSUME_TYPE_PUSH};
use crate::protocols::body::producer_data::ProducerData;
use crate::protocols::body::subscription_data::SubscriptionData;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::{request_code, SerializeDeserialize};
use log::{debug, info};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
pub struct HeartbeatData {
    pub clientID: String,
    pub producerDataSet: Vec<ProducerData>,
    pub consumerDataSet: Vec<ConsumerData>,
}

impl HeartbeatData {
    pub fn new_producer_data(client_id: String, group_name: String) -> Self {
        HeartbeatData {
            clientID: client_id,
            producerDataSet: vec![ProducerData {
                groupName: group_name,
            }],
            consumerDataSet: vec![],
        }
    }

    pub fn new_push_consumer_data(
        client_id: String,
        group_name: String,
        consume_from_where: i32,
        subscription_data: SubscriptionData,
        message_model: String,
    ) -> Self {
        HeartbeatData {
            clientID: client_id,
            producerDataSet: vec![],
            consumerDataSet: vec![ConsumerData {
                groupName: group_name,
                consumeFromWhere: consume_from_where,
                subscriptionDataSet: vec![subscription_data],
                consumeType: CONSUME_TYPE_PUSH.to_string(),
                messageModel: message_model,
                unitMode: false,
            }],
        }
    }

    pub async fn send_heartbeat(&self, broker_stream: &mut TcpStream) {
        let body = self.to_json_bytes();
        let body = MqCommand::new_with_body(request_code::HEART_BEAT, vec![], vec![], body);
        let opa = body.opaque;
        let body = body.to_bytes();
        let result = broker_stream.write_all(&body).await;
        if result.is_err() {
            panic!("send heartbeat failed: {:?}", result.err());
        }
        let _ = broker_stream.flush().await;
        let resp = MqCommand::read_from_stream_with_opaque(broker_stream, opa).await;
        debug!("send_heartbeat req opa:{}, resp opa:{}", opa, resp.opaque);
        if resp.req_code != 0 {
            info!(
                "send heartbeat resp:{}, remark:{:?},extends:{:?} ",
                resp.req_code,
                String::from_utf8(resp.r_body),
                String::from_utf8(resp.e_body)
            );
        }
    }
}

impl SerializeDeserialize for HeartbeatData {}
