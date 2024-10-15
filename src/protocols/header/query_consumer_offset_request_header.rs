use bytes::{Buf, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use crate::protocols::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::request_code::QUERY_CONSUMER_OFFSET;
use crate::protocols::{ConvertUtil, SerializeDeserialize};

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct QueryConsumerOffsetRequestHeader {
    pub consumerGroup: String,
    pub topic: String,
    pub queueId: i32,
}

impl SerializeDeserialize for QueryConsumerOffsetRequestHeader {}

impl QueryConsumerOffsetRequestHeader {
    pub fn new(consumer_group: String, topic: String, queue_id: i32) -> Self {
       Self {
            consumerGroup: consumer_group,
            topic,
            queueId: queue_id
        }
    }

    pub async fn send_request(&self, broker_stream: &mut TcpStream) -> i64 {
        let req_data = MqCommand::new_with_body(QUERY_CONSUMER_OFFSET, vec![],self.to_bytes_1(), vec![]);
        let write = broker_stream.write_all(&req_data.to_bytes()).await;
        if write.is_err() {
            panic!("send request failed:{:?}", write);
        }
        let _ = broker_stream.flush().await;
        let offset = QueryConsumerOffsetResponseHeader::read_from_broker(broker_stream, req_data.opaque).await;
        offset
    }

    pub fn convert_from_cmd(cmd: &MqCommand) -> Self {
        let body = &cmd.body;
        let mut body = Bytes::copy_from_slice(body);

        let consumer_group_len = body.get_i16();
        let _ = body.copy_to_bytes(consumer_group_len as usize);
        let consumer_group_v_len = body.get_i32();
        let consumer_group_body = body.copy_to_bytes(consumer_group_v_len as usize);

        let topic_key_len = body.get_i16();
        let _ = body.copy_to_bytes(topic_key_len as usize);
        let topic_value_len = body.get_i32();
        let topic_body = body.copy_to_bytes(topic_value_len as usize);

        let queue_id_key_len = body.get_i16();
        let _ =body.copy_to_bytes(queue_id_key_len as usize);
        let queue_id_value_len = body.get_i32();
        let queue_id_body = body.copy_to_bytes(queue_id_value_len as usize);
        let queue_id = ConvertUtil::convert_string_bytes_to_i32(queue_id_body.to_vec());

        Self {
            consumerGroup: String::from_utf8(consumer_group_body.to_vec()).unwrap(),
            topic: String::from_utf8(topic_body.to_vec()).unwrap(),
            queueId: queue_id
        }

    }

}