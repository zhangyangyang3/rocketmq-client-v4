use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use crate::protocols::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::request_code::QUERY_CONSUMER_OFFSET;
use crate::protocols::SerializeDeserialize;

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

}