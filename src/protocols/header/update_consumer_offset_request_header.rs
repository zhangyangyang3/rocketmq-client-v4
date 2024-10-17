use log::{debug};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::{request_code, SerializeDeserialize};

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct  UpdateConsumerOffsetRequestHeader {
    //    @CFNotNull
    //     private String consumerGroup;
    //     @CFNotNull
    //     private String topic;
    //     @CFNotNull
    //     private Integer queueId;
    //     @CFNotNull
    //     private Long commitOffset;
    pub consumerGroup: String,
    pub topic: String,
    pub queueId: i32,
    pub commitOffset: i64,

}

impl UpdateConsumerOffsetRequestHeader {
    pub fn new(consumer_group: String, topic: String, queue_id: i32, commit_offset: i64) -> Self {
        UpdateConsumerOffsetRequestHeader {
            consumerGroup: consumer_group,
            topic,
            queueId: queue_id,
            commitOffset: commit_offset,
        }
    }

    pub fn convert_from_command(cmd: &MqCommand) -> Self {
        let map = Self::bytes_1_to_map(cmd.e_body.clone());
        let consume_group = map.get("consumerGroup").unwrap();
        let topic = map.get("topic").unwrap();
        let queue_id = map.get("queueId").unwrap();
        let commit_offset = map.get("commitOffset").unwrap();
        Self {
            consumerGroup: consume_group.to_string(),
            topic: topic.to_string(),
            queueId: queue_id.parse().unwrap(),
            commitOffset: commit_offset.parse().unwrap(),
        }
    }

    pub fn command(&self) -> MqCommand {
        let body = self.to_bytes_1();
        MqCommand::new_with_body(request_code::UPDATE_CONSUMER_OFFSET, vec![], body, vec![])
    }
    pub async fn send_update_consumer_offset(&self, broker_stream: &mut TcpStream) {
        let body = self.command();
        let opaque = body.opaque;
        let body = body.to_bytes();

        let result = broker_stream.write_all(&body).await;
        if result.is_err() {
            panic!("send update consumer offset failed: {:?}", result.err());
        }
        let _ = broker_stream.flush().await;
        let cmd = MqCommand::read_from_stream_with_opaque(broker_stream, opaque).await;
        debug!("update consumer offset:{:?}, return:{}", &self, cmd.req_code);

    }
}

impl SerializeDeserialize for UpdateConsumerOffsetRequestHeader {}