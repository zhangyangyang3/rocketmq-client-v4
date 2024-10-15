use log::{warn};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::{request_code, SerializeDeserialize};


const MAX_MSG_NUMS: i32 = 128;
const SYS_FLAG: i32 = 2;

const SUSPEND_TIMEOUT_MILLIS : i64 = 1 * 1000;


#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct PullMessageRequestHeader {
    pub consumerGroup: String,
    pub topic: String,
    pub queueId: i32,
    pub queueOffset: i64,
    pub maxMsgNums: i32,
    pub sysFlag: i32,
    pub commitOffset: i64,
    pub suspendTimeoutMillis: i64,
    pub subscription: Option<String>,
    pub subVersion: i64,
    pub expressionType: Option<String>
}

impl SerializeDeserialize for PullMessageRequestHeader {

}
impl PullMessageRequestHeader {
    pub fn new(consumer_group: String, topic: String, queue_id: i32, queue_offset: i64, commit_offset: i64) -> Self {
        PullMessageRequestHeader {
            consumerGroup: consumer_group,
            topic,
            queueId: queue_id,
            queueOffset: queue_offset,
            maxMsgNums: MAX_MSG_NUMS,
            sysFlag: SYS_FLAG,
            commitOffset: commit_offset,
            suspendTimeoutMillis: SUSPEND_TIMEOUT_MILLIS,
            subscription: Some("*".to_string()),
            subVersion: 0,
            expressionType: Some("TAG".to_string()),
        }
    }

    pub fn to_command(&self) -> MqCommand {
        let header_body = self.to_bytes_1();
        let req = MqCommand::new_with_body(request_code::PULL_MESSAGE, vec![], header_body, vec![]);
        return req;
    }
    pub async fn send_request(&self, broker_stream: &mut TcpStream) -> Option<MqCommand> {
        let req = self.to_command();
        let opaque = req.opaque;
        let req = req.to_bytes();

        let req_result = broker_stream.write_all(&req).await;
        if req_result.is_err() {
            warn!("send request failed, error: {:?}", req_result)
        }
        let _ = broker_stream.flush().await;
        let cmd = MqCommand::read_from_stream_with_opaque(broker_stream, opaque).await;

        Some(cmd)

    }
}