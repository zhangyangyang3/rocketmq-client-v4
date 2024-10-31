use serde::Serialize;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::{request_code, SerializeDeserialize};

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
pub struct GetMaxOffsetRequestHeader {
    pub topic: String,
    pub queueId: i32,
}

impl GetMaxOffsetRequestHeader {
    pub fn new(topic: String, queue_id: i32) -> Self {
        Self {
            topic,
            queueId: queue_id,
        }
    }

    pub fn convert_from_cmd(cmd: &MqCommand) -> Self {
        let header = cmd.e_body.clone();
        let map = Self::bytes_1_to_map(header);
        Self {
            topic: map.get("topic").unwrap().to_string(),
            queueId: map.get("queueId").unwrap().parse().unwrap(),
        }
    }
    pub fn to_cmd(&self) -> MqCommand {

        let header = self.to_bytes_1();
        let cmd = MqCommand::new_with_body(request_code::GET_MAX_OFFSET, vec![], header, vec![]);
        cmd
    }
}

impl SerializeDeserialize for  GetMaxOffsetRequestHeader {

}