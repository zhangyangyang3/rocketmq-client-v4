use serde_json::Value;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::{mq_command, SerializeDeserialize};

#[derive(Debug)]
#[allow(non_snake_case)]
pub struct GetConsumerRunningInfoRequestHeader {
    pub consumerGroup: String,
    pub clientId: String,
    pub jstackEnable: Option<bool>
}


impl GetConsumerRunningInfoRequestHeader {
    pub fn convert_from_command(cmd: &MqCommand) -> Self {

        match cmd.header_serialize_method {
            mq_command::HEADER_SERIALIZE_METHOD_PRIVATE=> {
                let map = Self::bytes_1_to_map(cmd.e_body.clone());

                Self {
                    consumerGroup: map.get("consumerGroup").unwrap().to_string(),
                    clientId: map.get("clientId").unwrap().to_string(),
                    jstackEnable: None,
                }
            }
            _ => {
                let json = serde_json::from_slice::<Value>(&cmd.e_body).unwrap();
                let consumer_group = json.get("consumerGroup").unwrap().as_str().unwrap().to_string();
                let client_id = json.get("clientId").unwrap().as_str().unwrap().to_string();
                let j_stack = match json.get("jstackEnable") {
                    Some(v) => Some(v.as_str().unwrap() == "true"),
                    None => None
                };
                Self {
                    consumerGroup: consumer_group,
                    clientId: client_id,
                    jstackEnable: j_stack
                }
            }
        }

    }
}

impl SerializeDeserialize for GetConsumerRunningInfoRequestHeader {

}