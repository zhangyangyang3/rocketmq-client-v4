use serde::Deserialize;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::{response_code, SerializeDeserialize};

#[derive(Debug, Deserialize)]
pub struct GetMaxOffsetResponseHeader {
    pub offset: i64
}

impl GetMaxOffsetResponseHeader {

    pub fn convert_from_cmd(cmd: &MqCommand) -> Self {

        match cmd.req_code {

            response_code::SUCCESS => {
                let map = Self::bytes_1_to_map(cmd.e_body.clone());
                let v = map.get("offset").unwrap().to_string();
                Self {
                    offset: v.parse().unwrap(),
                }
            }

            _ => {
                Self{
                    offset: -1,
                }
            }
        }

    }

}


impl SerializeDeserialize for GetMaxOffsetResponseHeader {}