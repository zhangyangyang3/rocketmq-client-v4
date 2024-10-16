use bytes::{Buf, Bytes};
use log::debug;
use serde::Deserialize;
use crate::protocols::mq_command::{HEADER_SERIALIZE_METHOD_PRIVATE, MqCommand};

#[derive(Deserialize)]
#[allow(non_snake_case)]
pub struct NotifyConsumerIdsChangedRequestHeader {
    pub consumerGroup: String
}

impl NotifyConsumerIdsChangedRequestHeader {
    pub fn convert_from_cmd(cmd: &MqCommand) -> Self {
        let body = &cmd.e_body;
        let mut body = Bytes::copy_from_slice(body);
        debug!("NotifyConsumerIdsChangedRequestHeader, extend:{:?}",  String::from_utf8(cmd.e_body.clone()));
        match cmd.header_serialize_method {
            HEADER_SERIALIZE_METHOD_PRIVATE => {
                let consumer_group_len = body.get_i16();
                let _ = body.copy_to_bytes(consumer_group_len as usize);
                let consumer_group_v_len = body.get_i32();
                let consumer_group_body = body.copy_to_bytes(consumer_group_v_len as usize);
                return Self {
                    consumerGroup: String::from_utf8(consumer_group_body.to_vec()).unwrap()
                }
            }
            _ => {
                let json = serde_json::from_slice::<NotifyConsumerIdsChangedRequestHeader>(&body.to_vec()).unwrap();
                return json;
            }
        }
    }

}