use bytes::{Buf, Bytes};
use crate::protocols::mq_command::MqCommand;

pub struct NotifyConsumerIdsChangedRequestHeader {
    pub consumer_group: String
}

impl NotifyConsumerIdsChangedRequestHeader {
    pub fn convert_from_cmd(cmd: &MqCommand) -> Self {
        let body = &cmd.body;
        let mut body = Bytes::copy_from_slice(body);

        let consumer_group_len = body.get_i16();
        let _ = body.copy_to_bytes(consumer_group_len as usize);
        let consumer_group_v_len = body.get_i32();
        let consumer_group_body = body.copy_to_bytes(consumer_group_v_len as usize);
        Self {
            consumer_group: String::from_utf8(consumer_group_body.to_vec()).unwrap()
        }
    }

}