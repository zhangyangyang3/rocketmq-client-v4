use crate::protocols::mq_command::MqCommand;
use crate::protocols::{mq_command, ConvertUtil};
use bytes::{Buf, Bytes};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryConsumerOffsetResponseHeader {
    pub offset: i64,
}

impl QueryConsumerOffsetResponseHeader {
    pub fn convert_from_command(cmd: MqCommand) -> Option<Self> {
        return match cmd.header_serialize_method {
            mq_command::HEADER_SERIALIZE_METHOD_JSON => {
                info!(
                    "QueryConsumerOffsetResponseHeader e_body:{:?}",
                    String::from_utf8(cmd.e_body)
                );
                None
            }
            _ => {
                let mut bytes = Bytes::from(cmd.e_body);
                let key_len = bytes.get_i16();
                let _ = bytes.copy_to_bytes(key_len as usize);
                let body_len = bytes.get_i32();
                let body = bytes.copy_to_bytes(body_len as usize).to_vec();
                let offset = ConvertUtil::convert_string_bytes_to_i64(body);
                return Some(QueryConsumerOffsetResponseHeader { offset });
            }
        };
    }
    pub async fn read_from_broker(broker_stream: &mut TcpStream, opaque: i32) -> i64 {
        let frame = MqCommand::read_from_stream_with_opaque(broker_stream, opaque).await;

        if frame.e_len == 0 {
            warn!(
                "read from QueryConsumerOffsetResponseHeader failed, code:{}, r_body:{:?}",
                frame.req_code,
                String::from_utf8(frame.r_body)
            );
            return 0;
        }

        match Self::convert_from_command(frame) {
            None => {
                return 0;
            }
            Some(header) => header.offset,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::protocols::ConvertUtil;

    #[test]
    fn i64_test() {
        let v = vec![48];
        let i = ConvertUtil::convert_string_bytes_to_i64(v);
        println!("data:{:?}", i);
    }
}
