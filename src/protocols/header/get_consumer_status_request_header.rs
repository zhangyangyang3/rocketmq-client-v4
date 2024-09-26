use log::info;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::{request_code, SerializeDeserialize};
use crate::protocols::body::get_consumer_status_body::GetConsumerStatusBody;
use crate::protocols::body::message_queue::MessageQueue;

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct GetConsumerStatusRequestHeader {

    pub topic: String,
    pub group: String,
    pub clientAddr: String,
}

impl GetConsumerStatusRequestHeader {


    pub fn new(topic: String, group: String, client_addr: String) -> Self {
        GetConsumerStatusRequestHeader {
            topic,
            group,
            clientAddr: client_addr,
        }
    }

    pub async fn send_request(&mut self, broker_stream:  &mut TcpStream, queue_list: &Vec<MessageQueue>) {

        let header = self.to_bytes_1();
        let cmd = MqCommand::new_with_body(request_code::INVOKE_BROKER_TO_GET_CONSUMER_STATUS, vec![], header, vec![]);
        let write = broker_stream.write_all(&cmd.to_bytes()).await;
        if write.is_err() {
            panic!("send INVOKE_BROKER_TO_GET_CONSUMER_STATUS failed.:{:?}", write.err());
        }
        for _ in 0..5 {
            let resp_cmd = MqCommand::read_from_stream(broker_stream).await;
            info!("INVOKE_BROKER_TO_GET_CONSUMER_STATUS response:resp code:{:?}, opaque:{:?},req_opaque:{}, remark:{:?}, extend:{:?}, body:{:?}",
                 resp_cmd.req_code, resp_cmd.opaque, cmd.opaque, String::from_utf8(resp_cmd.r_body), String::from_utf8(resp_cmd.e_body), String::from_utf8(resp_cmd.body)
            );
            if resp_cmd.req_code == request_code::GET_CONSUMER_STATUS_FROM_CLIENT {
                // send response to broker
                let resp_body = GetConsumerStatusBody::new_from_queues(queue_list);
                resp_body.send_request(broker_stream, resp_cmd.opaque).await;
            }

            if resp_cmd.opaque == cmd.opaque {
                break;
            }
        }

    }

}

impl SerializeDeserialize for GetConsumerStatusRequestHeader {

}