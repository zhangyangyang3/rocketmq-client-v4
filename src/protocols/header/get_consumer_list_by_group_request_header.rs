use log::{warn};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::{request_code, response_code, SerializeDeserialize};
use crate::protocols::body::get_consumer_list_by_group_response_body::GetConsumerListByGroupResponseBody;

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
pub struct GetConsumerListByGroupRequestHeader {

    pub consumerGroup: String,
}

impl GetConsumerListByGroupRequestHeader {
    pub fn new(consumer_group: String) -> Self {
        GetConsumerListByGroupRequestHeader {
            consumerGroup: consumer_group
        }
    }

    pub async fn query_consumer_list_by_group(&self, stream: &mut TcpStream) -> Vec<String> {

        let req_body = MqCommand::new_with_body(request_code::GET_CONSUMER_LIST_BY_GROUP, vec![], self.to_bytes_1(), vec![]);
        let req_opa = req_body.opaque;
        let req = stream.write_all(&req_body.to_bytes()).await;
        if req.is_err() {
            panic!("query_consumer_list_by_group:{:?}", req.err());
        }
        let _ = stream.flush().await;
        let cmd = MqCommand::read_from_stream_with_opaque(stream, req_opa).await;

        match cmd.req_code {
            response_code::SUCCESS => {
                let body = GetConsumerListByGroupResponseBody::from_vec(&cmd.body);
                match body.consumerIdList  {
                    None => {vec![]}
                    Some(list) => {
                        list
                    }
                }
            }
            _ => {
                warn!("query_consumer_list_by_group failed. ret code:{:?}, remark:{:?}", cmd.req_code, String::from_utf8(cmd.r_body));
                vec![]
            }
        }
    }
}

impl SerializeDeserialize for GetConsumerListByGroupRequestHeader {

}