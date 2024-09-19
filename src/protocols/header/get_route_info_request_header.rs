use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use crate::protocols::body::topic_route_data::TopicRouteData;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::request_code::GET_ROUTE_BY_TOPIC;
use crate::protocols::{fixed_un_standard_json, response_code, SerializeDeserialize};


#[derive(Serialize, Deserialize)]
pub struct GetRouteInfoRequestHeader {
    pub topic: String,
}

impl GetRouteInfoRequestHeader {

    pub fn get_route_info_request(topic: &str) -> Self {
        Self { topic: topic.to_string() }
    }

    pub async fn get_topic_route_data(&self, name_server: &mut TcpStream) -> Option<TopicRouteData> {
        let header = self.to_bytes_1();
        let bytes = MqCommand::new_with_body(GET_ROUTE_BY_TOPIC, vec![], header, vec![]);
        let opa = bytes.opaque;
        let bytes = bytes.to_bytes();
        let req = name_server.write_all(&bytes).await;
        if req.is_err() {
            warn!("get_topic_route_data failed{:?}", req);
            return None;
        }
        let _ = name_server.flush().await;

        let cmd = MqCommand::read_from_stream_with_opaque(name_server, opa).await ;


            debug!("get_topic_route_data req opa:{}, resp opa{}",opa , cmd.opaque);
        return match cmd.req_code {
                response_code::SUCCESS => {
                    debug!("before fixed:{:?}", String::from_utf8(cmd.body.clone()).unwrap());
                    let body = fixed_un_standard_json(&cmd.body);
                    debug!("after fixed:{:?}", String::from_utf8(body.clone()).unwrap());
                    let data: TopicRouteData = serde_json::from_slice(&body).unwrap();
                    info!("topic route info:{:?}", data);
                    return Some(data);
                }
                _ => {
                    warn!("invalid response code:{}, {}", cmd.req_code, String::from_utf8(cmd.r_body).unwrap());
                    None
                }
            }

    }
}

impl SerializeDeserialize for GetRouteInfoRequestHeader {

}

#[cfg(test)]
mod test {
    use crate::protocols::header::get_route_info_request_header::GetRouteInfoRequestHeader;
    use crate::protocols::SerializeDeserialize;

    #[test]
    fn test_serialize() {
        let header = GetRouteInfoRequestHeader { topic: "topic111".to_string() };
        let bytes = header.to_bytes_1();
        println!("bytes:{:?}", bytes);
    }
}