use std::collections::{HashMap, HashSet};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use crate::protocols::body::broker_data::BrokerData;
use crate::protocols::mq_command::MqCommand;
use crate::protocols::request_code::GET_BROKER_CLUSTER_INFO;
use crate::protocols::{fixed_un_standard_json, response_code};


#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct ClusterInfo {
    pub brokerAddrTable: HashMap<String, BrokerData>,
    pub clusterAddrTable: HashMap<String, HashSet<String>>,
}

impl ClusterInfo {
    pub async fn get_cluster_info(name_server: &mut TcpStream) -> ClusterInfo {

        let broker_conf = MqCommand::new_with_body(GET_BROKER_CLUSTER_INFO, vec![], vec![], vec![]);
        let opa = broker_conf.opaque;
        let broker_conf = broker_conf.to_bytes();

        let req = name_server.write_all(&broker_conf).await;
        if req.is_err() {
            panic!("request route info failed:{:?}", req);
        }
        let req = name_server.flush().await;
        if req.is_err() {
            panic!("request route info failed:{:?}", req);
        }

        let cmd = MqCommand::read_from_stream_with_opaque(name_server, opa).await;

        debug!("get_cluster_info req opa:{}, resp opa{}", opa, cmd.opaque);
        return match cmd.req_code {
            response_code::SUCCESS => {
                let body = fixed_un_standard_json(&cmd.body);
                let cluster_info: ClusterInfo = serde_json::from_slice(&body).unwrap();
                info!("cluster info:{:?}", cluster_info);
                cluster_info
            }
            _ => {
                panic!("get route info failed:{:?}", cmd);
            }
        }

    }
}