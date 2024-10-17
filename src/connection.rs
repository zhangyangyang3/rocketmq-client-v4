use local_ip_address::local_ip;
use log::{debug, info};
use tokio::net::TcpStream;
use crate::protocols::body::cluster_info::ClusterInfo;
use crate::protocols::body::topic_route_data::TopicRouteData;
use crate::protocols::get_current_time_millis;
use crate::protocols::header::get_route_info_request_header::GetRouteInfoRequestHeader;

pub fn get_client_ip() -> String {

    let my_local_ip = local_ip();

    if let Ok(my_local_ip) = my_local_ip {
        let ip = my_local_ip.to_string();
        info!("client ip addr:{}", ip);
        ip
    } else {
        panic!("Error getting local IP: {:?}", my_local_ip);
    }
}

pub struct MqConnection {}

impl MqConnection {
    pub async fn get_cluster_info(address: &str) -> ClusterInfo {
        info!("connecting to rocketmq server:{}", address);
        let name_server_socket = TcpStream::connect(address).await;
        if name_server_socket.is_err() {
            panic!("connection to mq failed {:?}", name_server_socket);
        }
        let mut name_server_stream = name_server_socket.unwrap();
        ClusterInfo::get_cluster_info(&mut name_server_stream).await
    }

    pub async fn get_broker_tcp_stream(cluster_info: &ClusterInfo) ->(TcpStream, i64) {
        let mut broker_list = vec![];
        for (_,v) in &cluster_info.brokerAddrTable {
            broker_list.push(v);
        }




        let idx = get_current_time_millis() as usize % broker_list.len();
        let broker = broker_list[idx];
        for ( id,addr) in &broker.brokerAddrs {
            info!("try to connect to {}", addr);
            let tcp_stream = TcpStream::connect(addr).await;
            if tcp_stream.is_ok() {
                return (tcp_stream.unwrap(), id.parse().unwrap());
            }
        }
        panic!("connect to mq broker failed");
    }

    pub async fn get_all_master_broker_stream(cluster_info: &ClusterInfo) -> Vec<TcpStream> {
        let mut broker_list = vec![];

        for (_,v) in &cluster_info.brokerAddrTable {
            for ( id,addr) in &v.brokerAddrs {
                if id != "0" {
                    continue;
                }
                info!("try to connect to id:{}, addr {}", id, addr);
                let tcp_stream = TcpStream::connect(addr).await;
                if tcp_stream.is_ok() {
                    broker_list.push(tcp_stream.unwrap());
                }
            }
        }
        broker_list
    }

    pub async fn get_one_master_broker_stream(cluster_info: &ClusterInfo) -> TcpStream {
        let mut broker_list = MqConnection::get_all_master_broker_stream(cluster_info).await;
        return broker_list.pop().unwrap();
    }

    pub async fn get_topic_route_data(address: &str, topic: &str) -> Option<TopicRouteData> {
        let name_server_socket = TcpStream::connect(address).await;
        if name_server_socket.is_err() {
            panic!("connection to mq failed {:?}", name_server_socket);
        }
        let mut name_server_stream = name_server_socket.unwrap();
        debug!("stream:{:?}", name_server_stream);

        let req_data = GetRouteInfoRequestHeader::get_route_info_request(topic);
        req_data.get_topic_route_data(&mut name_server_stream).await
    }

}


#[cfg(test)]
mod test {
    use log::{info};
    use crate::connection::{get_client_ip, MqConnection};

    #[test]
    fn client_ip_test() {
        let ip = get_client_ip();
        println!("ip:{}", ip);
    }

    fn init_logger() {
    }

    #[tokio::test]
    async fn connection_test() {
        init_logger();
        MqConnection::get_cluster_info("192.168.3.49:9876".into()).await;
    }

    #[tokio::test]
    async fn get_broker_conn() {
        init_logger();
        let cluster = MqConnection::get_cluster_info("192.168.3.49:9876".into()).await;
        let (tcp_stream, id) = MqConnection::get_broker_tcp_stream(&cluster).await;
        info!("tcp stream:{:?}, id:{:?}", tcp_stream, id)
    }
}