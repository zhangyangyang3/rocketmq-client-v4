use std::sync::Arc;
use std::time::Duration;
use log::{info, LevelFilter};
use time::UtcOffset;
use tokio::sync::RwLock;
use rocketmq_client_v4::consumer::message_handler::MessageHandler;
use rocketmq_client_v4::consumer::pull_consumer_v2::PullConsumer;
use rocketmq_client_v4::protocols::body::message_body::MessageBody;


struct Handler {}
impl MessageHandler for Handler {
    async fn handle(&self, message: &MessageBody) {
        info!("read message:{:?}", String::from_utf8(message.body.clone()))
    }
}

unsafe impl Send for Handler {}

unsafe impl Sync for Handler {}
#[tokio::main]
pub async fn main() {
    let offset = UtcOffset::from_hms(8, 0, 0).unwrap();
    simple_logger::SimpleLogger::new()
        .with_utc_offset(offset)
        .with_level(LevelFilter::Debug)
        .env()
        .init()
        .unwrap();

    let name_addr = "192.168.3.49:9876".to_string();

    let topic = "MessageCluster_To".to_string();
    let consume_group = "Message_messageClusterInput_group".to_string();
    let consumer = PullConsumer::new_broadcast_consumer(name_addr.clone(), consume_group, topic);


    let handle = Arc::new(Handler {});
    let lock = Arc::new(RwLock::new(true));
    let run = lock.clone();

    tokio::spawn(async move {
        consumer.start_consume(handle.clone(), run.clone()).await;
    });
    tokio::time::sleep(Duration::from_secs(180)).await;
    {
        let mut run = lock.write().await;
        *run = false;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("quit the test")
}

