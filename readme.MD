
# desc

This is a rust rocket mq client for rocket mq version 4.  
if you use rocket mq 5. you should use [rocketmq-client-rs](https://crates.io/crates/rocketmq)

# how to use

## consumer

```rust
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use log::{info, LevelFilter};
use time::UtcOffset;
use tokio::sync::RwLock;
use rocketmq_client_v4::connection::MqConnection;
use rocketmq_client_v4::consumer::message_handler::MessageHandler;
use rocketmq_client_v4::consumer::pull_consumer::MqConsumer;
use rocketmq_client_v4::protocols::body::message_body::MessageBody;

struct Handler {}
impl MessageHandler for Handler {
    async fn handle(&self, message: &MessageBody) {
        info!("read message:{:?}", String::from_utf8(message.body.clone()))
    }
}
#[tokio::main]
pub async fn main() {
    let offset = UtcOffset::from_hms(8, 0, 0).unwrap();
    simple_logger::SimpleLogger::new().with_utc_offset(offset).with_level(LevelFilter::Debug).env().init().unwrap();

    let name_addr = "192.168.3.49:9876".to_string();
    let topic = "pushNoticeMessage_To".to_string();

    let cluster = MqConnection::get_cluster_info(&name_addr).await;
    let (tcp_stream, id) = MqConnection::get_broker_tcp_stream(&cluster).await;

    info!("tcp stream:{:?}, id:{}", tcp_stream, id);
    let mut consumer = MqConsumer::new_consumer(name_addr, "test1_group".to_string(), topic, tcp_stream, id);
    let lock = Arc::new(RwLock::new(true));
    let lock1 = lock.clone();
    let handle = Arc::new(Handler {});
    tokio::spawn(async move {consumer.pull_message(handle, lock).await;});
    tokio::time::sleep(Duration::from_secs(60)).await;
    *lock1.write().await = false;

}

```

## producer

```rust
#[tokio::test]
    async fn send_message_test() {

        let offset = UtcOffset::from_hms(8, 0, 0).unwrap();
        simple_logger::SimpleLogger::new().with_utc_offset(offset).with_level(LevelFilter::Debug).env().init().unwrap();

        let message_body = r#"{"id":"3910000000000056508"}"#;
        let body = message_body.as_bytes().to_vec();

        let name_addr = "192.168.3.49:9876".to_string();
        let topic = "topic_test_007".to_string();

        let mut producer = Producer::new("rust_send_group_1".to_string(), name_addr.clone()).await;
        for i in 0..10 {
            producer.send_message(topic.clone(), body.clone(), format!("{i}")).await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }


```

# attention*
<b>
for now. it only supports cluster mode consume message. 
</b>

<b>
for now. producer is ok. but KEYS and TAGS not supported.
</b>



# todo
consume broadcast message

# connect me
zyy20101289@outlook.com
