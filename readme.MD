
# desc

This is a rust rocket mq client for rocket mq version 4.  
if you use rocket mq 5. you should use [rocketmq-client-rs](https://crates.io/crates/rocketmq)

# how to use

## consumer

```rust

use log::{info, LevelFilter};
use rocketmq_client_v4::consumer::message_handler::MessageHandler;
use rocketmq_client_v4::consumer::pull_consumer::MqConsumer;
use rocketmq_client_v4::protocols::body::message_body::MessageBody;
use std::sync::Arc;
use std::time::Duration;
use time::UtcOffset;
use tokio::sync::RwLock;
use rocketmq_client_v4::consumer::pull_consumer_v2::PullConsumer;

struct Handler {}
impl MessageHandler for Handler {
    async fn handle(&self, message: &MessageBody) {
        info!("read message:{:?}", String::from_utf8(message.body.clone()))
    }
}

unsafe impl Send for Handler {
}

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
    let topic = "pushNoticeMessage_To".to_string();
    let consume_group = "consume_pushNoticeMessage_test_2".to_string();
    let consumer = PullConsumer::new(name_addr, consume_group, topic);

    let handle = Arc::new(Handler {});
    let lock = Arc::new(RwLock::new(true));
    let run = lock.clone();
    tokio::spawn(async move {
        consumer.start_consume(handle, run).await;
    });
    tokio::time::sleep(Duration::from_secs(40)).await;
    let mut run = lock.write().await;
    *run = false;
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("quit the test")
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


# todo
consume broadcast message

# connect me
zyy20101289@outlook.com
