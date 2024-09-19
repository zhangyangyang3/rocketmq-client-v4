use serde::Serialize;
use crate::protocols::body::subscription_data::SubscriptionData;


pub const CONSUME_TYPE_PULL: &'static str = "CONSUME_ACTIVELY";
pub const CONSUME_TYPE_PUSH: &'static str = "CONSUME_PASSIVELY";

pub const MESSAGE_MODEL_CLUSTER: &'static str = "CLUSTERING";
pub const MESSAGE_MODEL_BROADCAST: &'static str = "BROADCASTING";
//CONSUME_FROM_LAST_OFFSET,
//
//     @Deprecated
//     CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
//     @Deprecated
//     CONSUME_FROM_MIN_OFFSET,
//     @Deprecated
//     CONSUME_FROM_MAX_OFFSET,
//     CONSUME_FROM_FIRST_OFFSET,
//     CONSUME_FROM_TIMESTAMP,
pub const CONSUME_FROM_LAST_OFFSET: i32 = 0;
pub const CONSUME_FROM_FIRST_OFFSET: i32 = 4;
pub const CONSUME_FROM_TIMESTAMP: i32 = 5;
#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
pub struct ConsumerData {
    // private String groupName;
    //     private ConsumeType consumeType;
    //     private MessageModel messageModel;
    //     private ConsumeFromWhere consumeFromWhere;
    //     private Set<SubscriptionData> subscriptionDataSet = new HashSet<SubscriptionData>();
    //     private boolean unitMode;
    pub groupName: String,
    pub consumeType: String,
    pub messageModel: String,
    pub consumeFromWhere: i32,
    pub subscriptionDataSet: Vec<SubscriptionData>,
    pub unitMode: bool,
}