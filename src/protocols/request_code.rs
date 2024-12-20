pub const SEND_MESSAGE: i16 = 10;
pub const PULL_MESSAGE: i16 = 11;

pub const QUERY_MESSAGE: i16 = 12;
pub const QUERY_BROKER_OFFSET: i16 = 13;
pub const QUERY_CONSUMER_OFFSET: i16 = 14;
pub const UPDATE_CONSUMER_OFFSET: i16 = 15;
pub const UPDATE_AND_CREATE_TOPIC: i16 = 17;
pub const GET_ALL_TOPIC_CONFIG: i16 = 21;
pub const GET_TOPIC_CONFIG_LIST: i16 = 22;

pub const GET_TOPIC_NAME_LIST: i16 = 23;

pub const UPDATE_BROKER_CONFIG: i16 = 25;

pub const GET_BROKER_CONFIG: i16 = 26;

pub const TRIGGER_DELETE_FILES: i16 = 27;

pub const GET_BROKER_RUNTIME_INFO: i16 = 28;
pub const SEARCH_OFFSET_BY_TIMESTAMP: i16 = 29;
pub const GET_MAX_OFFSET: i16 = 30;
pub const GET_MIN_OFFSET: i16 = 31;

pub const GET_EARLIEST_MSG_STORE_TIME: i16 = 32;

pub const VIEW_MESSAGE_BY_ID: i16 = 33;

pub const HEART_BEAT: i16 = 34;

pub const UNREGISTER_CLIENT: i16 = 35;

pub const CONSUMER_SEND_MSG_BACK: i16 = 36;

pub const END_TRANSACTION: i16 = 37;
pub const GET_CONSUMER_LIST_BY_GROUP: i16 = 38;

pub const CHECK_TRANSACTION_STATE: i16 = 39;

pub const NOTIFY_CONSUMER_IDS_CHANGED: i16 = 40;

pub const LOCK_BATCH_MQ: i16 = 41;

pub const UNLOCK_BATCH_MQ: i16 = 42;
pub const GET_ALL_CONSUMER_OFFSET: i16 = 43;

pub const GET_ALL_DELAY_OFFSET: i16 = 45;

pub const CHECK_CLIENT_CONFIG: i16 = 46;

pub const PUT_KV_CONFIG: i16 = 100;

pub const GET_KV_CONFIG: i16 = 101;

pub const DELETE_KV_CONFIG: i16 = 102;

pub const REGISTER_BROKER: i16 = 103;

pub const UNREGISTER_BROKER: i16 = 104;
pub const GET_ROUTE_BY_TOPIC: i16 = 105;

pub const GET_BROKER_CLUSTER_INFO: i16 = 106;
pub const UPDATE_AND_CREATE_SUBSCRIPTION_GROUP: i16 = 200;
pub const GET_ALL_SUBSCRIPTION_GROUP_CONFIG: i16 = 201;
pub const GET_TOPIC_STATS_INFO: i16 = 202;
pub const GET_CONSUMER_CONNECTION_LIST: i16 = 203;
pub const GET_PRODUCER_CONNECTION_LIST: i16 = 204;
pub const WIPE_WRITE_PERM_OF_BROKER: i16 = 205;

pub const GET_ALL_TOPIC_LIST_FROM_NAMESERVER: i16 = 206;

pub const DELETE_SUBSCRIPTION_GROUP: i16 = 207;
pub const GET_CONSUME_STATS: i16 = 208;

pub const SUSPEND_CONSUMER: i16 = 209;

pub const RESUME_CONSUMER: i16 = 210;
pub const RESET_CONSUMER_OFFSET_IN_CONSUMER: i16 = 211;
pub const RESET_CONSUMER_OFFSET_IN_BROKER: i16 = 212;

pub const ADJUST_CONSUMER_THREAD_POOL: i16 = 213;

pub const WHO_CONSUME_THE_MESSAGE: i16 = 214;

pub const DELETE_TOPIC_IN_BROKER: i16 = 215;

pub const DELETE_TOPIC_IN_NAME_SRV: i16 = 216;
pub const GET_KV_LIST_BY_NAMESPACE: i16 = 219;

pub const RESET_CONSUMER_CLIENT_OFFSET: i16 = 220;

pub const GET_CONSUMER_STATUS_FROM_CLIENT: i16 = 221;

pub const INVOKE_BROKER_TO_RESET_OFFSET: i16 = 222;

pub const INVOKE_BROKER_TO_GET_CONSUMER_STATUS: i16 = 223;

pub const QUERY_TOPIC_CONSUME_BY_WHO: i16 = 300;

pub const GET_TOPICS_BY_CLUSTER: i16 = 224;

pub const REGISTER_FILTER_SERVER: i16 = 301;
pub const REGISTER_MESSAGE_FILTER_CLASS: i16 = 302;

pub const QUERY_CONSUME_TIME_SPAN: i16 = 303;

pub const GET_SYSTEM_TOPIC_LIST_FROM_NS: i16 = 304;
pub const GET_SYSTEM_TOPIC_LIST_FROM_BROKER: i16 = 305;

pub const CLEAN_EXPIRED_CONSUME_QUEUE: i16 = 306;

pub const GET_CONSUMER_RUNNING_INFO: i16 = 307;

pub const QUERY_CORRECTION_OFFSET: i16 = 308;
pub const CONSUME_MESSAGE_DIRECTLY: i16 = 309;

pub const SEND_MESSAGE_V2: i16 = 310;

pub const GET_UNIT_TOPIC_LIST: i16 = 311;

pub const GET_HAS_UNIT_SUB_TOPIC_LIST: i16 = 312;

pub const GET_HAS_UNIT_SUB_UN_UNIT_TOPIC_LIST: i16 = 313;

pub const CLONE_GROUP_OFFSET: i16 = 314;

pub const VIEW_BROKER_STATS_DATA: i16 = 315;

pub const CLEAN_UNUSED_TOPIC: i16 = 316;

pub const GET_BROKER_CONSUME_STATS: i16 = 317;

/**
 * update the config of name server
 */
pub const UPDATE_NAME_SRV_CONFIG: i16 = 318;

/**
 * get config from name server
 */
pub const GET_NAME_SRV_CONFIG: i16 = 319;

pub const SEND_BATCH_MESSAGE: i16 = 320;

pub const QUERY_CONSUME_QUEUE: i16 = 321;

pub const QUERY_DATA_VERSION: i16 = 322;
