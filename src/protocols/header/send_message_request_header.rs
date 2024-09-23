use std::collections::HashMap;
use serde::Serialize;
use crate::protocols::{get_current_time_millis, SerializeDeserialize};

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
pub struct SendMessageRequestHeader {
    pub producerGroup: String,
    pub topic: String,
    pub defaultTopic: String,
    pub defaultTopicQueueNums: i32,
    pub queueId: i32,
    pub sysFlag: i32,
    pub bornTimestamp: i64,
    pub flag: i32,
    pub properties: String,
    pub reconsumeTimes: i32,
    pub unitMode: bool,
    pub batch: bool,
    pub maxReconsumeTimes: i32,
}


impl SendMessageRequestHeader {
    pub fn new(producer_group: String, topic: String, queue_id: i32, properties: &HashMap<String, String>) -> Self {
        SendMessageRequestHeader {
            producerGroup: producer_group,
            topic,
            defaultTopic: "TBW102".to_string(),
            defaultTopicQueueNums: 1000,
            queueId: queue_id,
            sysFlag: 0,
            bornTimestamp: get_current_time_millis(),
            flag: 0,
            properties: Self::convert_map_to_string(properties),
            reconsumeTimes: 0,
            unitMode: false,
            batch: false,
            maxReconsumeTimes: 0,
        }
    }

    pub fn convert_map_to_string(map: &HashMap<String, String>) -> String {
        let mut ret = String::new();
        for (k, v) in map {
            ret.push_str(k);
            ret.push_str("1");
            ret.push_str(v);
            ret.push_str("2");
        }
        if ret.len() > 0 {
            ret.remove(ret.len()-1);
        }
        ret
    }
}

impl SerializeDeserialize for SendMessageRequestHeader {
}


#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
pub struct SendMessageRequestHeaderV2 {
    pub a: String, // producerGroup
    pub b: String, //topic
    pub c: String, // defaultTopic
    pub d: i32, // defaultTopicQueueNums
    pub e: i32, // queueId
    pub f: i32, // sysFlag
    pub g: i64, // bornTimestamp
    pub h: i32, // flag
    pub i: String, // properties
    pub j: i32, //reconsumeTimes
    pub k: bool, //unitMode
    pub l: i32, //consumeRetryTimes
    pub m: bool, // batch
}

impl SendMessageRequestHeaderV2 {
    pub fn new(header: SendMessageRequestHeader) -> Self {
        Self {
            a: header.producerGroup,
            b: header.topic,
            c: header.defaultTopic,
            d: header.defaultTopicQueueNums,
            e: header.queueId,
            f: header.sysFlag,
            g: header.bornTimestamp,
            h: header.flag,
            i: header.properties,
            j: header.reconsumeTimes,
            k: header.unitMode,
            l: header.maxReconsumeTimes,
            m: header.batch,
        }
    }
}

impl SerializeDeserialize for SendMessageRequestHeaderV2 {
}
