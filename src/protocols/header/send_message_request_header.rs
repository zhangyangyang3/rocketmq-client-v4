use std::collections::HashMap;
use bytes::{BufMut, BytesMut};
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
    fn to_bytes_1(&self) -> Vec<u8>
    where
        Self: Serialize
    {
        let mut buf = BytesMut::with_capacity(128);
        buf.put_i16(1);
        buf.put_slice("a".as_bytes());
        buf.put_i32(self.a.len() as i32);
        buf.put_slice(self.a.as_bytes());

        buf.put_i16(1);
        buf.put_slice("b".as_bytes());
        buf.put_i32(self.b.len() as i32);
        buf.put_slice(self.b.as_bytes());

        buf.put_i16(1);
        buf.put_slice("c".as_bytes());
        buf.put_i32(self.c.len() as i32);
        buf.put_slice(self.c.as_bytes());

        buf.put_i16(1);
        buf.put_slice("d".as_bytes());
        let td = self.d.to_string();
        buf.put_i32(td.len() as i32);
        buf.put_slice(td.as_bytes());

        buf.put_i16(1);
        buf.put_slice("e".as_bytes());
        let te = self.e.to_string();
        buf.put_i32(te.len() as i32);
        buf.put_slice(te.as_bytes());

        buf.put_i16(1);
        buf.put_slice("f".as_bytes());
        let tf = self.f.to_string();
        buf.put_i32(tf.len() as i32);
        buf.put_slice(tf.as_bytes());

        buf.put_i16(1);
        buf.put_slice("g".as_bytes());
        let tg = self.g.to_string();
        buf.put_i32(tg.len() as i32);
        buf.put_slice(tg.as_bytes());

        buf.put_i16(1);
        buf.put_slice("h".as_bytes());
        let th = self.h.to_string();
        buf.put_i32(th.len() as i32);
        buf.put_slice(th.as_bytes());

        buf.put_i16(1);
        buf.put_slice("i".as_bytes());
        buf.put_i32(self.i.len() as i32);
        buf.put_slice(self.i.as_bytes());

        buf.put_i16(1);
        buf.put_slice("j".as_bytes());
        let tj = self.j.to_string();
        buf.put_i32(tj.len() as i32);
        buf.put_slice(tj.as_bytes());

        buf.put_i16(1);
        buf.put_slice("k".as_bytes());
        match self.k {
            true => {
                buf.put_i32("true".len() as i32);
                buf.put_slice("true".as_bytes());
            }
            false => {
                buf.put_i32("false".len() as i32);
                buf.put_slice("false".as_bytes());
            }
        }

        buf.put_i16(1);
        buf.put_slice("l".as_bytes());
        let tl = self.l.to_string();
        buf.put_i32(tl.len() as i32);
        buf.put_slice(tl.as_bytes());

        buf.put_i16(1);
        buf.put_slice("m".as_bytes());
        match self.m {
            true => {
                buf.put_i32("true".len() as i32);
                buf.put_slice("true".as_bytes());
            }
            false => {
                buf.put_i32("false".len() as i32);
                buf.put_slice("false".as_bytes());
            }
        }

        // debug!("bytes:{:?}", buf.to_vec());
        buf.to_vec()
    }
}
