use std::collections::HashMap;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::debug;
use serde::{Serialize};
use serde_json::Value;

pub mod body;
pub mod header;

pub mod request_code;
pub mod response_code;

pub mod mq_command;

pub trait SerializeDeserialize {
    fn  to_bytes_1(&self) -> Vec<u8>
    where
        Self: Serialize,
    {
        let value = serde_json::to_value(&self).unwrap();
        match value {
            Value::Object(map) => {
                let mut buf = BytesMut::with_capacity(128);
                debug!("header map:{:?}", &map);
                for (k, v) in map {

                    let v = if v.is_string() {
                        v.as_str().unwrap().to_string()
                    } else if v.is_number() {
                        v.as_number().unwrap().to_string()
                    } else if v.is_boolean() {
                        match v.as_bool().unwrap() {
                            true => "true".to_string(),
                            false => "false".to_string(),
                        }
                    } else if v.is_null() {
                        continue;
                    }else {
                        panic!("not support type: key:{}, v:{}", k, v);
                    };
                    buf.put_i16(k.len() as i16);
                    buf.put_slice(k.as_bytes());
                    buf.put_i32(v.len() as i32);
                    buf.put_slice(v.as_bytes());
                }
                buf.to_vec()
            }
            _ => {
                vec![]
            }
        }
    }


    fn bytes_1_to_header(_bytes: Vec<u8>) -> Option<Box<Self>> {
        None
    }

    fn bytes_1_to_map(bytes: Vec<u8>) -> HashMap<String,String> {
        let mut bytes = Bytes::from(bytes);
        let mut value = HashMap::new();
        while bytes.remaining() > 0 {
            let key1_len = bytes.get_i16();
            let key1 = bytes.copy_to_bytes(key1_len as usize);

            let v1_len = bytes.get_i32();
            let v1 = bytes.copy_to_bytes(v1_len as usize).to_vec();
            value.insert(String::from_utf8(key1.to_vec()).unwrap(), String::from_utf8(v1.to_vec()).unwrap());
        }
        value
    }

    fn to_json_bytes(&self) -> Vec<u8> where Self: Serialize {
        serde_json::to_vec(self).unwrap()
    }
}

pub fn fixed_un_standard_json(src: &Vec<u8>) -> Vec<u8> {
    let lcub = "{".as_bytes()[0]; // 123
    let rcub = "}".as_bytes()[0]; // 125

    let comma = ",".as_bytes()[0];

    let quot = "\"".as_bytes()[0]; // 34

    let colon = ":".as_bytes()[0];

    let bsol = 92u8; // \

    let mut dest: Vec<u8> = vec![];
    let mut quot_count = 0;
    for i in 0..src.len() {
        if src[i] == quot {
            if i > 0 && src[i - 1] != bsol {
                quot_count = quot_count + 1;
            }
        }
        // start { ,

        if quot_count % 2 == 0 && (src[i] == lcub || src[i] == comma) {
            if src[i + 1] != quot && src[i + 1] != rcub {
                dest.push(src[i]);
                dest.push(quot);
                continue;
            }
        }

        if quot_count % 2 == 0 && src[i] == colon {
            if src[i - 1] != quot {
                dest.push(quot);
                dest.push(src[i]);
                continue;
            }
        }
        dest.push(src[i]);
    }
    dest
}


#[allow(dead_code)]
const PERM_PRIORITY: i32 = 0x1 << 3;
const PERM_READ: i32 = 0x1 << 2;
const PERM_WRITE: i32 = 0x1 << 1;

const PERM_INHERIT: i32 = 0x1 << 0;

pub struct PermName {}

impl PermName {
    pub fn perm_to_string(perm: i32) -> String {
        let mut perm_str = String::from("");
        if Self::is_readable(perm) {
            perm_str.push_str("R");
        } else {
            perm_str.push_str("-");
        }

        if Self::is_writeable(perm) {
            perm_str.push_str("W");
        } else {
            perm_str.push_str("-");
        }

        if Self::is_inherited(perm) {
            perm_str.push_str("X");
        } else {
            perm_str.push_str("-");
        }
        return perm_str;
    }

    pub fn is_readable(perm: i32) -> bool {
        return (perm & PERM_READ) == PERM_READ;
    }

    pub fn is_writeable(perm: i32) -> bool {
        return (perm & PERM_WRITE) == PERM_WRITE;
    }

    pub fn is_inherited(perm: i32) -> bool {
        return (perm & PERM_INHERIT) == PERM_INHERIT;
    }
}

pub struct ConvertUtil;

impl ConvertUtil {
    pub fn convert_string_bytes_to_i64(bytes: Vec<u8>) -> i64 {
        let str = String::from_utf8(bytes).unwrap();
        let ret: i64 =  str.parse().unwrap();
        ret
    }

    pub fn convert_string_bytes_to_i32(bytes: Vec<u8>) -> i32 {
        let str = String::from_utf8(bytes).unwrap();
        let ret: i32 =  str.parse().unwrap();
        ret
    }
}

pub fn get_current_time_millis() -> i64 {
    let now = std::time::SystemTime::now();
    let duration = now.duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap();
    return duration.as_millis() as i64;
}

pub async fn sleep(millis: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
}

#[cfg(test)]
mod test {
    use crate::protocols::fixed_un_standard_json;

    #[test]
    fn calc_ascii() {
        let data = r#"{"broke\"rAdd\trTable":{"xd":{"brokerAddrs":{0:"192.168.3.49:10911"},"brokerName":"xd","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["xd"]}}"#;
        let data = data.as_bytes();
        let mut src = vec![];
        for i in 0..data.len() {
            src.push(data[i]);
        }
        let dest = fixed_un_standard_json(&src);
        println!("dest data:{:?}", String::from_utf8(dest).unwrap());
    }
}