use log::{debug, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::protocols::mq_command::HEADER_SERIALIZE_METHOD_JSON;
use crate::protocols::SerializeDeserialize;


#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct PullMessageResponseHeader {
    pub suggestWhichBrokerId: Option<i64>,
    pub nextBeginOffset: Option<i64>,
    pub minOffset: Option<i64>,
    pub maxOffset: Option<i64>,
}
impl PullMessageResponseHeader {

    pub fn convert_from_cmd() -> Self {
        return Self {
            suggestWhichBrokerId: None,
            nextBeginOffset: None,
            minOffset: None,
            maxOffset: None,
        }
    }
    pub fn bytes_to_header(serialize_method: u8, bytes: Vec<u8>)-> Option<Box<Self>>  {
        debug!("PullMessageResponseHeader, method:{}, data:{:?}",serialize_method, String::from_utf8(bytes.clone()));
        if serialize_method == HEADER_SERIALIZE_METHOD_JSON {
            Self::bates_json_to_header(bytes)
        } else {
            Self::bytes_1_to_header(bytes)
        }
    }

    fn bates_json_to_header(bytes: Vec<u8>) -> Option<Box<Self>> {
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        let swbid: Option<i64> = match json.get("suggestWhichBrokerId") {
            None => {
                None
            }
            Some(v) => {
                v.as_str().unwrap().parse().ok()
            }
        };

        let next_begin_offset: Option<i64> = match json.get("nextBeginOffset") {
            None => {
                None
            }
            Some(v) => {
                v.as_str().unwrap().parse().ok()
            }
        };

        let min_offset: Option<i64> = match json.get("minOffset") {
            None => {
                None
            }
            Some(v) => {
                v.as_str().unwrap().parse().ok()
            }
        };

        let max_offset: Option<i64> = match json.get("maxOffset") {
            None => {
                None
            }
            Some(v) => {
                v.as_str().unwrap().parse().ok()
            }
        };


        Some(Box::new(Self {
            suggestWhichBrokerId: swbid,
            nextBeginOffset: next_begin_offset,
            minOffset: min_offset,
            maxOffset: max_offset,
        }))
    }

}
impl SerializeDeserialize for PullMessageResponseHeader {
    fn bytes_1_to_header(bytes: Vec<u8>) -> Option<Box<Self>> {
        debug!("pull message response header:{:?}", String::from_utf8(bytes.clone()));
        if bytes.len() <= 0 {
            warn!("header is empty");
            return None;
        }


        let value = Self::bytes_1_to_map(bytes);
        let suggest_which_broker_id: Option<i64> = match value.get("suggestWhichBrokerId") {
            None => {
                None
            }
            Some(va) => {
                Some(va.parse().unwrap())
            }
        };

        let next_begin_offset: Option<i64> = match value.get("nextBeginOffset") {
            None => {
                None
            }
            Some(va) => {
                Some(va.parse().unwrap())
            }
        };

        let min_offset: Option<i64> = match value.get("minOffset") {
            None => {
                None
            }
            Some(va) => {
                Some(va.parse().unwrap())
            }
        };

        let max_offset: Option<i64> = match value.get("maxOffset") {
            None => {
                None
            }
            Some(va) => {
                Some(va.parse().unwrap())
            }
        };


        Some(Box::new(Self {
            suggestWhichBrokerId: suggest_which_broker_id,
            nextBeginOffset: next_begin_offset,
            minOffset: min_offset,
            maxOffset: max_offset,
        }))
    }

}