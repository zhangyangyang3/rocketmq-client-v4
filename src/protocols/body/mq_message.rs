use crate::protocols::header::send_message_request_header::SendMessageRequestHeader;
use bytes::{BufMut, BytesMut};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug)]
#[allow(non_snake_case)]
pub struct MqMessage {
    pub topic: String,
    pub flag: i32,
    pub properties: HashMap<String, String>,
    pub body: Vec<u8>,
    pub transactionId: String,
}

impl MqMessage {
    pub fn new(topic: String, body: Vec<u8>) -> Self {
        MqMessage {
            topic,
            flag: 0,
            properties: HashMap::new(),
            body,
            transactionId: Uuid::new_v4().to_string(),
        }
    }

    pub fn set_properties(&mut self, key: String, value: String) {
        self.properties.insert(key, value);
    }

    pub fn set_keys(&mut self, value: String) {
        self.set_properties("KEYS".to_string(), value);
    }

    pub fn set_tags(&mut self, value: String) {
        self.set_properties("TAGS".to_string(), value);
    }

    pub fn encode_message(&self) -> Vec<u8> {
        let body_len = self.body.len();
        let header_str = SendMessageRequestHeader::convert_map_to_string(&self.properties);
        let header_len = header_str.len();
        let len = 4 + 4 + 4 + 4 + 4 + body_len + 2 + header_len;
        let mut bytes = BytesMut::with_capacity(len);
        // 1 TOTALSIZE
        bytes.put_i32(len as i32);

        // 2 MAGICCODE
        bytes.put_i32(0);
        // 3 BODYCRC
        bytes.put_i32(0);
        // 4 FLAG
        bytes.put_i32(self.flag);
        // 5 BODY
        bytes.put_i32(body_len as i32);
        bytes.put_slice(&self.body);
        // 6 properties
        bytes.put_i16(header_len as i16);
        bytes.put_slice(header_str.as_bytes());
        bytes.to_vec()
    }
}
