use log::{info, warn};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct GetConsumerListByGroupResponseBody {
    pub consumerIdList: Option<Vec<String>>
}

impl GetConsumerListByGroupResponseBody {
    pub fn from_vec( bytes: &Vec<u8>) -> Self {
        if bytes.is_empty() {
            return Self {
                consumerIdList: None
            };
        }

        info!("GetConsumerListByGroupResponseBody:{:?}", String::from_utf8(bytes.clone()));
        let body = serde_json::from_slice::<Self>(bytes);
        match body {
            Ok(body) => body,
            Err(err) => {
                warn!("GetConsumerListByGroupResponseBody::from_vec failed:{:?}, data len :{:?}", err, bytes.len());
                    Self {
                        consumerIdList: None
                }
            }
        }
    }
}