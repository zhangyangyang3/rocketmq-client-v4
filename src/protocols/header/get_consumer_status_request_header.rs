use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct GetConsumerStatusRequestHeader {
//    @CFNotNull
    //     private String topic;
    //     @CFNotNull
    //     private String group;
    //     @CFNullable
    //     private String clientAddr;
    pub topic: String,
    pub group: String,
    pub client_addr: Option<String>,
}