use serde::Serialize;

#[derive(Debug,Serialize)]
#[allow(non_snake_case)]
pub struct ProducerData {
    pub groupName: String,
}