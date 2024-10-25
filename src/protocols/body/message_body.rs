use bytes::{Buf, Bytes};
use log::warn;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use zip::ZipArchive;

#[derive(Debug, Clone)]
#[allow(non_snake_case)]
pub struct MessageBody {
    // base
    pub topic: String,
    pub flag: i32,
    pub properties: HashMap<String, String>,
    pub body: Vec<u8>,
    pub transaction_id: String,
    // extend
    pub queue_id: i32,

    pub store_size: i32,

    pub queue_offset: i64,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    //pub SocketAddress bornHost;
    pub born_host: Vec<u8>,
    pub born_port: i32,

    pub store_timestamp: i64,
    // pub SocketAddress storeHost;
    pub store_host: Vec<u8>,
    pub store_port: i32,

    pub msg_id: String,
    pub commit_log_offset: i64,
    pub body_crc: i32,
    pub reconsume_times: i32,
    pub prepared_transaction_offset: i64,
}

impl MessageBody {
    // see java  org.apache.rocketmq.common.message; MessageDecoder decode method
    pub fn decode_from_bytes(bytes: Vec<u8>) -> Vec<Self> {
        let mut bytes = Bytes::from(bytes);
        let mut ret = vec![];
        while bytes.remaining() > 0 {
            // 1 TOTAL SIZE
            let store_size = bytes.get_i32();

            // 2 MAGIC CODE
            let _magic_code = bytes.get_i32();
            //

            // 3 BODY CRC
            let body_crc = bytes.get_i32();

            // 4 QUEUE ID
            let queue_id = bytes.get_i32();

            // 5 FLAG
            let flag = bytes.get_i32();

            // 6 QUEUE OFFSET
            let queue_offset = bytes.get_i64();

            //  7 PHYSICAL OFFSET commit log offset
            let physic_offset = bytes.get_i64();

            // 8 SYS FLAG
            let sys_flag = bytes.get_i32();

            // 9 BORN TIMESTAMP

            let born_timestamp = bytes.get_i64();

            // 10 BORN HOST
            let born_host = bytes.copy_to_bytes(4).to_vec();
            let born_port = bytes.get_i32();

            // 11 STORE TIMESTAMP
            let store_timestamp = bytes.get_i64();

            // 12 STORE HOST
            let store_host = bytes.copy_to_bytes(4).to_vec();
            let store_port = bytes.get_i32();

            // 13 RECONSUME TIMES
            let reconsume_times = bytes.get_i32();

            // 14 Prepared Transaction Offset
            let prepared_transaction_offset = bytes.get_i64();

            // 15 BODY
            let body_len = bytes.get_i32();
            let mut body = vec![];
            if body_len > 0 {
                body = bytes.copy_to_bytes(body_len as usize).to_vec();
                if (sys_flag & 1) == 1 {
                    body = unzip_body(body);
                }
            }

            // 16 TOPIC
            let topic_len = bytes.get_i8();
            let topic = bytes.copy_to_bytes(topic_len as usize).to_vec();

            // 17 properties
            let properties_len = bytes.get_i16();
            let mut properties = HashMap::new();
            if properties_len > 0 {
                let properties_bytes = bytes.copy_to_bytes(properties_len as usize);
                properties = decode_properties(properties_bytes);
            }

            // create msg id
            let msg_id = create_msg_id(&store_host.to_vec(), store_port, physic_offset);

            let transaction_id = if properties.contains_key("UNIQ_KEY") {
                properties.get("UNIQ_KEY").unwrap().to_string()
            } else {
                "".to_string()
            };
            let msg = MessageBody {
                topic: String::from_utf8(topic).unwrap(),
                flag,
                properties,
                body,
                transaction_id,
                queue_id,
                store_size,
                queue_offset,
                sys_flag,
                born_timestamp,
                born_host,
                born_port,
                store_timestamp,
                store_host,
                store_port,
                msg_id,
                commit_log_offset: physic_offset,
                body_crc,
                reconsume_times,
                prepared_transaction_offset,
            };
            ret.push(msg);
        }
        ret
    }
}

fn create_msg_id(host: &Vec<u8>, port: i32, offset: i64) -> String {
    let mut dest: [u8; 16] = [0; 16];
    dest[0] = host[0];
    dest[1] = host[1];
    dest[2] = host[2];
    dest[3] = host[3];
    let port: [u8; 4] = port.to_be_bytes();
    dest[4] = port[0];
    dest[5] = port[1];
    dest[6] = port[2];
    dest[7] = port[3];
    let offset: [u8; 8] = offset.to_be_bytes();
    dest[8] = offset[0];
    dest[9] = offset[1];
    dest[10] = offset[2];
    dest[11] = offset[3];
    dest[12] = offset[4];
    dest[13] = offset[5];
    dest[14] = offset[6];
    dest[15] = offset[7];
    // println!("src:{:?}, hex:{:?}, decode:{:?}", dest, hex::encode(dest), hex::decode(hex::encode(dest)));
    hex::encode_upper(dest)
}

fn decode_properties(properties: Bytes) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let p_str = String::from_utf8(properties.to_vec()).unwrap();
    for p in p_str.split("2") {
        let kv: Vec<&str> = p.split("1").collect();
        if kv.len() == 2 {
            map.insert(String::from(kv[0]), String::from(kv[1]));
        }
    }
    map
}

fn unzip_body(body: Vec<u8>) -> Vec<u8> {
    let reader = Cursor::new(&body);
    let zip = ZipArchive::new(reader);
    if zip.is_err() {
        warn!("unzip failed,return empty {:?}", zip);
        return vec![];
    }
    let mut zip = zip.unwrap();
    // zip文件中只有一个文件
    let mut file = zip.by_index(0).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).expect("unzip failed");
    buffer
}

#[cfg(test)]
mod test {
    use crate::protocols::body::message_body::create_msg_id;

    #[test]
    fn hex_bytes_convert_test() {
        let mut host = Vec::new();
        host.push(192);
        host.push(168);
        host.push(3);
        host.push(51);

        create_msg_id(&host, 11099, 123432);
    }
}
