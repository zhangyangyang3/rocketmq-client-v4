use atomic_counter::{AtomicCounter, ConsistentCounter};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::TcpStream;

/**
https://github.com/apache/rocketmq-client-go/blob/master/docs/zh/rocketmq-protocol_zh.md

total frame
    ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    + frame_size | header_length |         header_body        |     body     +
    ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    +   4bytes   |     4bytes    | (21 + r_len + e_len) bytes | remain bytes +
    ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

item 	type 	description
frame_size 	int32 	一个 RemotingCommand 数据包大小
header_length 	int32 	高8位表示数据的序列化方式，余下的表示真实 header 长度
header_body 	[]byte 	header 的 payload，长度由附带的 remark 和 properties 决定
body 	[]byte 	具体 Request/Response 的 payload



header_body
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+  request_code | l_flag | v_flag | opaque | request_flag |  r_len  |   r_body    |  e_len  |    e_body   +
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
+     2bytes    |  1byte | 2bytes | 4bytes |    4 bytes   | 4 bytes | r_len bytes | 4 bytes | e_len bytes +
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

item 	type 	description
request_code 	int16 	哪一种 Request 或 ResponseCode，具体类别由 request_flag 决定
l_flag 	byte 	language 位，用来标识Request来源方的开发语言
v_flag 	int16 	版本标记位
request_flag 	int32 	Header标记位，用来标记该 RemotingCommand 的类型和请求方式
opaque 	int32 	标识 Request/Response 的 RequestID，Broker 返回的 Response 通过该值和 Client 缓存的 Request 一一对应
r_len 	int32 	length of remark, remark 是 Request/Response 的附带说明信息，一般在 Response 中用来说明具体的错误原因
r_body 	[]byte 	payload of remark
e_len 	int32 	length of extended fields，即 properties，一些非标准字段会存储在这里，在 RocketMQ 的各种 feature 中均有广泛应用
e_body 	int32 	payload of extended fields

*/

const LANGUAGE_FLAG: i8 = 12; // 12 is define as rust
pub const VERSION_FLAG: i16 = 63;
static OPAQUE: LazyLock<ConsistentCounter> = LazyLock::new(|| ConsistentCounter::new(200));

pub const HEADER_SERIALIZE_METHOD_JSON: u8 = 0;
pub const HEADER_SERIALIZE_METHOD_PRIVATE: u8 = 1;

#[derive(Debug, Deserialize, Serialize)]
#[allow(non_snake_case)]
pub struct RemotingCommand {
    //private int code;
    //     private LanguageCode language = LanguageCode.JAVA;
    //     private int version = 0;
    //     private int opaque = requestId.getAndIncrement();
    //     private int flag = 0;
    //     private String remark;
    //     private HashMap<String, String> extFields;
    pub code: i16,
    pub language: String,
    pub version: i32,
    pub opaque: i32,
    pub flag: i32,
    pub remark: Option<String>,
    pub extFields: HashMap<String, String>,
    pub serializeTypeCurrentRPC: Option<String>,
}

impl RemotingCommand {
    pub fn get_language_i16(&self) -> i16 {
        return match self.language.as_str() {
            "JAVA" => 0,
            _ => 7,
        };
    }
}

#[derive(Debug)]
pub struct MqCommand {
    pub req_code: i16,
    pub l_flag: i8,
    /** for rust ,it always 7, means other language */
    pub v_flag: i16,
    pub opaque: i32,
    pub request_flag: i32,
    pub r_len: i32,
    pub r_body: Vec<u8>,
    pub e_len: i32,
    pub e_body: Vec<u8>,
    pub body: Vec<u8>,
    pub header_serialize_method: u8,
}

impl MqCommand {
    pub fn new() -> MqCommand {
        return MqCommand {
            req_code: 0,
            l_flag: LANGUAGE_FLAG,
            v_flag: VERSION_FLAG,
            opaque: OPAQUE.add(1) as i32,
            request_flag: 0,
            r_len: 0,
            r_body: Vec::new(),
            e_len: 0,
            e_body: Vec::new(),
            body: Vec::new(),
            header_serialize_method: HEADER_SERIALIZE_METHOD_PRIVATE,
        };
    }

    pub fn new_with_body(
        req_code: i16,
        r_body: Vec<u8>,
        e_body: Vec<u8>,
        body: Vec<u8>,
    ) -> MqCommand {
        return MqCommand {
            req_code,
            l_flag: LANGUAGE_FLAG,
            v_flag: VERSION_FLAG,
            opaque: OPAQUE.add(1) as i32,
            request_flag: 0,
            r_len: r_body.len() as i32,
            r_body,
            e_len: e_body.len() as i32,
            e_body,
            body,
            header_serialize_method: HEADER_SERIALIZE_METHOD_PRIVATE,
        };
    }

    pub async fn read_from_stream_with_opaque(broker_stream: &mut TcpStream, opaque: i32) -> Self {
        for _ in 0..5 {
            let cmd = Self::read_from_stream(broker_stream).await;
            if cmd.opaque != opaque {
                debug!("receive server send extended message, req_code:{}, remark:{:?}, extend:{:?}, body:{:?}",
            cmd.req_code, String::from_utf8(cmd.r_body), String::from_utf8(cmd.e_body), String::from_utf8(cmd.body));
            } else {
                return cmd;
            }
        }
        panic!("read from mq server failed, stop to connect");
    }

    pub async fn read_from_stream(stream: &mut TcpStream) -> Self {
        let size = stream.read_i32().await;
        if size.is_err() {
            panic!("read command from mq failed! {:?}", size.err());
        }
        let size = size.unwrap();
        let mut buf = vec![0u8; size as usize];
        let body = stream.read_exact(&mut buf).await;
        if body.is_err() {
            panic!("read command data from mq failed!, ignore it");
        }
        let mut frame = BytesMut::with_capacity((4 + size) as usize);
        frame.put_i32(size);
        frame.put_slice(&buf.to_vec());

        let cmd = Self::convert_bytes_to_mq_command(frame.to_vec());
        cmd
    }

    pub async fn read_from_read_half(stream: &mut OwnedReadHalf) -> Self {
        stream.readable().await.unwrap();
        let size = stream.read_i32().await;
        if size.is_err() {
            panic!("read command from mq failed! {:?}", size.err());
        }
        let size = size.unwrap();
        let mut buf = vec![0u8; size as usize];
        let body = stream.read_exact(&mut buf).await;
        if body.is_err() {
            panic!("read command data from mq failed!, ignore it");
        }
        let mut frame = BytesMut::with_capacity((4 + size) as usize);
        frame.put_i32(size);
        frame.put_slice(&buf.to_vec());

        let cmd = Self::convert_bytes_to_mq_command(frame.to_vec());
        cmd
    }

    /**
       convert a command to bytes
    */
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(1024);
        buf.put_i16(self.req_code);
        buf.put_i8(self.l_flag);
        buf.put_i16(self.v_flag);
        buf.put_i32(self.opaque);
        buf.put_i32(self.request_flag);
        buf.put_i32(self.r_body.len() as i32);
        buf.put_slice(&self.r_body);
        buf.put_i32(self.e_body.len() as i32);
        buf.put_slice(&self.e_body);
        let header_body = buf.to_vec();
        let header_len: i32 = header_body.len() as i32;
        let mut header_buf: [u8; 4] = [0; 4];
        // 0 means json, 1 means rocket mq protocol
        header_buf[0] = HEADER_SERIALIZE_METHOD_PRIVATE;
        header_buf[1] = ((header_len >> 16) & 0xFF) as u8;
        header_buf[2] = ((header_len >> 8) & 0xFF) as u8;
        header_buf[3] = ((header_len) & 0xFF) as u8;
        let body_len: i32 = self.body.len() as i32;
        // frame_size = header_len(4byte) + header_data's len + body_data's len
        let frame_size: i32 = 4 + header_len + body_len;

        let mut total_frame = BytesMut::with_capacity((4 + frame_size) as usize);
        total_frame.put_i32(frame_size);
        total_frame.put_slice(&header_buf);
        total_frame.put_slice(&header_body);
        total_frame.put_slice(&self.body);
        return total_frame.to_vec();
    }

    pub fn convert_bytes_to_mq_command(bytes: Vec<u8>) -> MqCommand {
        let mut buf = Bytes::from(bytes);
        if buf.len() < 4 {
            panic!("invalid body. the len less than 4!");
        }

        let frame_size = buf.get_i32();
        if buf.len() as i32 != frame_size {
            panic!(
                "invalid body. the len is not equal to frame_size!, frame_size: {}, buf_len: {}",
                frame_size,
                buf.len()
            );
        }

        let header_len = buf.get_i32();
        let header_serialize_method = ((header_len >> 24) & 0xFF) as u8;
        let header_len = header_len & 0x00FFFFFF;
        let header_body = buf.copy_to_bytes(header_len as usize).to_vec();
        let mut head_buf = Bytes::from(header_body);
        if header_serialize_method == HEADER_SERIALIZE_METHOD_JSON {
            let remoting_cmd: RemotingCommand = serde_json::from_slice(&head_buf.to_vec()).unwrap();
            let r_body = if remoting_cmd.remark.is_none() {
                vec![]
            } else {
                Vec::from(remoting_cmd.remark.unwrap().to_string())
            };

            let ext_fields = if remoting_cmd.extFields.is_empty() {
                vec![]
            } else {
                Vec::from(serde_json::to_string(&remoting_cmd.extFields).unwrap())
            };

            let body_len = buf.remaining();
            let body = buf.copy_to_bytes(body_len).to_vec();

            return MqCommand {
                req_code: remoting_cmd.code,
                l_flag: 0,
                v_flag: 0,
                opaque: remoting_cmd.opaque,
                request_flag: remoting_cmd.flag,
                r_len: r_body.len() as i32,
                r_body,
                e_len: ext_fields.len() as i32,
                e_body: ext_fields,
                body,
                header_serialize_method,
            };
        }
        let req_code = head_buf.get_i16();
        let l_flag = head_buf.get_i8();
        let v_flag = head_buf.get_i16();
        let opaque = head_buf.get_i32();
        let request_flag = head_buf.get_i32();
        let r_len = head_buf.get_i32();
        let r_body = if r_len > 0 {
            head_buf.copy_to_bytes(r_len as usize).to_vec()
        } else {
            vec![]
        };
        let e_len = head_buf.get_i32();
        let e_body = if e_len > 0 {
            head_buf.copy_to_bytes(e_len as usize).to_vec()
        } else {
            vec![]
        };

        let body_len = buf.remaining();
        let body = buf.copy_to_bytes(body_len).to_vec();

        MqCommand {
            req_code,
            l_flag,
            v_flag,
            opaque,
            request_flag,
            r_len,
            r_body,
            e_len,
            e_body,
            body,
            header_serialize_method,
        }
    }

    pub fn convert_extend_header_to_json(&self) -> String {
        match self.header_serialize_method {
            HEADER_SERIALIZE_METHOD_JSON => {
                warn!("not support header_serialize_method");
                panic!("not support header_serialize_method")
            }
            HEADER_SERIALIZE_METHOD_PRIVATE => {
                if self.e_len == 0 {
                    return "{}".to_string();
                } else {
                    let mut data = Bytes::from(self.e_body.clone());
                    let mut map = HashMap::new();
                    while data.has_remaining() {
                        let k_len = data.get_i16();
                        let k_name = data.copy_to_bytes(k_len as usize);
                        let v_len = data.get_i32();
                        let v_value = data.copy_to_bytes(v_len as usize);
                        map.insert(
                            String::from_utf8(k_name.to_vec()).unwrap(),
                            String::from_utf8(v_value.to_vec()).unwrap(),
                        );
                    }

                    serde_json::to_string(&map).unwrap()
                }
            }

            _ => {
                warn!("not support header_serialize_method");
                panic!("not support header_serialize_method");
            }
        }
    }
}

#[cfg(test)]
mod test {
    use atomic_counter::{AtomicCounter, ConsistentCounter};

    #[test]
    fn auto_incr_test() {
        let c = ConsistentCounter::new(0);
        for _ in 0..5 {
            let k = c.add(1) as i32;
            println!("k value is :{k}");
        }
    }
}
