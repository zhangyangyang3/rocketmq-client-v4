use crate::protocols::body::message_body::MessageBody;

pub trait MessageHandler {
    fn handle(&self, message: &MessageBody) -> impl std::future::Future<Output = ()> + Send;
}