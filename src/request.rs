use crate::resp::Resp;
use crate::server_result::{ServerError, ServerMessage, ServerResult, ServerValue};

use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct Request {
    pub value: Resp,
    pub sender: mpsc::Sender<ServerMessage>,
    pub binary: Vec<u8>,
    pub client_id: u64,
    pub master_connection: bool,
}

impl Request {
    pub async fn error(&self, e: ServerError) {
        let _ = self.sender.send(ServerMessage::Error(e)).await;
    }

    pub async fn data(&self, d: ServerValue) {
        let _ = self.sender.send(ServerMessage::Data(d)).await;
    }

    pub async fn result(&self, r: ServerResult) {
        let _ = self.sender.send(ServerMessage::from(r)).await;
    }
}
