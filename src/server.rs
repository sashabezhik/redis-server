use crate::commands::{echo, get, info, ping, psync, replconf, set, wait};
use crate::connection::{self, ConnectionMessage};
use crate::replication::ReplicationConfig;
use crate::request::Request;
use crate::resp::{self, Resp};
use crate::server_result::{ServerError, ServerMessage, ServerResult, ServerValue};
use crate::storage::Storage;

use tokio::{io::AsyncWriteExt, net::TcpStream, sync::mpsc};

use std::time::Duration;

pub struct ServerInfo {
    #[expect(dead_code)]
    pub host: String,
    pub port: u16,
}

pub struct Server {
    pub info: ServerInfo,
    pub storage: Option<Storage>,
    pub replication: ReplicationConfig,
    pub replica_senders: Vec<mpsc::Sender<ServerMessage>>,
    pub wait_handler_sender: Option<mpsc::Sender<ServerMessage>>,
}

impl Server {
    pub fn new(host: String, port: u16) -> Self {
        Self {
            info: ServerInfo { host, port },
            storage: None,
            replication: ReplicationConfig::new_master(),
            replica_senders: Vec::new(),
            wait_handler_sender: None,
        }
    }

    pub fn set_storage(&mut self, storage: Storage) {
        self.storage = Some(storage);
    }

    pub fn set_replication(&mut self, config: ReplicationConfig) {
        self.replication = config;
    }

    // the entry point for key expiry operations.
    pub fn expire_keys(&mut self) {
        let storage = match self.storage.as_mut() {
            Some(storage) => storage,
            None => return,
        };

        storage.expire_keys();
    }

    pub fn generate_rdb(&self) -> Vec<u8> {
        let v = vec![
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64,
            0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a,
            0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05,
            0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73,
            0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61,
            0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe,
            0xc0, 0xff, 0x5a, 0xa2,
        ];

        v
    }
}

// listen for incoming messages and process the contained requests.
pub async fn run_server(mut server: Server, mut crx: mpsc::Receiver<ConnectionMessage>) {
    let mut interval_timer = tokio::time::interval(Duration::from_millis(10));

    loop {
        tokio::select! {
            Some(message) = crx.recv() => {
                match message {
                    ConnectionMessage::Request(request) => {
                        process_request(request, &mut server).await;
                    }
                }
            }

            _ = interval_timer.tick() => {
                server.expire_keys();
            }
        }
    }
}

// forward the given request to all replicas known to the server.
pub async fn send_request_to_replicas(request: &Request, server: &Server) {
    for replica_sender in server.replica_senders.iter() {
        let _ = replica_sender
            .send(ServerMessage::Data(ServerValue::Binary(
                request.binary.clone(),
            )))
            .await;
    }
}

// process an incoming request and return a result.
pub async fn process_request(request: Request, server: &mut Server) {
    let elements = match &request.value {
        Resp::Array(v) => v,
        _ => {
            request.error(ServerError::IncorrectData).await;
            return;
        }
    };

    let mut command = Vec::new();

    for elem in elements.iter() {
        match elem {
            Resp::BulkString(v) => command.push(v.clone()),
            _ => {
                request.error(ServerError::IncorrectData).await;
                return;
            }
        }
    }

    let command_name = command[0].to_lowercase();

    match command_name.as_str() {
        "echo" => {
            echo::command(server, &request, &command).await;
        }
        "get" => {
            get::command(server, &request, &command).await;
        }
        "info" => {
            info::command(server, &request, &command).await;
        }
        "ping" => {
            ping::command(server, &request, &command).await;
        }
        "psync" => {
            psync::command(server, &request, &command).await;
        }
        "replconf" => {
            replconf::command(server, &request, &command).await;
        }
        "set" => {
            set::command(server, &request, &command).await;

            send_request_to_replicas(&request, server).await;
        }
        "wait" => {
            wait::command(server, &request, &command).await;
        }
        _ => {
            request
                .error(ServerError::CommandNotAvailable(command[0].clone()))
                .await;
        }
    }

    // track how many bytes have been processed
    // by the server. The master will use this
    // value to check if the replicas are in sync.
    server.replication.repl_offset += request.binary.len();
}

// the implementation of the handshake process
// between a replica and the master.
pub async fn handshake(stream: &mut TcpStream, info: &ServerInfo) -> ServerResult {
    let mut buffer = [0; 512];

    // send ping

    let ping = Resp::bulk_array(vec!["PING"]);

    let resp = connection::stream_send_receive_resp(stream, &ping, &mut buffer)
        .await
        .map_err(|e| ServerError::HandshakeFailed(e.to_string()))?;

    if resp != Resp::simple_string("PONG") {
        return Err(ServerError::HandshakeFailed(String::from("PING failed")));
    }

    // send replconf listening-port

    let replconf = Resp::bulk_array(vec!["REPLCONF", "listening-port", &info.port.to_string()]);

    let resp = connection::stream_send_receive_resp(stream, &replconf, &mut buffer)
        .await
        .map_err(|e| ServerError::HandshakeFailed(e.to_string()))?;

    if resp != Resp::simple_string("OK") {
        return Err(ServerError::HandshakeFailed(format!(
            "Sending {} - Wrong server answer: {}",
            replconf, resp,
        )));
    }

    // send replconf capa psync2

    let replconf = Resp::bulk_array(vec!["REPLCONF", "capa", "psync2"]);

    let resp = connection::stream_send_receive_resp(stream, &replconf, &mut buffer)
        .await
        .map_err(|e| ServerError::HandshakeFailed(e.to_string()))?;

    if resp != Resp::simple_string("OK") {
        return Err(ServerError::HandshakeFailed(format!(
            "Sending {} - Wrong server answer: {}",
            replconf, resp,
        )));
    }

    // send psync ? -1

    let psync = Resp::bulk_array(vec!["PSYNC", "?", "-1"]);

    stream
        .write_all(psync.to_string().as_bytes())
        .await
        .map_err(|e| {
            ServerError::HandshakeFailed(format!(
                "Sending {} - Cannot write to stream: {}",
                psync, e,
            ))
        })?;

    let mut index = 0;

    if connection::stream_read_line(stream, &mut buffer)
        .await
        .is_err()
    {
        return Err(ServerError::HandshakeFailed(String::from(
            "PSYNC failed, no FULLRESYNC",
        )));
    }

    let response = resp::bytes_to_resp(&buffer, &mut index).ok();
    match response {
        Some(Resp::SimpleString(_)) => {}
        _ => {
            return Err(ServerError::HandshakeFailed(String::from(
                "PSYNC failed, wrong FULLRESYNC response",
            )));
        }
    };

    if connection::stream_read_line(stream, &mut buffer)
        .await
        .is_err()
    {
        return Err(ServerError::HandshakeFailed(String::from(
            "PSYNC failed, cannot read RDB length",
        )));
    }

    index = 0;

    if resp::resp_remove_type('$', &buffer, &mut index).is_err() {
        return Err(ServerError::HandshakeFailed(String::from(
            "PSYNC failed, RDB doesn't start with $",
        )));
    }

    let length = resp::resp_extract_length(&buffer, &mut index).map_err(|_| {
        ServerError::HandshakeFailed(String::from("PSYNC failed, malformed RDB length"))
    })?;

    if connection::stream_read_data_length(stream, &mut buffer, length as usize)
        .await
        .is_err()
    {
        return Err(ServerError::HandshakeFailed(String::from(
            "PSYNC failed, cannot read RDB",
        )));
    }

    Ok(ServerValue::None)
}

// collect replica ACKs and reply to the client
// as soon as the requested number of replicas
// has acknowledged.
pub async fn wait_handler(
    timeout: u64,
    numreplicas: i64,
    mut wait_handler_receiver: mpsc::Receiver<ServerMessage>,
    client_sender: mpsc::Sender<ServerMessage>,
) {
    let mut acks = 0;

    // we need to call interval_timer once,
    // because the first call returns immediately.
    let mut interval_timer = tokio::time::interval(Duration::from_millis(timeout));
    interval_timer.tick().await;

    loop {
        // when the requested number of replicas
        // is reached, reply to the client.
        if acks >= numreplicas {
            let _ = client_sender
                .send(ServerMessage::Data(ServerValue::Resp(Resp::Integer(acks))))
                .await;
            break;
        }

        tokio::select! {
            Some(_) = wait_handler_receiver.recv() => {
                acks += 1;
            }

            _ = interval_timer.tick() => {
                let _ = client_sender
                    .send(ServerMessage::Data(ServerValue::Resp(Resp::Integer(acks))))
                    .await;
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::{ServerMessage, ServerValue};

    use tokio::sync::mpsc;

    // process_request

    #[tokio::test]
    async fn test_process_request_ping() {
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::bulk_array(vec!["PING"]),
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        let storage = Storage::new();
        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        process_request(request, &mut server).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string("PONG"))),
        );
    }

    #[tokio::test]
    async fn test_process_request_echo() {
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::bulk_array(vec!["ECHO", "42"]),
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        let storage = Storage::new();
        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        process_request(request, &mut server).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::bulk_string("42"))),
        );
    }

    #[tokio::test]
    async fn test_process_request_not_array() {
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::bulk_string("PING"),
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        let storage = Storage::new();
        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        process_request(request, &mut server).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::IncorrectData),
        );
    }

    #[tokio::test]
    async fn test_process_request_not_bulkstrings() {
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Array(vec![Resp::simple_string("PING")]),
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        let storage = Storage::new();
        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        process_request(request, &mut server).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::IncorrectData),
        );
    }

    // new

    #[test]
    fn test_create_new() {
        let server = Server::new("localhost".to_string(), 6379);

        assert!(server.storage.is_none());
    }

    // set_storage

    #[test]
    fn test_set_storage() {
        let storage = Storage::new();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        assert!(server.storage.is_some());
    }
}
