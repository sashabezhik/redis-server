use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::ServerValue;

pub async fn command(_server: &Server, request: &Request, _command: &[String]) {
    // when a master sends PING to a replica
    // the replica must stay silent.
    if request.master_connection {
        return;
    }

    request
        .data(ServerValue::Resp(Resp::simple_string("PONG")))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;

    use tokio::sync::mpsc;

    // master

    #[tokio::test]
    async fn test_command_ping() {
        let cmd = vec![String::from("ping")];
        let server = Server::new("localhost".to_string(), 6379);
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string("PONG"))),
        );
    }

    #[tokio::test]
    async fn test_command_ping_uppercase() {
        let cmd = vec![String::from("PING")];
        let server = Server::new("localhost".to_string(), 6379);
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string("PONG"))),
        );
    }

    // replica

    #[tokio::test]
    async fn test_command_ping_master_connection() {
        let cmd = vec![String::from("PING")];
        let server = Server::new("localhost".to_string(), 6379);
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: true,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Empty,
        );
    }
}
