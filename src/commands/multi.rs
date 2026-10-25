use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::{ServerError, ServerValue};

pub async fn command(server: &mut Server, request: &Request, _command: &[String]) {
    let client = match server.clients.get_mut(&request.client_id) {
        Some(client) => client,
        None => {
            request
                .error(ServerError::CommandInternalError(String::from(
                    "Cannot find client ID",
                )))
                .await;
            return;
        }
    };

    client.multi = true;

    request
        .data(ServerValue::Resp(Resp::simple_string("OK")))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Client;
    use crate::server_result::ServerMessage;

    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_command_multi() {
        let cmd = vec![String::from("multi")];
        let mut server = Server::new("localhost".to_string(), 6379);
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let client = Client::new(1, connection_sender.clone());
        server.clients.insert(1, client);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string("OK"))),
        );
    }

    #[tokio::test]
    async fn test_command_multi_without_client() {
        let cmd = vec![String::from("multi")];
        let mut server = Server::new("localhost".to_string(), 6379);
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::CommandInternalError(String::from(
                "Cannot find client ID",
            ))),
        );
    }
}
