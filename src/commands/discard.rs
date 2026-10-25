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

    if !client.multi {
        request
            .data(ServerValue::Resp(Resp::simple_error(
                "ERR DISCARD without MULTI",
            )))
            .await;
        return;
    }

    client.multi = false;
    client.requests.clear();

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
    async fn test_command_discard_without_client() {
        let cmd = vec![String::from("discard")];
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

    #[tokio::test]
    async fn test_command_discard() {
        let cmd = vec![String::from("discard")];
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let mut server = Server::new("localhost".to_string(), 6379);

        let mut client = Client::new(1, connection_sender.clone());

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        client.multi = true;
        client.requests.push(request.clone());

        server.clients.insert(1, client);

        command(&mut server, &request, &cmd).await;

        let client = server.clients.get(&1).unwrap();

        assert_eq!(client.requests.len(), 0);
        assert!(!client.multi);

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string("OK"))),
        );
    }

    #[tokio::test]
    async fn test_command_discard_without_multi() {
        let cmd = vec![String::from("discard")];
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let mut server = Server::new("localhost".to_string(), 6379);

        let mut client = Client::new(1, connection_sender.clone());

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        client.multi = false;

        server.clients.insert(1, client);

        command(&mut server, &request, &cmd).await;

        let client = server.clients.get(&1).unwrap();

        assert_eq!(client.requests.len(), 0);
        assert!(!client.multi);

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_error(
                "ERR DISCARD without MULTI",
            ))),
        );
    }
}
