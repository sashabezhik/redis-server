use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::{ServerError, ServerValue};
use crate::set::SetArgs;

pub async fn command(server: &mut Server, request: &Request, command: &[String]) {
    let storage = match server.storage.as_mut() {
        Some(storage) => storage,
        None => {
            request.error(ServerError::StorageNotInitialised).await;
            return;
        }
    };

    if command.len() != 2 {
        request
            .error(ServerError::CommandSyntaxError(command.join(" ")))
            .await;
        return;
    }

    let key = &command[1];

    let output = storage.get(key);

    let result = match output {
        Ok(storage_value) => {
            let storage_value = storage_value.unwrap_or_else(|| String::from("0"));

            match storage_value.parse::<i64>() {
                Ok(n) => {
                    let value = n + 1;

                    match storage.set(key.clone(), value.to_string(), SetArgs::new()) {
                        Ok(_) => Ok(ServerValue::Resp(Resp::Integer(value))),
                        Err(_) => Err(ServerError::CommandInternalError(command.join(" "))),
                    }
                }
                Err(_) => Ok(ServerValue::Resp(Resp::simple_error(
                    "ERR value is not an integer or out of range",
                ))),
            }
        }
        Err(_) => Err(ServerError::CommandInternalError(command.join(" "))),
    };

    request.result(result).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;
    use crate::storage::Storage;

    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_command_incr_storage_not_initialised() {
        let mut server = Server::new("localhost".to_string(), 6379);

        let cmd = vec![String::from("incr"), String::from("key")];

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
            ServerMessage::Error(ServerError::StorageNotInitialised),
        );
    }

    #[tokio::test]
    async fn test_command_incr_wrong_syntax() {
        let storage = Storage::new();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("incr")];

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
            ServerMessage::Error(ServerError::CommandSyntaxError("incr".to_string()))
        );
    }

    #[tokio::test]
    async fn test_command_incr_key_exists() {
        let mut storage = Storage::new();
        storage
            .set("key".to_string(), "42".to_string(), SetArgs::new())
            .unwrap();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("incr"), String::from("key")];

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
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(43))),
        );

        assert_eq!(
            server.storage.as_mut().unwrap().get("key").unwrap(),
            Some("43".to_string()),
        );
    }

    #[tokio::test]
    async fn test_command_incr_key_does_not_exist() {
        let storage = Storage::new();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("incr"), String::from("key")];

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
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(1))),
        );
    }

    #[tokio::test]
    async fn test_command_incr_key_is_not_numeric() {
        let mut storage = Storage::new();
        storage
            .set("key".to_string(), "answer".to_string(), SetArgs::new())
            .unwrap();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("incr"), String::from("key")];

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
            ServerMessage::Data(ServerValue::Resp(Resp::simple_error(
                "ERR value is not an integer or out of range",
            ))),
        );
    }
}
