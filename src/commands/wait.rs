use crate::request::Request;
use crate::resp::Resp;
use crate::server::{self, Server};
use crate::server_result::{ServerError, ServerMessage, ServerValue};

use tokio::sync::mpsc;

pub async fn command(server: &mut Server, request: &Request, command: &[String]) {
    // if no write has happened since the last WAIT
    // there is nothing for the replicas to
    // acknowledge. Report the replica count right
    // away and skip the handler machinery.
    if !server.replication.pending_write_operations {
        request
            .data(ServerValue::Resp(Resp::Integer(
                server.replica_senders.len() as i64,
            )))
            .await;
        return;
    }

    // no need to check if server is a replica
    // before going further, because on replica
    // `server.replication.pending_write_operations` is always false.

    if command.len() != 3 {
        request
            .error(ServerError::CommandSyntaxError(command.join(" ")))
            .await;
        return;
    }

    // the command comes in the form `WAIT numreplicas timeout`.
    let numreplicas = match command[1].parse() {
        Ok(v) => v,
        Err(_) => {
            request
                .error(ServerError::CommandSyntaxError(command.join(" ")))
                .await;
            return;
        }
    };

    let timeout = match command[2].parse() {
        Ok(v) => v,
        Err(_) => {
            request
                .error(ServerError::CommandSyntaxError(command.join(" ")))
                .await;
            return;
        }
    };

    server.replication.pending_write_operations = false;

    let getack = Resp::bulk_array(vec!["REPLCONF", "GETACK", "*"]);

    for replica in server.replica_senders.iter() {
        let _ = replica
            .send(ServerMessage::Data(ServerValue::Resp(getack.clone())))
            .await;
    }

    let (wait_handler_sender, wait_handler_receiver) = mpsc::channel::<ServerMessage>(32);

    server.wait_handler_sender = Some(wait_handler_sender);

    // spawn the wait handler. It owns the receiver
    // and a clone of the client's reply channel, so
    // it can answer the client directly.
    tokio::spawn(server::wait_handler(
        timeout,
        numreplicas,
        wait_handler_receiver,
        request.sender.clone(),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;

    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_command_wait_no_replicas() {
        let cmd = vec![String::from("wait"), String::from("0"), String::from("100")];
        let mut server = Server::new("localhost".to_string(), 6379);
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(0))),
        )
    }

    #[tokio::test]
    async fn test_command_wait_counts_replicas() {
        let cmd = vec![String::from("wait"), String::from("0"), String::from("100")];
        let mut server = Server::new("localhost".to_string(), 6379);

        let (replica_a_tx, _) = mpsc::channel::<ServerMessage>(32);
        let (replica_b_tx, _) = mpsc::channel::<ServerMessage>(32);
        server.replica_senders.push(replica_a_tx);
        server.replica_senders.push(replica_b_tx);

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(2))),
        )
    }

    #[tokio::test]
    async fn test_command_wait_pending_writes() {
        let cmd = vec![String::from("wait"), String::from("2"), String::from("100")];
        let mut server = Server::new("localhost".to_string(), 6379);

        let (replica_a_tx, mut replica_a_rx) = mpsc::channel::<ServerMessage>(32);
        let (replica_b_tx, mut replica_b_rx) = mpsc::channel::<ServerMessage>(32);
        server.replica_senders.push(replica_a_tx);
        server.replica_senders.push(replica_b_tx);

        server.replication.pending_write_operations = true;

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        let getack = ServerMessage::Data(ServerValue::Resp(Resp::bulk_array(vec![
            "REPLCONF", "GETACK", "*",
        ])));
        assert_eq!(replica_a_rx.try_recv().unwrap(), getack);
        assert_eq!(replica_b_rx.try_recv().unwrap(), getack);

        assert!(!server.replication.pending_write_operations);

        assert!(server.wait_handler_sender.is_some());

        assert_eq!(
            connection_receiver.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Empty
        );
    }

    #[tokio::test]
    async fn test_command_wait_handler_replies_on_threshold() {
        let cmd = vec![String::from("wait"), String::from("2"), String::from("100")];
        let mut server = Server::new("localhost".to_string(), 6379);

        let (replica_a_tx, _) = mpsc::channel::<ServerMessage>(32);
        let (replica_b_tx, _) = mpsc::channel::<ServerMessage>(32);
        server.replica_senders.push(replica_a_tx);
        server.replica_senders.push(replica_b_tx);

        server.replication.pending_write_operations = true;

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        let wait_sender = server.wait_handler_sender.clone().unwrap();
        wait_sender
            .send(ServerMessage::Data(ServerValue::None))
            .await
            .unwrap();
        wait_sender
            .send(ServerMessage::Data(ServerValue::None))
            .await
            .unwrap();

        let response = connection_receiver.recv().await.unwrap();
        assert_eq!(
            response,
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(2))),
        );
    }
}
