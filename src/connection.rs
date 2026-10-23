use crate::request::Request;
use crate::resp::{self, Resp};
use crate::server::{self, ServerInfo};
use crate::server_result::{ServerError, ServerMessage, ServerValue};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

use std::fmt;

#[derive(Debug)]
pub enum ConnectionMessage {
    Request(Request),
}

#[derive(Debug)]
pub enum ConnectionError {
    BufferTooSmall(usize),
    CannotReadFromStream(String),
    CannotWriteToStream(String),
    MalformedResp(String),
    RequestFailed(String, String),
    ServerError(ServerError),
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall(size) => {
                write!(f, "Buffer too small, size {}.", size)
            }
            Self::CannotReadFromStream(string) => {
                write!(f, "Cannot read from stream: {}.", string)
            }
            Self::CannotWriteToStream(string) => {
                write!(f, "Cannot write to stream: {}.", string)
            }
            Self::MalformedResp(string) => {
                write!(f, "Cannot convert bytes to RESP: {}.", string)
            }
            Self::RequestFailed(request, e) => {
                write!(f, "Request {} failed: {}", request, e)
            }
            Self::ServerError(e) => {
                write!(f, "Server error: {}", e)
            }
        }
    }
}

// run a task that listens to the bound TCP port
// for incoming connection from clients and
// spawns a connection handler for each one of them.
pub async fn run_listener(host: String, port: u16, server_sender: mpsc::Sender<ConnectionMessage>) {
    let listener = TcpListener::bind(format!("{}:{}", host, port))
        .await
        .unwrap();

    loop {
        tokio::select! {
            connection = listener.accept() => {
                match connection {
                    Ok((stream, _)) => {
                        tokio::spawn(handle_connection(stream, server_sender.clone(), false));
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        continue;
                    }
                }
            }
        }
    }
}

// initialise and manage a connection with the master.
pub async fn run_master_listener(
    host: String,
    port: u16,
    info: &ServerInfo,
    server_sender: mpsc::Sender<ConnectionMessage>,
) {
    let mut stream = TcpStream::connect(format!("{}:{}", host, port))
        .await
        .unwrap();

    if let Err(e) = server::handshake(&mut stream, info).await {
        eprintln!("Handshake failed: {}", e);
        std::process::exit(1);
    }

    tokio::spawn(async move { handle_connection(stream, server_sender, true).await });
}

// the main entry point for valid TCP connections.
pub async fn handle_connection(
    mut stream: TcpStream,
    server_sender: mpsc::Sender<ConnectionMessage>,
    master_connection: bool,
) {
    let mut buffer = [0; 512];

    // create the MPSC channel to communicate with the connection.
    let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

    loop {
        tokio::select! {
            result = stream.read(&mut buffer) => {
                match result {
                    Ok(size) if size != 0 => {
                        let mut index = 0;

                        while index < size {
                            let start = index;

                            let resp = match resp::bytes_to_resp(&buffer[..size], &mut index) {
                                Ok(v) => v,
                                Err(e) => {
                                    eprintln!("Error: {}", e);
                                    return;
                                }
                            };

                            let request = Request {
                                value: resp,
                                sender: connection_sender.clone(),
                                binary: buffer[start..index].to_vec(),
                                master_connection,
                            };

                            match server_sender.send(ConnectionMessage::Request(request)).await {
                                Ok(()) => {},
                                Err(e) => {
                                    eprintln!("Error sending request: {}", e);
                                    return;
                                }
                            }
                        }
                    }
                    Ok(_) => {
                        println!("Connection closed");
                        break;
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                        break;
                    }
                }
            }

            Some(response) = connection_receiver.recv() => {
                let _ = match response {
                    ServerMessage::Data(ServerValue::Resp(v)) => stream.write_all(v.to_string().as_bytes()).await,
                    ServerMessage::Data(ServerValue::None) => Ok(()),
                    ServerMessage::Data(ServerValue::Binary(data)) => stream.write_all(&data).await,
                    ServerMessage::Error(e) => {
                        eprintln!("Error: {}", ConnectionError::ServerError(e));
                        return;
                    },
                };
            }
        }
    }
}

type ConnectionResult<T> = Result<T, ConnectionError>;

// write a RESP request to the stream and read the RESP response.
pub async fn stream_send_receive_resp<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    data: &Resp,
    buffer: &mut [u8],
) -> ConnectionResult<Resp> {
    stream_write_resp(stream, data)
        .await
        .map_err(|e| ConnectionError::RequestFailed(data.to_string(), e.to_string()))?;

    stream_read_resp(stream, buffer)
        .await
        .map_err(|e| ConnectionError::RequestFailed(data.to_string(), e.to_string()))
}

// write RESP data to the stream.
pub async fn stream_write_resp<W: AsyncWrite + Unpin>(
    stream: &mut W,
    data: &Resp,
) -> ConnectionResult<usize> {
    let string_data = data.to_string();
    let bytes = string_data.as_bytes();

    match stream.write_all(bytes).await {
        Ok(_) => Ok(bytes.len()),
        Err(e) => Err(ConnectionError::CannotWriteToStream(e.to_string())),
    }
}

// read RESP data from the stream.
async fn stream_read_resp<R: AsyncRead + Unpin>(
    stream: &mut R,
    buffer: &mut [u8],
) -> ConnectionResult<Resp> {
    let size = stream
        .read(buffer)
        .await
        .map_err(|e| ConnectionError::CannotReadFromStream(e.to_string()))?;

    let mut index = 0;

    resp::bytes_to_resp(&buffer[..size], &mut index)
        .map_err(|e| ConnectionError::MalformedResp(e.to_string()))
}

// read bytes from the stream until the \r\n is reached (including those bytes).
pub async fn stream_read_line<R: AsyncRead + Unpin>(
    stream: &mut R,
    buffer: &mut [u8],
) -> ConnectionResult<usize> {
    let mut byte = [0; 1];
    let mut index = 0;

    let mut previous_byte = 0;

    loop {
        if index >= buffer.len() {
            return Err(ConnectionError::BufferTooSmall(buffer.len()));
        }

        stream
            .read_exact(&mut byte)
            .await
            .map_err(|e| ConnectionError::CannotReadFromStream(e.to_string()))?;

        buffer[index] = byte[0];

        if byte[0] == b'\n' && previous_byte == b'\r' {
            break;
        }

        previous_byte = byte[0];
        index += 1;
    }

    Ok(index)
}

// read the given amount of bytes from the stream
pub async fn stream_read_data_length<R: AsyncRead + Unpin>(
    stream: &mut R,
    buffer: &mut [u8],
    length: usize,
) -> ConnectionResult<usize> {
    if length > buffer.len() {
        return Err(ConnectionError::BufferTooSmall(buffer.len()));
    }

    stream
        .read_exact(&mut buffer[..length])
        .await
        .map_err(|e| ConnectionError::CannotReadFromStream(e.to_string()))?;

    Ok(length)
}

#[cfg(test)]
mod tests {
    use super::*;

    // stream_send_receive_resp

    #[tokio::test]
    async fn test_stream_send_receive_resp() {
        let (mut client, mut server) = tokio::io::duplex(64);

        let peer = tokio::spawn(async move {
            let mut request_buffer = [0; 64];
            let n = server.read(&mut request_buffer).await.unwrap();

            assert_eq!(&request_buffer[..n], b"+PING\r\n");
            server.write_all(b"+PONG\r\n").await.unwrap();
        });

        let request = Resp::simple_string("PING");
        let mut buffer = [0; 64];

        let response = stream_send_receive_resp(&mut client, &request, &mut buffer)
            .await
            .unwrap();

        assert_eq!(response, Resp::simple_string("PONG"));
        peer.await.unwrap();
    }

    // stream_write_resp

    #[tokio::test]
    async fn test_stream_write_resp() {
        let mut stream = Vec::new();
        let data = Resp::simple_string("OK");

        let size = stream_write_resp(&mut stream, &data).await.unwrap();

        assert_eq!(stream, b"+OK\r\n");
        assert_eq!(size, 5);
    }

    // stream_read_resp

    #[tokio::test]
    async fn test_stream_read_resp() {
        let mut stream: &[u8] = b"+OK\r\n";
        let mut buffer = [0; 16];

        let resp = stream_read_resp(&mut stream, &mut buffer).await.unwrap();

        assert_eq!(resp, Resp::simple_string("OK"));
    }

    #[tokio::test]
    async fn test_stream_read_resp_short_message_in_large_buffer() {
        let mut stream: &[u8] = b"+OK\r\n";
        let mut buffer = [b'X'; 64];

        let resp = stream_read_resp(&mut stream, &mut buffer).await.unwrap();

        assert_eq!(resp, Resp::simple_string("OK"));
    }

    #[tokio::test]
    async fn test_stream_read_resp_malformed() {
        let mut stream: &[u8] = b"not resp";
        let mut buffer = [0; 16];

        match stream_read_resp(&mut stream, &mut buffer).await {
            Err(ConnectionError::MalformedResp(_)) => {}
            _ => panic!(),
        }
    }

    // stream_read_line

    #[tokio::test]
    async fn test_stream_read_line() {
        let mut stream: &[u8] = b"OK\r\n";
        let mut buffer = [0; 16];

        let index = stream_read_line(&mut stream, &mut buffer).await.unwrap();

        assert_eq!(&buffer[..=index], b"OK\r\n");
        assert_eq!(index, 3);
    }

    #[tokio::test]
    async fn test_stream_read_line_leading_newline() {
        let mut stream: &[u8] = b"\n\r\n";
        let mut buffer = [0; 16];

        let index = stream_read_line(&mut stream, &mut buffer).await.unwrap();

        assert_eq!(&buffer[..=index], b"\n\r\n");
        assert_eq!(index, 2);
    }

    #[tokio::test]
    async fn test_stream_read_line_buffer_too_small() {
        let mut stream: &[u8] = b"HELLO\r\n";
        let mut buffer = [0; 3];

        match stream_read_line(&mut stream, &mut buffer).await {
            Err(ConnectionError::BufferTooSmall(size)) => {
                assert_eq!(size, 3);
            }
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn test_stream_read_line_longer_string() {
        let mut stream: &[u8] = b"ECHO\r\n";
        let mut buffer = [0; 16];

        let index = stream_read_line(&mut stream, &mut buffer).await.unwrap();

        assert_eq!(&buffer[..=index], b"ECHO\r\n");
        assert_eq!(index, 5);
    }

    #[tokio::test]
    async fn test_stream_read_line_empty_stream() {
        let mut stream: &[u8] = b"";
        let mut buffer = [0; 16];

        match stream_read_line(&mut stream, &mut buffer).await {
            Err(ConnectionError::CannotReadFromStream(_)) => {}
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn test_stream_read_line_no_separator() {
        let mut stream: &[u8] = b"OK";
        let mut buffer = [0; 16];

        match stream_read_line(&mut stream, &mut buffer).await {
            Err(ConnectionError::CannotReadFromStream(_)) => {}
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn test_stream_read_line_half_separator() {
        let mut stream: &[u8] = b"OK\r";
        let mut buffer = [0; 16];

        match stream_read_line(&mut stream, &mut buffer).await {
            Err(ConnectionError::CannotReadFromStream(_)) => {}
            _ => panic!(),
        }
    }

    // stream_read_data_length

    #[tokio::test]
    async fn test_stream_read_data_length() {
        let mut stream: &[u8] = b"HELLO WORLD";
        let mut buffer = [0; 16];

        let size = stream_read_data_length(&mut stream, &mut buffer, 5)
            .await
            .unwrap();

        assert_eq!(size, 5);
        assert_eq!(&buffer[..size], b"HELLO");
    }

    #[tokio::test]
    async fn test_stream_read_data_length_short_stream() {
        let mut stream: &[u8] = b"HI";
        let mut buffer = [0; 16];

        match stream_read_data_length(&mut stream, &mut buffer, 5).await {
            Err(ConnectionError::CannotReadFromStream(_)) => {}
            _ => panic!(),
        }
    }
}
