mod client;
mod commands;
mod connection;
mod replication;
mod request;
mod resp;
mod resp_result;
mod server;
mod server_result;
mod set;
mod storage;
mod storage_result;

use connection::ConnectionMessage;
use replication::ReplicationConfig;
use server::Server;
use storage::Storage;

use clap::Parser;
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(
        short = 'H',
        long,
        help = "The host address to bind the server to",
        default_value_t = String::from("127.0.0.1"),
    )]
    host: String,

    #[arg(
        short,
        long,
        help = "The TCP port to use for the server",
        default_value_t = 6379
    )]
    port: u16,

    #[arg(
        short,
        long,
        help = "The master server for this replica, in the form `address port`"
    )]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let replication_config = match args.replicaof {
        None => ReplicationConfig::new_master(),
        Some(params) => {
            let (host, port_string) = match params.split_once(" ") {
                Some(value) => value,
                None => {
                    eprintln!("Please provide 'HOST PORT' separated by space");
                    std::process::exit(1);
                }
            };

            let port = match port_string.parse() {
                Ok(p) => p,
                Err(_) => {
                    eprintln!("Port is not a number");
                    std::process::exit(1);
                }
            };

            ReplicationConfig::new_replica(host.to_owned(), port)
        }
    };

    let mut storage = Storage::new();
    storage.set_active_expiry(true);

    let mut server = Server::new(args.host.clone(), args.port);
    server.set_storage(storage);
    server.set_replication(replication_config);

    let (server_sender, server_receiver) = mpsc::channel::<ConnectionMessage>(32);

    // store the server sender in the server itself.
    // this allows the server to spawn functions
    // that send requests back to itself (e.g EXEC).
    server.self_sender = Some(server_sender.clone());

    // if this server is a replica,
    // connect to the master and listen.
    if let Some(master_config) = &server.replication.master {
        connection::run_master_listener(
            master_config.host.clone(),
            master_config.port,
            &server.info,
            server_sender.clone(),
        )
        .await;
    };

    // spawn the server task
    tokio::spawn(server::run_server(server, server_receiver));

    connection::run_listener(args.host, args.port, server_sender).await;

    Ok(())
}
