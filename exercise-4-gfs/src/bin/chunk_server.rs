//! This module implements the `ChunkServer` for a simplified version of the
//! Google File System (GFS). It manages the storage and retrieval of chunks
//! (data blocks) in the system.

use futures::StreamExt;
use rand::Rng;
use std::{
    env,
    future::ready,
    net::{IpAddr, Ipv6Addr},
};
use tarpc::{
    client, context,
    serde_transport::tcp::connect,
    serde_transport::tcp::listen,
    server::{BaseChannel, Channel},
    tokio_serde::formats::Json,
};

use gfs_lite::ChunkMasterClient;
use gfs_lite::{chunk::ChunkServer, Chunk};

/// The `main` function sets up the `ChunkServer`, connects it to the
/// `ChunkMaster`, and starts listening for chunk operations.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // parse the command-line arguments for the master servers IP-address
    let _master_ip = env::args()
        .nth(1)
        .and_then(|ip| ip.parse::<IpAddr>().ok())
        .unwrap_or(IpAddr::V6(Ipv6Addr::LOCALHOST));
    let _master_addr = (_master_ip, 60000);

    // connect to the master server
    let transport = connect(&_master_addr, Json::default).await?;
    let master_client = ChunkMasterClient::new(client::Config::default(), transport).spawn();

    // open a channel for client commands
    let client_ip = rand::thread_rng().gen_range(60000..64000);
    let client_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), client_ip);
    let mut client_listener = listen(&client_addr, Json::default).await?;

    // initialize the chunk server
    let chunk_server_id = master_client
        .register(context::current(), client_listener.local_addr())
        .await?;
    println!("{}", chunk_server_id);
    let chunk_server = ChunkServer::new(master_client, chunk_server_id);

    // listen for RPC traffic from clients
    client_listener.config_mut().max_frame_length(usize::MAX);
    client_listener
        .filter_map(|r| ready(r.ok()))
        .map(|transport| {
            BaseChannel::with_defaults(transport)
                .execute(chunk_server.clone().serve())
                .for_each(|future| async move {
                    tokio::spawn(future);
                })
        })
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;

    Ok(())
}
