//! This binary crate implements a simplified version of the master server for
//! the Google File System (GFS).
//!
use futures::StreamExt;
use std::{
    future::ready,
    net::{IpAddr, Ipv6Addr},
    time::Duration,
};
use tarpc::{
    serde_transport::tcp::listen,
    server::{BaseChannel, Channel},
    tokio_serde::formats::Json,
};
use tokio::time::sleep;
use tracing_subscriber;

use gfs_lite::master::GfsMaster;
use gfs_lite::ChunkMaster;
use gfs_lite::Master;

/// The `main` function sets up and runs the TCP servers for both the `Master`
/// and `ChunkMaster` services. It listens on different ports for each service
/// and spawns tasks to handle incoming connections.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // create the GFS master server
    let _server = GfsMaster::new();
    let _client_server = _server.clone();
    let _chunk_server = _server.clone();

    // create (Client) Master connection on port 50000
    let client_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), 50000);
    let mut client_listener = listen(&client_addr, Json::default).await?;

    client_listener.config_mut().max_frame_length(usize::MAX);

    let _ = tokio::spawn(async {
        client_listener
            .filter_map(|r| ready(r.ok()))
            .map(move |transport| {
                BaseChannel::with_defaults(transport)
                    .execute(Master::serve(_client_server.clone()))
                    .for_each(|future| async move {
                        tokio::spawn(future);
                    })
            })
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await
    });

    // create ChunkMaster connection on port 60000
    let chunk_master_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), 60000);
    let mut chunk_master_listener = listen(&chunk_master_addr, Json::default).await?;

    chunk_master_listener
        .config_mut()
        .max_frame_length(usize::MAX);

    let _ = tokio::spawn(async move {
        chunk_master_listener
            .filter_map(|r| ready(r.ok()))
            .map(move |transport| {
                BaseChannel::with_defaults(transport)
                    .execute(ChunkMaster::serve(_chunk_server.clone()))
                    .for_each(|future| async move {
                        tokio::spawn(future);
                    })
            })
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await
    });

    println!("Waiting on tasks..");
    sleep(Duration::from_secs(60 * 60)).await;

    Ok(())
}
