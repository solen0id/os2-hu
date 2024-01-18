use gfs_lite::{ChunkClient, MasterClient};
use std::{
    env,
    net::{IpAddr, Ipv6Addr},
};
use tarpc::{client, context, serde_transport::tcp::connect, tokio_serde::formats::Json};

/// A simple test-client that should be extended to a command-line client
/// that can interface with the file system.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // parse the command-line arguments for the master servers IP-address
    let master_addr = env::args()
        .nth(1)
        .and_then(|ip| ip.parse::<IpAddr>().ok())
        .unwrap_or(IpAddr::V6(Ipv6Addr::LOCALHOST));

    // TODO: implement an interactive command line client

    // establish a connection with the master server
    let transport = connect((master_addr, 50000), Json::default).await?;
    let master = MasterClient::new(client::Config::default(), transport).spawn();

    // attempt to look-up the chunk server responsible for some file
    let chunk_addr = master.lookup(context::current(), "Foo".to_string()).await?;
    println!("Chunk-Server Addr: {chunk_addr:?}");

    // connect to the corresponding chunk server
    let transport = connect(chunk_addr, Json::default).await?;
    let chunk = ChunkClient::new(client::Config::default(), transport).spawn();

    // attempt to write data to that file
    let store_result = chunk
        .set(
            context::current(),
            "Foo".to_string(),
            Some("Bar".to_string()),
        )
        .await?;
    println!("Store Result: {store_result:?}");

    // read back the data
    let retrieve_result = chunk.get(context::current(), "Foo".to_string()).await?;
    println!("Retrieve Result: {retrieve_result:?}");

    Ok(())
}
