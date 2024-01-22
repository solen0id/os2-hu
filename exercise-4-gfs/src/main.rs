use gfs_lite::{ChunkClient, MasterClient};
use std::{
    env,
    net::{IpAddr, Ipv6Addr},
};
use tarpc::{client, context, serde_transport::tcp::connect, tokio_serde::formats::Json};
use text_io::read;

//NOTE: make sure to run a master_server and a chunk_server before running this client
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // parse the command-line arguments for the master servers IP-address
    let master_addr = env::args()
        .nth(1)
        .and_then(|ip| ip.parse::<IpAddr>().ok())
        .unwrap_or(IpAddr::V6(Ipv6Addr::LOCALHOST));

    // establish a connection with the master server
    // we hard-code the port to 50000
    let transport = connect((master_addr, 50000), Json::default).await?;
    let master = MasterClient::new(client::Config::default(), transport).spawn();

    loop {
        println!("\nEnter a command: (get/set/lookup/delete/exit)");
        let command: String = read!();

        if command == "get" {
            println!("Enter a file name: ");
            let file_name: String = read!();

            let chunk_addr = master.lookup(context::current(), file_name.clone()).await?;
            let transport = connect(chunk_addr, Json::default).await?;
            let chunk = ChunkClient::new(client::Config::default(), transport).spawn();

            let retrieve_result = chunk.get(context::current(), file_name.clone()).await?;
            println!("Retrieve Result from {chunk_addr}: {retrieve_result:?}");
        } else if command == "set" {
            println!("Enter a file name: ");
            let file_name: String = read!();
            println!("Enter a file content: ");
            let file_content: String = read!();

            let chunk_addr = master.lookup(context::current(), file_name.clone()).await?;
            let transport = connect(chunk_addr, Json::default).await?;
            let chunk = ChunkClient::new(client::Config::default(), transport).spawn();

            let store_result = chunk
                .set(context::current(), file_name, Some(file_content))
                .await?;
            println!("Stored new value on {chunk_addr} (Previous value: {store_result:?})");
        } else if command == "lookup" {
            println!("Enter a file name: ");
            let file_name: String = read!();
            let chunk_addr = master.lookup(context::current(), file_name).await?;
            println!("Chunk-Server Addr: {chunk_addr:?}");
        } else if command == "delete" {
            println!("Enter a file name: ");
            let file_name: String = read!();

            let chunk_addr = master.lookup(context::current(), file_name.clone()).await?;
            let transport = connect(chunk_addr, Json::default).await?;
            let chunk = ChunkClient::new(client::Config::default(), transport).spawn();

            let delete_result = chunk.set(context::current(), file_name, None).await?;
            println!("Deleted previosly stored result: {delete_result:?}");
        } else if command == "exit" {
            break;
        } else {
            println!("Invalid command");
        }
    }

    Ok(())
}
