//! This module implements the `ChunkServer` for a simplified version of the
//! Google File System (GFS). It manages the storage and retrieval of chunks
//! (data blocks) in the system.

use std::{
	env,
	net::{IpAddr, Ipv6Addr},
};

/// The `main` function sets up the `ChunkServer`, connects it to the
/// `ChunkMaster`, and starts listening for chunk operations.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	// parse the command-line arguments for the master servers IP-address 
	let _master_addr =
		env::args().nth(1)
			.and_then(|ip| ip.parse::<IpAddr>().ok())
			.unwrap_or(IpAddr::V6(Ipv6Addr::LOCALHOST));
	
	// connect to the master server
	// open a channel for client commands
	// initialize the chunk server
	// listen for RPC traffic from clients
	
	Ok(())
}
