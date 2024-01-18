//! This binary crate implements a simplified version of the master server for
//! the Google File System (GFS).

use gfs_lite::master::GfsMaster;

/// The `main` function sets up and runs the TCP servers for both the `Master`
/// and `ChunkMaster` services. It listens on different ports for each service
/// and spawns tasks to handle incoming connections.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	// create the GFS master server
	let _server = GfsMaster::default();
	
	// simultaneously listen to the chunk servers and the client applications
	
	Ok(())
}
