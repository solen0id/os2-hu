//! This crate defines the Remote Procedure Call (RPC) interface for the
//! simplified Google File System (GFS) using the `tarpc` crate. It includes the
//! definitions for the `Master`, `Chunk`, and `ChunkMaster` services, which are
//! essential for the communication between different components of the system.
//!
//! ## Services
//!
//! - `Master`: Service interface for the interaction between client
//!    applications and the master server.
//! - `Chunk`: Service interface for the interaction between client applications
//!    and the chunk servers.
//! - `ChunkMaster`: Service interface for chunk server management operations.

use std::net::SocketAddr;

pub mod master;
pub mod chunk;

/// The `Master` trait defines the RPC interface for general master server
/// operations.
#[tarpc::service]
pub trait Master {
	/// Looks up the socket address for a given URL. If the URL is not
	/// registered, it returns a random chunk server address.
	///
	/// # Parameters
	/// - `url`: `String` - The URL to lookup.
	///
	/// # Returns
	/// `SocketAddr` - The socket address of the chunk server managing the given URL.
	async fn lookup(url: String) -> SocketAddr;
}

/// The `Chunk` trait defines the RPC interface for operations on individual
/// data chunks.
#[tarpc::service]
pub trait Chunk {
	/// Retrieves a chunk by its URL.
	///
	/// # Parameters
	/// - `url`: `String` - The URL of the chunk to retrieve.
	///
	/// # Returns
	/// `Option<String>` - The chunk data if available.
	async fn get(url: String) -> Option<String>;
	
	/// Stores a chunk and optionally returns the previous version of the chunk.
	///
	/// # Parameters
	/// - `url`: `String` - The URL of the chunk.
	/// - `chunk`: `Option<String>` - The chunk data to store or `None` if it should be removed.
	///
	/// # Returns
	/// `Option<String>` - The old value of the chunk if it was previously stored.
	async fn set(url: String, chunk: Option<String>) -> Option<String>;
}

/// The `ChunkMaster` trait defines the RPC interface for chunk server
/// management operations.
#[tarpc::service]
pub trait ChunkMaster {
	/// Registers a new chunk server and returns its ID.
	///
	/// # Parameters
	/// - `socket_addr`: `SocketAddr` - The socket address of the new chunk server.
	///
	/// # Returns
	/// `u64` - The ID assigned to the new chunk server.
	async fn register(socket_addr: SocketAddr) -> u64;
	
	/// Associates a URL with a chunk server.
	///
	/// # Parameters
	/// - `sender`: `u64` - The ID of the chunk server.
	/// - `url`: `String` - The URL to be associated with the chunk server.
	async fn insert(sender: u64, url: String);
	
	/// Removes a URL from the chunk registry.
	///
	/// # Parameters
	/// - `url`: `String` - The URL to remove from the registry.
	async fn remove(url: String);
}
