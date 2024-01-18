//! This module implements the `ChunkServer` for a simplified version of the
//! Google File System (GFS). It manages the storage and retrieval of chunks
//! (data blocks) in the system.

use std::sync::Arc;

use tarpc::context::Context;

use crate::{Chunk, ChunkMasterClient};

/// `ChunkServer` is responsible for handling chunk operations and interacting
/// with the `ChunkMaster`.
#[derive(Clone)]
pub struct ChunkServer(Arc<Inner>);

/// `Inner` holds the state of the `ChunkServer`, including a reference to the
/// `ChunkMasterClient`, a hashmap for chunk storage, and the server's own ID.
struct Inner;

impl ChunkServer {
	/// Creates a new ChunkServer, given the remote object for the master server
	/// and its ID.
	pub fn new(_master: ChunkMasterClient, _my_id: u64) -> Self {
		Self(Arc::new(Inner))
	}
}

impl Chunk for ChunkServer {
	async fn get(self, _: Context, _url: String) -> Option<String> {
		todo!()
	}
	
	async fn set(self, _ctx: Context, _url: String, _chunk: Option<String>) -> Option<String> {
		todo!()
	}
}
