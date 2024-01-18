//! This module implements the `ChunkServer` for a simplified version of the
//! Google File System (GFS). It manages the storage and retrieval of chunks
//! (data blocks) in the system.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tarpc::context::Context;
use tracing::{info, instrument};

use crate::{Chunk, ChunkMasterClient};

/// `ChunkServer` is responsible for handling chunk operations and interacting
/// with the `ChunkMaster`.
#[derive(Clone, Debug)]
pub struct ChunkServer(Arc<RwLock<Inner>>);

/// `Inner` holds the state of the `ChunkServer`, including a reference to the
/// `ChunkMasterClient`, a hashmap for chunk storage, and the server's own ID.
#[derive(Debug)]
struct Inner {
    id: u64,
    master: ChunkMasterClient,
    mapping: HashMap<String, String>,
}

impl ChunkServer {
    /// Creates a new ChunkServer, given the remote object for the master server
    /// and its ID.
    pub fn new(_master: ChunkMasterClient, _my_id: u64) -> Self {
        let inner = Inner {
            id: _my_id,
            master: _master,
            mapping: HashMap::new(),
        };

        Self(Arc::new(RwLock::new(inner)))
    }

    pub fn set_chunk(&self, url: String, chunk: Option<String>) -> Option<String> {
        let mut inner = self.0.write().unwrap();

        match chunk {
            Some(chunk) => {
                return inner.mapping.insert(url, chunk);
            }
            None => {
                inner.mapping.remove(&url);
                return None;
            }
        }
    }

    pub fn get_chunk(&self, url: String) -> Option<String> {
        let inner = self.0.read().unwrap();
        return inner.mapping.get(&url).cloned();
    }
}

impl Chunk for ChunkServer {
    #[instrument]
    async fn get(self, _: Context, _url: String) -> Option<String> {
        info!("Chunk::ChunkServer::get(_url={})", _url);
        return self.get_chunk(_url);
    }

    #[instrument]
    async fn set(self, _ctx: Context, _url: String, _chunk: Option<String>) -> Option<String> {
        info!(
            "Chunk::ChunkServer::set(_url={}, _chunk={:?})",
            _url, _chunk
        );
        return self.set_chunk(_url, _chunk);
    }
}
