//! This module implements a simplified version of the Google File System (GFS)
//! master server.
//!
//! The master server primarily focuses on managing chunk servers and handling
//! their metadata.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use rand::Rng;
use tarpc::context::Context;
use tracing::{info, instrument};

use crate::{chunk, ChunkMaster, Master};

/// `GfsMaster` is a wrapper around `Inner` that provides thread-safe access.
#[derive(Clone, Default, Debug)]
pub struct GfsMaster(Arc<RwLock<Inner>>);

impl GfsMaster {
    pub fn new() -> Self {
        return Self(Arc::new(RwLock::new(Inner::default())));
    }

    pub fn add_chunk_server(&self, chunk_server_addr: SocketAddr) -> u64 {
        let mut inner = self.0.write().unwrap();

        // Assumption: ChunkServers live forever --> position in vector never changes
        inner.chunk_servers.push(chunk_server_addr);
        let chunk_server_id = inner.chunk_servers.len() - 1;

        return chunk_server_id.try_into().unwrap();
    }

    pub fn add_url(&self, url: String, value: u64) {
        let mut inner = self.0.try_write().unwrap();
        inner.mapping.insert(url, value);
    }

    pub fn remove_url(&self, url: &String) {
        let mut inner = self.0.write().unwrap();
        let _ = inner.mapping.remove(url);
    }

    pub fn lookup_or_choose(&self, url: String) -> u64 {
        let mut inner = self.0.write().unwrap();
        let chunk_server_id = inner.mapping.get(&url);

        match chunk_server_id {
            Some(&id) => id,
            None => {
                let n_chunk_servers = inner.chunk_servers.len();
                let random_chunk_server_id: u64 = rand::thread_rng()
                    .gen_range(0..n_chunk_servers)
                    .try_into()
                    .unwrap();

                inner.mapping.insert(url, random_chunk_server_id);

                return random_chunk_server_id;
            }
        }
    }

    pub fn get_chunk_server_addr_for_id(&self, id: usize) -> SocketAddr {
        let inner = self.0.read().unwrap();
        let addr = inner
            .chunk_servers
            .get(id)
            .expect("Illegal access to chunk server");
        return addr.clone();
    }
}

/// `Inner` holds the state of the `GfsMaster`.
/// It contains a list of chunk servers and a registry mapping URLs to their
/// respective chunk server addresses.
#[derive(Default, Debug)]
struct Inner {
    chunk_servers: Vec<SocketAddr>,
    mapping: HashMap<String, u64>,
}

// Implementation of the `Master` trait for `GfsMaster`.
impl Master for GfsMaster {
    #[instrument]
    async fn lookup(self, _: Context, _url: String) -> SocketAddr {
        info!("Master::GfsMaster::lookup(_url={})", _url);
        let chunk_server_id = self.lookup_or_choose(_url);
        return self.get_chunk_server_addr_for_id(chunk_server_id.try_into().unwrap());
    }
}

// Implementation of the `ChunkMaster` trait for `GfsMaster`.
impl ChunkMaster for GfsMaster {
    #[instrument]
    async fn register(self, _: Context, _socket_addr: SocketAddr) -> u64 {
        info!(
            "ChunkMaster::GfsMaster::register(_socket_addr={})",
            _socket_addr
        );
        let chunk_server_id = self.add_chunk_server(_socket_addr);
        return chunk_server_id;
    }

    #[instrument]
    async fn insert(self, _: Context, _sender: u64, _url: String) {
        info!(
            "ChunkMaster::GfsMaster::insert(_sender={}, _url={})",
            _sender, _url
        );
        self.add_url(_url, _sender);
    }

    #[instrument]
    async fn remove(self, _: Context, _url: String) {
        info!("ChunkMaster::GfsMaster::remove(_url={})", _url);
        self.remove_url(&_url);
    }
}
