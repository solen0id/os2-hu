//! This module implements a simplified version of the Google File System (GFS)
//! master server.
//!
//! The master server primarily focuses on managing chunk servers and handling
//! their metadata.

use std::{
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use tracing::{info, instrument};

use tarpc::context::Context;

use crate::{ChunkMaster, Master};

/// `GfsMaster` is a wrapper around `Inner` that provides thread-safe access.
#[derive(Clone, Default, Debug)]
pub struct GfsMaster(Arc<RwLock<Inner>>);

impl GfsMaster {
    pub fn new() -> Self {
        // let inner = Inner::new();
        // let locked = RwLock::new(inner);
        // let arced = Arc::new(locked);

        // return GfsMaster(arced);

        return Self(Arc::new(RwLock::new(Inner::default())));
    }
}

/// `Inner` holds the state of the `GfsMaster`.
/// It contains a list of chunk servers and a registry mapping URLs to their
/// respective chunk server addresses.
#[derive(Default, Debug)]
struct Inner {
    chunk_servers: Vec<u64>,
}

// impl Inner {
//     pub fn new() -> Self {
//         Self {
//             chunk_servers: true,
//         }
//     }
// }

// Implementation of the `Master` trait for `GfsMaster`.
impl Master for GfsMaster {
    #[instrument]
    async fn lookup(self, _: Context, _url: String) -> SocketAddr {
        info!("Master::GfsMaster::lookup(_url={})", _url);
        todo!();
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
        return 11111;
    }

    #[instrument]
    async fn insert(self, _: Context, _sender: u64, _url: String) {
        info!(
            "ChunkMaster::GfsMaster::insert(_sender={}, _url={})",
            _sender, _url
        );
    }

    #[instrument]
    async fn remove(self, _: Context, _url: String) {
        info!("ChunkMaster::GfsMaster::remove(_url={})", _url);
    }
}
