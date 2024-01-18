//! This module implements a simplified version of the Google File System (GFS)
//! master server.
//! 
//! The master server primarily focuses on managing chunk servers and handling
//! their metadata.

use std::{
	net::SocketAddr,
	sync::{Arc, RwLock}
};

use tarpc::context::Context;

use crate::{ChunkMaster, Master};

/// `GfsMaster` is a wrapper around `Inner` that provides thread-safe access.
#[derive(Clone, Default)]
pub struct GfsMaster(Arc<RwLock<Inner>>);

/// `Inner` holds the state of the `GfsMaster`.
/// It contains a list of chunk servers and a registry mapping URLs to their
/// respective chunk server addresses.
#[derive(Default)]
struct Inner;

// Implementation of the `Master` trait for `GfsMaster`.
impl Master for GfsMaster {
	async fn lookup(self, _: Context, _url: String) -> SocketAddr {
		todo!()
	}
}

// Implementation of the `ChunkMaster` trait for `GfsMaster`.
impl ChunkMaster for GfsMaster {
	async fn register(self, _: Context, _socket_addr: SocketAddr) -> u64 {
		todo!()
	}
	
	async fn insert(self, _: Context, _sender: u64, _url: String) {
		todo!()
	}
	
	async fn remove(self, _: Context, _url: String) {
		todo!()
	}
}
