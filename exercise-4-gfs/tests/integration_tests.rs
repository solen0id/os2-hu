//! Contains integration tests for the simplified Google File System.
//! 
//! The two test-cases currently implemented are:
//! 1. storing key-value-pairs on a single chunk server,
//! 2. storing key-value-pairs on multiple chunk servers.
//! 
//! I should probably add tests for removal but I'm low on time. Sorry guys!

use std::{
	future::{Future, ready},
	io,
	net::Ipv6Addr
};

use futures::StreamExt;
use proptest::{collection::hash_map, prop_assert_eq, proptest};
use tarpc::{
	context,
	serde_transport::tcp::{connect, listen},
	server::{BaseChannel, Channel},
	tokio_serde::formats::Json
};

use gfs_lite::{Chunk, chunk::ChunkServer, ChunkClient, ChunkMaster, ChunkMasterClient, Master, master::GfsMaster, MasterClient};

/// Runs a future to completion.
#[inline]
fn block_on<F: Future>(future: F) -> F::Output {
	use tokio::runtime::Builder;
	Builder::new_current_thread().enable_all().build().unwrap().block_on(future)
}

/// Spawns a new listener for client-side communication with the GFS master.
async fn create_master_client(master: GfsMaster) -> io::Result<MasterClient> {
	// pick any port to prevent collisions during testing
	let listener = listen((Ipv6Addr::LOCALHOST, 0), Json::default).await?;
	let addr = listener.local_addr();
	
	// spawn the listener
	tokio::spawn(
		listener
			.filter_map(|r| ready(r.ok()))
			.map(move |transport| BaseChannel::with_defaults(transport)
				.execute(Master::serve(master.clone()))
				.for_each(|future| async move { tokio::spawn(future); }))
			.buffer_unordered(10)
			.for_each(|_| ready(()))
	);
	
	// return a remote-object to the GFS master for the client
	Ok(MasterClient::new(Default::default(), connect(addr, Json::default).await?).spawn())
}

/// Spawns a new listener for chunk-server-side communication with the GFS master.
async fn create_chunk_master(master: GfsMaster) -> io::Result<ChunkMasterClient> {
	// pick any port to prevent collisions, as above
	let listener = listen((Ipv6Addr::LOCALHOST, 0), Json::default).await?;
	let addr = listener.local_addr();
	
	// spawn the listener
	tokio::spawn(
		listener
			.filter_map(|r| ready(r.ok()))
			.map(move |transport| BaseChannel::with_defaults(transport)
				.execute(ChunkMaster::serve(master.clone()))
				.for_each(|future| async move { tokio::spawn(future); }))
			.buffer_unordered(10)
			.for_each(|_| ready(()))
	);
	
	// return a remote-object to the GFS master for the chunk server
	Ok(ChunkMasterClient::new(Default::default(), connect(addr, Json::default).await?).spawn())
}

/// Spawns a new listener for client-side communication with a chunk server.
async fn create_chunk_client(chunk_master: ChunkMasterClient) -> io::Result<ChunkClient> {
	// pick any port to prevent collisions
	let listener = listen((Ipv6Addr::LOCALHOST, 0), Json::default).await?;
	let addr = listener.local_addr();
	
	// register the chunk server with the master
	let id = chunk_master.register(context::current(), addr).await.unwrap();
	
	// create the chunk server
	let server = ChunkServer::new(chunk_master, id);
	
	// spawn the listener
	tokio::spawn(
		listener
			.filter_map(|r| ready(r.ok()))
			.map(move |transport| BaseChannel::with_defaults(transport)
				.execute(server.clone().serve())
				.for_each(|future| async move { tokio::spawn(future); }))
			.buffer_unordered(10)
			.for_each(|_| ready(()))
	);
	
	// return a remote-object to the spawn server for the client
	Ok(ChunkClient::new(Default::default(), connect(addr, Json::default).await?).spawn())
}

proptest! {
	#[test]
	fn single_chunk_server(hash_map in hash_map("[a-z]*", "[a-z]*", 1..10)) {
		block_on(async move {
			// create one GFS-master and one chunk server
			let gfs_master = GfsMaster::default();
			let chunk_master = create_chunk_master(gfs_master).await?;
			let chunk_client = create_chunk_client(chunk_master).await?;
			
			// insert all of the entries into the one chunk server (no lookup)
			for (key, val) in hash_map.iter() {
				chunk_client.set(context::current(), key.clone(), Some(val.clone()))
					.await.expect("insertion of key-value-pair successful");
			}
			
			// check if they are all there
			for (key, val) in hash_map.iter() {
				prop_assert_eq!(
					chunk_client.get(context::current(), key.clone()).await.expect("lookup of key-value-pair successful"),
					Some(val.clone())
				);
			}
			
			Ok(())
		})?;
	}
	
	#[test]
	fn multi_chunk_server(chunk_servers in 2..10, hash_map in hash_map("[a-z]*", "[a-z]*", 10..20)) {
		block_on(async move {
			// create one GFS-master
			let gfs_master = GfsMaster::default();
			let chunk_master = create_chunk_master(gfs_master.clone()).await?;
			let master_client = create_master_client(gfs_master.clone()).await?;
			
			// create multiple chunk server
			for _ in 0..chunk_servers {
				create_chunk_client(chunk_master.clone()).await?;
			}
			
			// perform a lookup in the chunk master before inserting the
			// key-value-pair into the corresponding chunk server
			for (key, val) in hash_map.iter() {
				let chunk_addr = master_client.lookup(context::current(), key.clone()).await?;
				let chunk_client = ChunkClient::new(Default::default(), connect(chunk_addr, Json::default).await?).spawn();
				chunk_client.set(context::current(), key.clone(), Some(val.clone()))
					.await.expect("insertion of key-value-pair successful");
			}
			
			// check if they are all there
			for (key, val) in hash_map.iter() {
				let chunk_addr = master_client.lookup(context::current(), key.clone()).await?;
				let chunk_client = ChunkClient::new(Default::default(), connect(chunk_addr, Json::default).await?).spawn();
				
				prop_assert_eq!(
					chunk_client.get(context::current(), key.clone()).await.expect("lookup of key-value-pair successful"),
					Some(val.clone())
				);
			}
			
			Ok(())
		})?;
	}
}
