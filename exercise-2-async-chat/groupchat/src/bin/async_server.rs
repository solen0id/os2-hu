#![allow(unused_imports)]
use std::{io, env};
use async_std::{prelude::*, net::TcpListener};
use futures::{task::SpawnExt, executor::{LocalPool, LocalSpawner}};

fn main() -> io::Result<()> {
	let _addr = env::args().nth(1).unwrap_or(String::from("127.0.0.1:8000"));
	// TODO: implement asynchronous server logic
	Ok(())
}
