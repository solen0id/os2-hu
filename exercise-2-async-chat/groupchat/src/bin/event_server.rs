#![allow(unused_imports)]
use std::{io::{self, Read, Write, ErrorKind}, env};
use mio::{net::TcpListener, Events, Interest, Token, Poll};

fn main() -> io::Result<()> {
	let addr = env::args().nth(1).unwrap_or(String::from("127.0.0.1:8000"));
	let listener = TcpListener::bind(addr.parse().unwrap())?;
	
	// TODO: implement an event loop for the server logic
	println!("Bound to {:?}", listener);
	Ok(())
}
