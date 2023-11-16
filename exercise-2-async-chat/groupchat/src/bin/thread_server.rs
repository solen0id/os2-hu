use std::{io, env, net::TcpListener};

fn main() -> io::Result<()> {
	let addr = env::args().nth(1).unwrap_or(String::from("127.0.0.1:8000"));
	let listener = TcpListener::bind(addr)?;
	
	// TODO: implement threads for the server logic
	println!("Bound to {:?}", listener);
	for stream in listener.incoming() {
		println!("Connection attempt by {:?}", stream?);
	}
	
	Ok(())
}
