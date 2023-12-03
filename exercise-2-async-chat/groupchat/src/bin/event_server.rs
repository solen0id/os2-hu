#![allow(unused_imports)]
use mio::{net::TcpListener, Events, Interest, Poll, Token};
use std::{
	env,
	io::{self, ErrorKind, Read, Write},
	net::Shutdown,
	str,
};
use futures::future::err;
use mio::net::TcpStream;

fn main() -> io::Result<()> {
	let mut events = Events::with_capacity(1024);
	let mut poll = Poll::new()?;
	let mut clients: Vec<Option<TcpStream>> = Vec::new();
	let mut messages: Vec<String> = Vec::new();
	let mut buf = [0u8; 256];

	let addr = env::args().nth(1).unwrap_or(String::from("127.0.0.1:8000"));
	let mut listener = TcpListener::bind(addr.parse().unwrap())?;
	println!("Server: listening on {:?}", listener);

	poll.registry()
		.register(&mut listener, Token(0), Interest::READABLE)?;

	loop {
		poll.poll(&mut events, None)?;

		for event in events.iter() {
			match event.token() {
				Token(0) => {
					while let Ok((mut stream, _)) = listener.accept() {
						let mut token_position = clients.len()+1;
						// Check if we can reuse a previous ID
						for i in 0..clients.len() {
							if clients[i].is_none() {
								token_position = i+1;
							}
						}
						println!("Server: connected {:?}", stream);
						poll.registry().register(
							&mut stream,
							Token(token_position),
							Interest::READABLE,
						)?;
						// Create new slot
						if token_position>clients.len(){
							clients.push(Some(stream));
							messages.push(String::new());

						}
						// Reuse previous slot
						else {
							clients[token_position-1] = Some(stream);
							messages[token_position-1].clear();
						}
					}
				}

				Token(n) => {
					let mut input = String::new();
					let disconnect = loop{
						match clients[n-1].as_mut().unwrap().read(&mut buf) {
							Ok(b) => {
								if b == 0 {
									break true;
								}
								input.push_str(str::from_utf8(&buf[0..b]).unwrap());
								// send message to all clients when it is complete
								if input.ends_with("\n"){
									if input.trim() == "!exit" {
										break true;
									}
									input = format!("Client {}: {}", n-1, input);
									for i in 0..clients.len() {
										// skip disconnected clients
										if !clients[i].is_none() {
											let receiver = clients[i].as_mut().unwrap();
											if receiver.write(input.as_bytes()).is_err() {
												let _ = receiver.shutdown(Shutdown::Both);
												continue;
											}
										}
									}
									input.clear();
								}
							}
							Err(e) => {
								// Close connection in case of connection errors
								if  e.kind() == ErrorKind::ConnectionReset ||
									e.kind() == ErrorKind::ConnectionAborted ||
									e.kind() == ErrorKind::NotConnected {
									break true;
								}
								break e.kind() != ErrorKind::WouldBlock
							}
						}
					};
					if disconnect {
						// Try to send !exit to client
						input = format!("!exit\n");
						let _ = clients[n - 1].as_mut().unwrap().write(input.as_bytes());
						// Delete connection
						println!("Server: disconnected {:?}", clients[n-1]);
						poll.registry().deregister(clients[n-1].as_mut().unwrap())?;
						clients[n - 1] = None;
						// Send disconnected message to other clients
						input = format!("Client {} disconnected\n", n-1);
						for i in 0..clients.len() {
							// skip disconnected clients
							if !clients[i].is_none() {
								let receiver = clients[i].as_mut().unwrap();
								let _ = receiver.write(input.as_bytes());
							}
						}
					}
				}
			}
		}
	}
}
