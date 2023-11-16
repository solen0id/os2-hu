use mio::{net::TcpListener, Events, Token, Interest, Poll};
use std::{io::{self, Read, Write, ErrorKind}};

fn main() -> io::Result<()> {
	let mut listener = TcpListener::bind("127.0.0.1:8000".parse().unwrap())?;
	let mut events = Events::with_capacity(4);
	let mut poll = Poll::new()?;
	let mut clients = vec![];
	let mut buf = [0u8; 256];
	
	poll.registry().register(
		&mut listener, Token(0), Interest::READABLE
	)?;
	
	println!("Server: listening on {:?}", listener);
	
	loop {
		poll.poll(&mut events, None)?;
		
		for event in events.iter() {
			match event.token() {
				Token(0) => {
					while let Ok((mut stream, _)) = listener.accept() {
						poll.registry().register(
							&mut stream, Token(clients.len() + 1),
							Interest::READABLE
						)?;
						
						println!("Server: connected {:?}", stream);
						clients.push(Some(stream));
					}
				}
				Token(n) => {
					let stream = clients[n-1].as_mut().unwrap();
					let disconnect = loop {
						match stream.read(&mut buf) {
							Ok(bytes) =>
								if bytes == 0 || stream.write(&buf[0..bytes]).is_err() {
									break true;
								}
							Err(e) => break e.kind() != ErrorKind::WouldBlock
						}
					};
					
					if disconnect {
						println!("Server: disconnected {:?}", stream);
						poll.registry().deregister(stream)?;
						clients[n-1] = None;
					}
				}
			}
		}
	}
}
