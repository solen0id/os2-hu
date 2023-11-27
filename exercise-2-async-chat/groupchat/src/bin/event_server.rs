#![allow(unused_imports)]
use std::{io::{self, Read, Write, ErrorKind}, env};
use mio::{net::TcpListener, Events, Interest, Token, Poll};

fn main() -> io::Result<()> {
	let addr = env::args().nth(1).unwrap_or(String::from("127.0.0.1:8000"));
	let mut listener = TcpListener::bind(addr.parse().unwrap())?;
	
	// TODO: implement an event loop for the server logic
	let mut events = Events::with_capacity(4);
	let mut poll = Poll::new()?;
	let mut clients = vec![];
	let mut buf = [0u8; 256];

	poll.registry().register(
		&mut listener, Token(0), Interest::READABLE
	)?;
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
						clients.push(Some(stream));
						}
				}
				
				Token(n) => {
					let mut bytes = 0;
					let stream = clients[n-1].as_mut().unwrap();
					let disconnect = loop {
						match stream.read(&mut buf) {
							Ok(b) => {
								bytes = b;
								if bytes== 0{
									break true;
								}
							}
							Err(e) => break e.kind() != ErrorKind::WouldBlock
						}
					};
					// diconnect by empty message TOOO add !exit
					if disconnect {
						poll.registry().deregister(stream)?;
						clients[n-1] = None;
					}
					// Send message to all connected clients
					for i in 0..clients.len(){
						// skip disconnected clients
						if(!clients[i].is_none() ){
							let receiver = clients[i].as_mut().unwrap();
							if(receiver.write(&buf[0..bytes]).is_err()){
								// TODO Fehlerbehandlung beim schreiben
								continue;
							}
						}
					}
				}
			}
		}
	}
}
