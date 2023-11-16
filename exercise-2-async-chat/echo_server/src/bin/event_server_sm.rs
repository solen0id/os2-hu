use mio::{net::TcpListener, Events, Token, Interest, Poll};
use std::{io::{self, Read, Write, ErrorKind}};

struct Client {
	state: usize,
	stream: mio::net::TcpStream,
	buf: [u8; 256]
}

fn main() -> io::Result<()> {
	let mut listener = TcpListener::bind("127.0.0.1:8000".parse().unwrap())?;
	let mut events = Events::with_capacity(4);
	let mut poll = Poll::new()?;
	let mut clients = vec![];
	
	poll.registry().register(&mut listener, Token(0), Interest::READABLE)?;
	
	println!("Server: listening on {:?}", listener);
	
	loop {
		poll.poll(&mut events, None)?;
		
		for event in events.iter() {
			match event.token() {
				Token(0) => {
					while let Ok((mut stream, _)) = listener.accept() {
						poll.registry().register(
							&mut stream, 
							Token(clients.len() + 1),
							Interest::READABLE
						)?;
						
						clients.push(Some((Client {
							state: 0, stream, buf: [0; 256]
						}, |Client {state, stream, buf}: &mut _|
							loop {
								match state {
									0 => {
										println!("Server: connected {:?}", stream);
										*state = 1;
									}
									1 => {
										match stream.read(buf) {
											Ok(b) => {
												if b == 0 || stream.write(&buf[0..b]).is_err() {
													*state = 2;
												}
											}
											Err(e) => {
												if e.kind() != ErrorKind::WouldBlock {
													*state = 2;
												} else {
													break true;
												}
											}
										}
									}
									2 => {
										println!("Server: disconnected {:?}", stream);
										break false;
									}
									_ => unreachable!()
								}
							}
						)));
					}
				}
				Token(n) => {
					let client = clients[n-1].as_mut().unwrap();
					if !client.1(&mut client.0) {
						poll.registry().deregister(&mut client.0.stream)?;
						clients[n-1] = None;
					}
				}
			}
		}
	}
}
