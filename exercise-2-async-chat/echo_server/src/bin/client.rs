use std::thread::sleep;
use std::time::Duration;

use std::{str, net, io::{self, Read, Write}};

fn main() -> io::Result<()> {
	let mut stream = net::TcpStream::connect("127.0.0.1:8000")?;
	let mut buf = [0u8; 256];
	
	for i in 0..100 {
		stream.write(format!("{}", i).as_bytes())?;
		let len = stream.read(&mut buf)?;
		let msg = str::from_utf8(&buf[0..len]).unwrap();
		
		sleep(Duration::new(1, 0));
		println!("recv: \"{}\"", msg);
	}
	
	Ok(())
}
