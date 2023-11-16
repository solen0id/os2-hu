use std::io;
use async_std::{prelude::*, net::TcpListener};
use futures::{task::SpawnExt, executor::LocalPool};

fn main() -> io::Result<()> {
	let mut exec = LocalPool::new();
	let spawner = exec.spawner();
	
	exec.run_until(async {
		let listener = TcpListener::bind("127.0.0.1:8000").await?;
		
		println!("Server: listening on {:?}", listener);
		
		while let Ok((mut stream, _)) = listener.accept().await {
			spawner.spawn(async move {
				let mut buf = [0u8; 256];
				
				println!("Server: connected {:?}", stream);
				while let Ok(bytes) = stream.read(&mut buf).await {
					if bytes == 0 || stream.write(&buf[0..bytes]).await.is_err() {
						break;
					}
				}
				println!("Server: disconnected {:?}", stream);
			}).unwrap();
		}
		
		Ok(())
	})
}
