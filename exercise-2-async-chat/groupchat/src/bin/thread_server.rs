use std::{
    io::{self, Read, Write},
    net::TcpListener,
    thread,
};

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8000")?;

    println!("Server: listening on {:?}", listener);
    while let Ok((mut stream, _)) = listener.accept() {
        thread::spawn(move || {
            let mut buf = [0u8; 256];

            println!("Server: connected {:?}", stream);
            while let Ok(bytes) = stream.read(&mut buf) {
                if bytes == 0 || stream.write(&buf[0..bytes]).is_err() {
                    break;
                }
            }
            println!("Server: disconnected {:?}", stream);
        });
    }

    Ok(())
}
