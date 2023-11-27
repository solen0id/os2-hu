use std::sync::mpsc;

use std::{
    io::{self, Read, Write},
    net::TcpListener,
    thread,
};

fn main() -> io::Result<()> {
    let mut conn_count = 0;
    let mut clients = vec![];

    let listener = TcpListener::bind("127.0.0.1:8000")?;
    let (tx, rx) = mpsc::channel();

    println!("Server: listening on {:?}", listener);

    while let Ok((mut stream, _)) = listener.accept() {
        let tx_clone = tx.clone();
        let stream_clone = stream.try_clone()?;
        clients.push(stream_clone);

        thread::spawn(move || {
            let conn_id = conn_count.clone();
            let mut buf = [0u8; 256];

            println!("Server: connected {:?}", stream);

            loop {
                let len = match stream.read(&mut buf) {
                    Ok(0) => break,
                    Ok(len) => len,
                    Err(_) => break,
                };
                let msg = String::from_utf8((&buf[0..len]).to_vec()).unwrap();

                if tx_clone.send(msg).is_err() {
                    break;
                }
            }

            println!("Server: disconnected {:?}", stream);
        });

        conn_count += 1;

        for msg in &rx {
            for mut client in &clients {
                client.write(msg.as_bytes()).unwrap();
            }
        }
    }

    Ok(())
}
