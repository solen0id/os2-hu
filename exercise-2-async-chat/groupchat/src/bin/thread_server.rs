use std::sync::mpsc;

use std::{
    io::{self, Read, Write},
    net::TcpListener,
    net::TcpStream,
    thread,
};

#[derive(Debug)]
struct Message {
    id: usize,
    is_connect: bool,
    is_disconnect: bool,
    text: Option<String>,
    stream: Option<TcpStream>,
}

const CLIENT_CAPACITY: usize = 100;

fn main() -> io::Result<()> {
    let mut conn_count = 0; // incremented for each new connection
    let listener = TcpListener::bind("127.0.0.1:8000")?;
    let (tx, rx) = mpsc::channel::<Message>();

    // spawn a thread to handle incoming messages and broadcast them to all clients
    //
    // This thread keeps a list of clients in a vector. When a client connects, it adds
    // the client to the list. When a client disconnects, it removes the client from the
    // list. When a message is received, it broadcasts the message to all clients in the
    // list.
    //
    // The thread is spawned with `move` so that it takes ownership of the `rx` channel.
    // The consumer part of the mpsc channel receives from each producer, i.e each
    // thread spawned by our TCPListener.
    thread::spawn(move || {
        let mut clients: Vec<Option<TcpStream>> = Vec::with_capacity(CLIENT_CAPACITY);
        for _ in 0..CLIENT_CAPACITY {
            clients.push(None);
        }

        for msg in &rx {
            // text can be None, so we need to unwrap_or a default value
            let mut text = msg.text.unwrap_or("".to_string());

            // if the client is connecting or disconnecting, we need to update the
            // clients list.
            if msg.is_connect {
                clients[msg.id] = msg.stream;
            } else if (msg.is_disconnect || text == "!exit") && clients[msg.id].is_some() {
                clients
                    .get(msg.id)
                    .unwrap()
                    .as_ref()
                    .expect("Client not found")
                    .shutdown(std::net::Shutdown::Both)
                    .unwrap();
                clients[msg.id] = None;
                text = format!("Client {} disconnected", msg.id);
            }

            // Do not broadcast empty messages
            if text == "" {
                continue;
            }

            // broadcast the message to all active clients
            for client in &clients {
                if client.is_none() {
                    continue;
                }
                let mut client = client.as_ref().unwrap();
                client.write(&text.as_bytes()).unwrap();
            }
        }
    });

    println!("Server: listening on {:?}", listener);
    // This loop accepts incoming connections and spawns a new thread to handle each
    // connection.
    while let Ok((mut stream, _)) = listener.accept() {
        let tx_clone = tx.clone();

        thread::spawn(move || {
            let conn_id = conn_count.clone();
            let mut buf = [0u8; 256];

            println!("Server: connected {:?}", stream);

            // Send a connect message to the channel consumer, so it can add the client
            // to the list of recipients for broadcast messages.
            let message = Message {
                id: conn_id,
                is_connect: true,
                is_disconnect: false,
                text: None,
                stream: Some(stream.try_clone().unwrap()),
            };
            tx_clone.send(message).unwrap();

            // Read messages from the client and send them to the channel consumer.
            // If the client disconnects, send a disconnect message to the channel
            // consumer.
            loop {
                let len = match stream.read(&mut buf) {
                    Ok(0) => break,
                    Ok(len) => len,
                    Err(_) => break,
                };

                // Forward the message to the channel consumer, so it can broadcast it
                // to all clients.
                let message = Message {
                    id: conn_id,
                    is_connect: false,
                    is_disconnect: false,
                    text: Some(String::from_utf8((&buf[0..len]).to_vec()).unwrap()),
                    stream: None,
                };

                if tx_clone.send(message).is_err() {
                    break;
                }
            }

            // At this point, the client has disconnected. Send a disconnect message to
            // the channel consumer, so it can remove the client from the list of
            // recipients for broadcast messages.
            let message = Message {
                id: conn_id,
                is_connect: false,
                is_disconnect: true,
                text: None,
                stream: None,
            };

            tx_clone.send(message).unwrap();
            println!("Server: disconnected {:?}", stream);
        });

        conn_count += 1;
    }

    Ok(())
}
