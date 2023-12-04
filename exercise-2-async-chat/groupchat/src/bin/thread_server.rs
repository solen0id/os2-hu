use std::sync::mpsc;

use std::{
    io::{self, Read, Write},
    net::Shutdown,
    net::TcpListener,
    net::TcpStream,
    str, thread,
};

#[derive(Debug)]
struct Message {
    client_id: usize,
    is_connect: bool,
    is_disconnect: bool,
    text: Option<String>,
    stream: Option<TcpStream>,
}

const BUFFER_SIZE: usize = 256;

fn main() -> io::Result<()> {
    let mut conn_count = 0; // incremented for each new connection
    let listener = TcpListener::bind("127.0.0.1:8000")?;
    let (tx, rx) = mpsc::channel::<Message>();

    // spawn a thread to handle incoming messages and broadcast them to all clients
    //
    // This thread keeps a list of clients* in a vector. When a client connects, it adds
    // the client to the list. When a client disconnects, it removes the client from the
    // list. When a message is received, it broadcasts the message to all clients in the
    // list.
    // * = We actually store client TCPStreams wrapped in an Option
    //
    // The thread is spawned with `move` so that it takes ownership of the `rx` channel.
    // The consumer part of the mpsc channel receives from each producer, i.e each
    // thread spawned by our TCPListener.
    thread::spawn(move || {
        let mut clients: Vec<Option<TcpStream>> = Vec::new();

        for msg in &rx {
            // text can be None, so we need to unwrap_or a default value
            let mut text = msg.text.unwrap_or("".to_string());

            // if the client is connecting or disconnecting, we need to update the
            // clients list.
            if msg.is_connect {
                clients.push(msg.stream);
                assert!(clients.len() == msg.client_id + 1);
            } else if msg.is_disconnect && clients[msg.client_id].is_some() {
                let _ = clients
                    .get(msg.client_id)
                    .unwrap()
                    .as_ref()
                    .expect("Client not found")
                    .shutdown(Shutdown::Both);
                clients[msg.client_id] = None;
                text = "disconnected\n".to_string();
                println!("Removed client {} due to disconnect", msg.client_id);
            }

			let single_messages: Vec<&str> = text.split('\n').collect();
			for m in single_messages {
				// Do not broadcast empty messages
				if m.len()==0{
					continue;
				}
				// prepend the client client_id to the message
				let message = format!("Client {}: {}\n", msg.client_id, m);
		
		
				// broadcast the message to all active clients
				// and remove unreachable clients from the list
				let mut failed = Vec::new();
		
				for (client_ix, client) in clients.iter().enumerate() {
					if client.is_none() {
						continue;
					}
		
					let mut client = client.as_ref().unwrap();
		
					match client.write(&message.as_bytes()) {
						Ok(_) => {}
						Err(e) => {
							// if we can't write to the client, it probably has disconnected
							// remove it from the list of clients.
							let _ = client.shutdown(Shutdown::Both);
							failed.push(client_ix);
							println!("Removed client {} due to write error {}", client_ix, e);
						}
					}
				}
		
				// remove failed clients
				for client_id in failed {
					clients[client_id] = None;
				}
			}
        }
    });

    println!("Server: listening on {:?}", listener);
    // This loop accepts incoming connections and spawns a new thread to handle each
    // connection.
    // We read messages from the TCPStream and push them into a string buffer until we
    // receive a newline. Then we forward the message to the channel consumer, so it can
    // broadcast it to all clients.
    // This lets us transmit messages of any length "in one piece", regardless of the
    // size of the TCPStream's internal buffer on the server and client(s).
    while let Ok((mut stream, _)) = listener.accept() {
        let tx_clone = tx.clone();

        thread::spawn(move || {
            let conn_client_id = conn_count.clone();
            let mut buf = [0u8; BUFFER_SIZE];
            let mut text = String::new();

            println!("Server: connected {:?}", stream);

            // Send a connect message to the channel consumer, so it can add the client
            // to the list of recipients for broadcast messages.
            let message = Message {
                client_id: conn_client_id,
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
                while let Ok(len) = stream.read(&mut buf) {
                    if len == 0 {
                        break;
                    }

                    text.push_str(str::from_utf8(&buf[0..len]).unwrap());

                    if text.ends_with("\n") {
                        break;
                    }
                }
				if text.trim() == "!exit" {
					break;
				}

                // Forward the message to the channel consumer, so it can broadcast it
                // to all clients.
                let message = Message {
                    client_id: conn_client_id,
                    is_connect: false,
                    is_disconnect: false,
                    text: Some(text.clone()),
                    stream: None,
                };

                // clear the string buffer for the next message
                text.clear();

                if tx_clone.send(message).is_err() {
                    break;
                }
            }

            // At this point, the client has disconnected. Send a disconnect message to
            // the channel consumer, so it can remove the client from the list of
            // recipients for broadcast messages.
            let message = Message {
                client_id: conn_client_id,
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
