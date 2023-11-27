use std::{
    io::{self, Read, Write},
    net, str, thread,
};

const BUFFER_SIZE: usize = 256;

fn main() -> io::Result<()> {
    let mut reader = net::TcpStream::connect("127.0.0.1:8000")?;
    let mut buf = [0u8; BUFFER_SIZE];

    let mut writer = reader.try_clone()?;

    // Spawn a thread to read incoming messages from the server and print to stdout
    let handle = thread::spawn(move || loop {
        let len = match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(len) => len,
            Err(_) => break,
        };

        let msg = match str::from_utf8(&buf[0..len]) {
            Ok(msg) => msg,
            Err(_) => break,
        };

        println!("{}", msg);
    });

    // Read messages from stdin and send them to the server forever, or until the
    // user types "!exit".
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        // clear the last line of input,
        // so we don't print the user's input twice
        print!("\x1b[1A\x1b[2K");

        writer.write(input.trim().as_bytes())?;

        if input.trim() == "!exit" {
            break;
        }
    }

    match handle.join() {
        Ok(_) => {
            println!("Client exited successfully");
            Ok(())
        }
        Err(e) => {
            println!("Client exited with error: {:?}", e);
            Ok(())
        }
    }
}
