use std::{
    io::{self, Read, Write},
    net, str, thread,
};

const BUFFER_SIZE: usize = 256;

fn main() -> io::Result<()> {
    let mut reader = net::TcpStream::connect("127.0.0.1:8000")?;
    let mut buf = vec![0u8; BUFFER_SIZE];
    let mut output = String::new();

    let mut writer = reader.try_clone()?;

    // Spawn a thread to read incoming messages from the server and print to stdout
    //
    // We use a string buffer to store the output from the server. We read from the
    // reader into the buffer until we reach a newline, then print buffered string
    // to stdout.
    let handle = thread::spawn(move || loop {
        let mut finished = false;

        while let Ok(len) = reader.read(&mut buf) {
            if len == 0 {
                finished = true;
                break;
            }

            output.push_str(str::from_utf8(&buf[0..len]).unwrap());

            if output.ends_with("\n") {
                break;
            }
        }

        if finished {
            break;
        }

        print!("{}", output);
        io::stdout().flush().unwrap();
        output.clear();
    });

    // Read messages from stdin and send them to the server forever, or until the
    // user types "!exit".
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        // clear the last line of input,
        // so we don't print the user's input twice
        print!("\x1b[1A\x1b[2K");
        writer.write(input.as_bytes())?;

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
