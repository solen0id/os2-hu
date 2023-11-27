use std::{
    io::{self, Read, Write},
    net, str, thread,
};

fn main() -> io::Result<()> {
    let mut reader = net::TcpStream::connect("127.0.0.1:8000")?;
    let mut buf = [0u8; 256];

    let mut writer = reader.try_clone()?;

    let handle = thread::spawn(move || loop {
        let len = reader.read(&mut buf).unwrap();
        let msg = str::from_utf8(&buf[0..len]).unwrap();

        println!("recv: \"{}\"", msg);

        if msg == "!exit" {
            break;
        }
    });

    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        writer.write(input.trim().as_bytes())?;

        if input.trim() == "!exit" {
            break;
        }
    }

    handle.join().unwrap();
    Ok(())

    // for i in 0..100 {
    //     stream.write(format!("{}", i).as_bytes())?;
    //     let len = stream.read(&mut buf)?;
    //     let msg = str::from_utf8(&buf[0..len]).unwrap();

    //     sleep(Duration::new(1, 0));
    //     println!("recv: \"{}\"", msg);
    // }
}
