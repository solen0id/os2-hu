use spsc::*;
use std::thread;

fn main() {
    let (px, cx) = channel();

    thread::spawn(move || {
        for i in 0..10 {
            px.send(format!("Ping {}", i)).unwrap();
        }
    });

    for result in &cx {
        println!("recv: {}", result);
    }

    println!("Received all messages: {}", cx.recv().is_err());
}
