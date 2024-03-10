// Some good reference for streams
// https://github.com/thepacketgeek/rust-tcpstream-demo

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        thread::spawn(|| handle_request(stream));
    }
}

fn handle_request(stream: Result<TcpStream, std::io::Error>) {
    match stream {
        Ok(stream) => match process_request(stream) {
            Ok(_) => (),
            Err(err) => println!("error handlign request: {err:?}"),
        },
        Err(e) => {
            println!("error: {}", e);
        }
    }
}

fn process_request(stream: TcpStream) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    loop {
        let mut command = String::new();
        reader.read_line(&mut command)?;

        if command.is_empty() {
            break;
        }

        // TODO: Array and Bulk string
        if command.starts_with('*') || command.starts_with('$') {
            continue;
        }

        match command.to_lowercase().as_str().trim_end() {
            "ping" => {
                let buf = "+PONG\r\n".as_bytes();
                writer.write_all(buf)?;
            }
            v => {
                // TODO: This should probably not disconnect but the example
                //   `echo -e "ping\nping\n" | redis-cli`
                // seems to disconnect and send three different commands somehow.
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("'{v}' not implemented"),
                )));
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn dump_stream(stream: &std::net::TcpStream) {
    let mut tmp = stream.try_clone().unwrap();
    let mut received: Vec<u8> = vec![];
    let mut rx_bytes = [0u8; 1024];
    let bytes_read = tmp.read(&mut rx_bytes).unwrap();
    received.extend_from_slice(&rx_bytes[..bytes_read]);

    println!("{}", std::str::from_utf8(&received).unwrap());
}
