use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let mut writer = stream.try_clone().unwrap();
                let mut reader = BufReader::new(&stream);

                loop {
                    let mut command = String::new();
                    reader.read_line(&mut command).unwrap();

                    // TODO: Array and Bulk string
                    if command.starts_with('*') || command.starts_with('$') {
                        continue;
                    }

                    match command.to_lowercase().as_str().trim_end() {
                        "ping" => {
                            let buf = "+PONG\r\n".as_bytes();
                            writer.write_all(buf).unwrap();
                        }
                        v => {
                            println!("{v} NOT IMPLEMENTED");

                            // TODO: This should probably not disconnect but the example
                            //   `echo -e "ping\nping\n" | redis-cli`
                            // seems to disconnect and send three different commands somehow.
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
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
