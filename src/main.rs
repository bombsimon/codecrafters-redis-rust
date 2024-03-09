use std::{
    io::{BufRead, BufReader, Write},
    net::TcpListener,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut reader = BufReader::new(&stream);

                loop {
                    let mut command = String::new();
                    reader.read_line(&mut command).unwrap();

                    if command.starts_with('*') {
                        println!("TODO: Array");
                        continue;
                    }

                    if command.starts_with('$') {
                        println!("TODO: Bulk string");
                        continue;
                    }

                    match command.to_lowercase().as_str().trim_end() {
                        "ping" => {
                            let buf = "+PONG\r\n".as_bytes();
                            stream.write_all(buf).unwrap();
                        }
                        v => println!("{v} NOT IMPLEMENTED"),
                    }

                    break;
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
