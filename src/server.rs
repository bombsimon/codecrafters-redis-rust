use crate::command::Command;
use crate::resp_type::RespType;

use std::{
    collections::HashMap,
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

pub struct Server {
    listener: TcpListener,
    storage: Arc<Mutex<HashMap<String, String>>>,
}

impl Server {
    pub fn new(addr: &str) -> Arc<Self> {
        Arc::new(Self {
            listener: TcpListener::bind(addr).unwrap(),
            storage: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn serve_forever(&self) {
        for stream in self.listener.incoming() {
            let s = self.storage.clone();
            thread::spawn(|| handle_request(stream, s));
        }
    }
}

fn handle_request(
    stream: Result<TcpStream, std::io::Error>,
    storage: Arc<Mutex<HashMap<String, String>>>,
) {
    match stream {
        Ok(stream) => match process_request(stream, storage) {
            Ok(_) => (),
            Err(err) => println!("error handlign request: {err:?}"),
        },
        Err(e) => {
            println!("error: {}", e);
        }
    }
}

fn process_request(
    stream: TcpStream,
    storage: Arc<Mutex<HashMap<String, String>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    loop {
        let resp_type = match RespType::parse(&mut reader) {
            Ok(rt) => rt,
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => return Ok(()),
            err @ Err(_) => err?,
        };

        let command = process_resp_type(&resp_type)?;
        process_command(command, storage.clone(), &mut writer)?;
    }
}

fn process_resp_type(
    resp_type: &RespType,
) -> Result<Command, Box<dyn std::error::Error + Send + Sync>> {
    match resp_type {
        RespType::Array(arr) if !arr.is_empty() => {
            // safety: We just checked for length.
            let command = &arr[0];

            match process_resp_type(command)? {
                Command::Literal(s) if s.to_lowercase() == "ping" => Ok(Command::Ping),
                Command::Literal(s) if s.to_lowercase() == "echo" => {
                    let arg = process_resp_type(&arr[1])?.literal_value()?;
                    Ok(Command::Echo(arg))
                }
                Command::Literal(s) if s.to_lowercase() == "set" => {
                    let key = process_resp_type(&arr[1])?.literal_value()?;
                    let value = process_resp_type(&arr[2])?.literal_value()?;
                    Ok(Command::Set(key, value))
                }
                Command::Literal(s) if s.to_lowercase() == "get" => {
                    let key = process_resp_type(&arr[1])?.literal_value()?;
                    Ok(Command::Get(key))
                }
                v => Ok(v),
            }
        }
        RespType::BulkString(_, command) => Ok(Command::Literal(command.trim_end().to_string())),
        _ => todo!(),
    }
}

fn process_command(
    command: Command,
    storage: Arc<Mutex<HashMap<String, String>>>,
    writer: &mut TcpStream,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match command {
        Command::Literal(value) => {
            let err = format!("-ERROR '{value}' not implemented\r\n");
            let buf = err.as_bytes();
            writer.write_all(buf)?;
        }
        Command::Ping => {
            let buf = "+PONG\r\n".as_bytes();
            writer.write_all(buf)?;
        }
        Command::Echo(response) => {
            let size = response.len();
            let reply = format!("${size}\r\n{response}\r\n");
            let buf = reply.as_bytes();

            writer.write_all(buf)?;
        }
        Command::Set(key, value) => {
            let mut s = storage.lock().unwrap();
            let v = s.entry(key).or_default();
            *v = value;

            let buf = "+OK\r\n".as_bytes();
            writer.write_all(buf)?;
        }
        Command::Get(key) => {
            let s = storage.lock().unwrap();
            match s.get(&key) {
                Some(value) => {
                    let size = value.len();
                    let reply = format!("${size}\r\n{value}\r\n");
                    let buf = reply.as_bytes();

                    writer.write_all(buf)?;
                }
                None => {
                    let buf = "$-1\r\n".as_bytes();
                    writer.write_all(buf)?;
                }
            };
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
