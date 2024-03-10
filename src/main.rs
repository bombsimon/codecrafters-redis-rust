// Some good reference for streams
// https://github.com/thepacketgeek/rust-tcpstream-demo

use std::{
    collections::{HashMap, HashSet},
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

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
}

// https://redis.io/docs/reference/protocol-spec/#resp-protocol-description
#[allow(dead_code)] // TODO: We might actually need them...
#[derive(Debug)]
enum RespType {
    SimpleString(String),                    // + (data)
    SimpleError(String),                     // - (data)
    Integer(i64),                            // : (data)
    BulkString(usize, String),               // $ (length, data)
    Array(Vec<RespType>),                    // * (data)
    Null,                                    // _ (empty)
    Boolean(bool),                           // # (data)
    Double(f64),                             // , (data)
    BigNumber(f64),                          // ( (data)
    BulkError(usize, String),                // ! (length, data)
    VerbatimString(usize, String, String),   // = (length, encoding, data)
    Map(usize, HashMap<RespType, RespType>), // % (length, data)
    Set(usize, HashSet<RespType>),           // ~ (length, data)
    Push(usize),                             // > TODO
}

impl RespType {
    fn parse(reader: &mut BufReader<TcpStream>) -> Result<Self, std::io::Error> {
        let mut command = String::new();
        reader.read_line(&mut command)?;

        match command.chars().next() {
            Some('$') => Self::parse_bulk_string(&command, reader),
            Some('*') => Self::parse_array(&command, reader),
            Some(c) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("resp type '{c:?}' not implemented"),
            )),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "empty command",
            )),
        }
    }

    fn parse_size(command: &str) -> Result<usize, std::io::Error> {
        command
            .trim_end()
            .chars()
            .skip(1)
            .collect::<String>()
            .parse::<usize>()
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("failed to parse size: {err}"),
                )
            })
    }

    fn parse_bulk_string(
        command: &str,
        reader: &mut BufReader<TcpStream>,
    ) -> Result<Self, std::io::Error> {
        let size = Self::parse_size(command)?;

        // TODO: Should we just read size and skip \r\n?
        let mut bulk_string = String::new();
        reader.read_line(&mut bulk_string)?;

        Ok(Self::BulkString(size, bulk_string))
    }

    fn parse_array(
        command: &str,
        reader: &mut BufReader<TcpStream>,
    ) -> Result<Self, std::io::Error> {
        let size = Self::parse_size(command)?;
        let mut values = Vec::with_capacity(size);

        for _ in 0..size {
            values.push(Self::parse(reader)?);
        }

        Ok(Self::Array(values))
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
        let resp_type = match RespType::parse(&mut reader) {
            Ok(rt) => rt,
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => return Ok(()),
            err @ Err(_) => err?,
        };

        let command = process_resp_type(&resp_type)?;
        process_command(command, &mut writer)?;
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
                Command::Ping => Ok(Command::Ping),
                Command::Echo(_) => {
                    let rest = &arr[1..]
                        .iter()
                        .filter_map(|x| match x {
                            RespType::BulkString(_, s) => Some(s.trim_end()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" ");

                    Ok(Command::Echo(rest.to_owned()))
                }
            }
        }
        RespType::BulkString(_, command) => match command.to_lowercase().trim_end() {
            "ping" => Ok(Command::Ping),
            // TODO: This won't do - can't hack an empty string. Should use separate enums, I
            // guess?
            "echo" => Ok(Command::Echo("".to_string())),
            // TODO: This should probably not disconnect but the example
            //   `echo -e "ping\nping\n" | redis-cli`
            // seems to disconnect and send three different commands somehow.
            command => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("'{command:?}' not implemented"),
            ))),
        },
        _ => todo!(),
    }
}

fn process_command(
    command: Command,
    writer: &mut TcpStream,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match command {
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
