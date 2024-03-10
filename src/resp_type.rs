use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader},
    net::TcpStream,
};

// https://redis.io/docs/reference/protocol-spec/#resp-protocol-description
#[allow(dead_code)] // TODO: We might actually need them...
#[derive(Debug)]
pub enum RespType {
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
    pub fn parse(reader: &mut BufReader<TcpStream>) -> Result<Self, std::io::Error> {
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
