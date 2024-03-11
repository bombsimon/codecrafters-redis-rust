#[derive(Debug)]
pub enum Command {
    Literal(String),
    Ping,
    Echo(String),
    Set(String, String),
    Get(String),
}

impl Command {
    pub fn literal_value(self) -> Result<String, std::io::Error> {
        match self {
            Self::Literal(v) => Ok(v),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "not a literal command",
            )),
        }
    }
}
