#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Set(String, String),
    Get(String),
}
