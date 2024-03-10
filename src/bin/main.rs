// Some good reference for streams
// https://github.com/thepacketgeek/rust-tcpstream-demo

fn main() {
    let server = redis_starter_rust::server::Server::new("127.0.0.1:6379");
    server.serve_forever();
}
