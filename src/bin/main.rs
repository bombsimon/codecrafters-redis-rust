// Some good reference for streams
// https://github.com/thepacketgeek/rust-tcpstream-demo

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let server = redis_starter_rust::server::Server::new("127.0.0.1:6379");
    server.serve_forever();
}
