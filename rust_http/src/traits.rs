pub trait HttpSocket{
    fn new(bufsize: usize, socket: tokio::net::TcpStream, addr: std::net::SocketAddr)->Self;
}