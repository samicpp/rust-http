pub trait HttpSocket{
    fn new(socket: tokio::net::TcpStream, addr: std::net::SocketAddr)->Self;
    fn set_header(&mut self, name: &str, value: &str)->bool;
    fn remove_header(&mut self, name: &str)->Option<Vec<String>>;
    async fn send_head(&mut self)->std::io::Result<()>;
}