use tokio;
use crate::client;
pub trait HttpSocket{
    fn new(socket: tokio::net::TcpStream, addr: std::net::SocketAddr)->Self;
    
    fn set_header(&mut self, name: &str, value: &str)->bool;
    fn remove_header(&mut self, name: &str)->Option<Vec<String>>;
    
    async fn get_client(&mut self)->std::io::Result<&client::HttpClient>;

    async fn send_head(&mut self)->std::io::Result<()>;
    async fn close(&mut self, bytes: &[u8])->std::io::Result<()>;
    async fn write(&mut self, bytes: &[u8])->std::io::Result<()>;
}