use tokio;
use crate::structs;
use std::io;


#[allow(async_fn_in_trait)]
pub trait HttpSocket{
    fn new(socket: tokio::net::TcpStream, addr: std::net::SocketAddr)->Self;
    
    fn set_header(&mut self, name: &str, value: &str)->io::Result<()>;
    fn remove_header(&mut self, name: &str)->Option<Vec<String>>;
    fn set_compression(&mut self, new_compression: structs::Compression)->io::Result<()>;
    
    async fn get_client(&mut self)->io::Result<structs::HttpClient>;

    async fn send_head(&mut self)->io::Result<()>;
    async fn close(&mut self, bytes: &[u8])->io::Result<()>;
    async fn write(&mut self, bytes: &[u8])->io::Result<()>;
}
