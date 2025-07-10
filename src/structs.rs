#[derive(Debug,Clone)]
pub struct HttpClient{
    // indicates wether data is default or modified
    pub read: bool,
    pub info: std::net::SocketAddr,

    pub path: String,
    pub method: String,
    pub version: String,

    pub host: String,
    pub headers: std::collections::HashMap<String,Vec<String>>,
    pub body: Vec<u8>,
}

#[derive(Debug,Clone)]
pub enum Compression{
    Plain,
    Gzip,
}
