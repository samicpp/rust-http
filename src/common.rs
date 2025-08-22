use tokio;
// use crate::structs;
use std::io;
use std::fmt;


// Http

#[allow(async_fn_in_trait)]
pub trait HttpSocket{
    fn new(socket: tokio::net::TcpStream, addr: std::net::SocketAddr)->Self;
    
    fn set_header(&mut self, name: &str, value: &str)->HttpResult<()>;
    fn remove_header(&mut self, name: &str)->HttpResult<Vec<String>>;
    fn set_compression(&mut self, new_compression: Compression)->HttpResult<()>;
    
    async fn get_client(&mut self)->HttpResult<HttpClient>;

    async fn send_head(&mut self)->HttpResult<()>;
    async fn close(&mut self, bytes: &[u8])->HttpResult<()>;
    async fn write(&mut self, bytes: &[u8])->HttpResult<()>;
}

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

// Errors

pub type HttpResult<T> = Result<T,HttpError>;

#[derive(Debug)]
pub enum HttpError{
    Io(io::Error),
    ConnectionClosed,

    HeadersSent,
    InvalidHeader,

    Invalid,
}

impl fmt::Display for HttpError{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self{
            Self::Io(io_err)=>write!(f,"I/O Error: {}",io_err),
            Self::ConnectionClosed=>write!(f,"Connection is closed"),
            Self::HeadersSent=>write!(f,"Headers already sent"),
            Self::InvalidHeader=>write!(f,"Cannot use this header"),
            Self::Invalid=>write!(f,"Invalid invocation"),
        }
    }
}

impl std::error::Error for HttpError{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self{
            Self::Io(err)=>Some(err),
            _=>None,
        }
    }
}

impl From<io::Error> for HttpError{
    fn from(err: io::Error)->Self{
        Self::Io(err)
    }
}