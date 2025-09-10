// use tokio::net;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, /*AsyncWriteExt*/};
use std::collections::HashMap;
// use std::fmt::write;
// use crate::structs;
use std::io;
use std::fmt;
use std::sync::Arc;

use crate::http1::Http1Socket;
use crate::http2::Http2Session;
use crate::websocket::WebSocket;
// use std::pin::Pin;


// # Http

// #[allow(async_fn_in_trait)]
pub trait HttpSocket{
    type Stream: Stream;
    // fn new(socket: S, addr: std::net::SocketAddr)->Self;
    
    fn set_header(&mut self, name: &str, value: &str)->HttpResult<()>;
    fn remove_header(&mut self, name: &str)->HttpResult<Vec<String>>;
    fn set_compression(&mut self, new_compression: Compression)->HttpResult<()>;
    fn set_status(&mut self, status: u16, msg: String)->HttpResult<()>;
    
    fn read_client<'_a>(&'_a mut self)->impl Future<Output = Result<&'_a HttpClient, HttpError>>;
    fn get_client<'_a>(&'_a mut self)->impl Future<Output = Result<&'_a HttpClient, HttpError>>;

    fn send_head(&mut self)->impl Future<Output = HttpResult<()>>;
    fn close(&mut self, bytes: &[u8])->impl Future<Output = HttpResult<()>>;
    fn write(&mut self, bytes: &[u8])->impl Future<Output = HttpResult<()>>;

    // fn websocket(self)->impl Future<Output = HttpResult<WebSocket<Self::Stream>>>;

    fn r#type(&self)->HttpType;
    fn get_http1(self)->Http1Socket<Self::Stream>;
    // fn get_http2(&self)->Arc<Http2Session<'a,Self::Stream>>;
}

// type DynSocket=Box<dyn HttpSocket>;

pub trait HttpConstructor<S:Stream>{
    fn new(socket: S, addr: std::net::SocketAddr)->Self;
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

impl HttpClient{
    pub fn empty()->Self{
        Self {
            read: false,
            info: std::net::SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::new(0,0,0,0),0)),
            path: String::new(),
            method: String::new(),
            version: String::new(),
            host: String::new(),
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }
}

impl Default for HttpClient{
    fn default() -> Self {
        Self {
            read: false,
            info: std::net::SocketAddr::V4(std::net::SocketAddrV4::new(std::net::Ipv4Addr::new(0,0,0,0),0)),
            path: "/".to_owned(),
            method: "NILL".to_owned(),
            version: String::new(),
            host: "about:blank".to_owned(),
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }
}

#[derive(Debug,Clone,Copy)]
pub enum Compression{
    Plain,
    Gzip,
}

#[derive(Debug,Clone,Copy,PartialEq)]
pub enum HttpType{
    Http1,
    Http2,
}
// ## Generic streams
// #[allow(async_fn_in_trait)]
// pub trait NetStream{
//     async fn readable(&self) -> io::Result<()>;
//     fn try_read(&self, buf: &mut [u8]) -> io::Result<usize>;
//     async fn write_all<'a>(&'a mut self, src: &'a [u8]) -> io::Result<()>;
// }
// #[allow(async_fn_in_trait)]
pub trait Stream:AsyncRead+AsyncWrite+Unpin+Send+Sync{
    fn read_all(&mut self)->impl Future<Output = io::Result<Vec<u8>>>;
}

impl<T> Stream for T
where
    T: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    async fn read_all(&mut self)->io::Result<Vec<u8>>{
        let mut buf=[0u8; 4096];
        let mut total = Vec::new();
        loop{
            let n=self.read(&mut buf).await?;
            if n==0{ break };
            total.extend_from_slice(&buf[..n]);
            if n<buf.len(){
                break
            }
        }
        Ok(total)
    }
}
// impl Stream for net::TcpStream{}

// # Errors

pub type HttpResult<T> = Result<T,HttpError>;

#[derive(Debug)]
pub enum HttpError{
    Io(io::Error),
    ConnectionClosed,
    Invalid,

    HeadersSent,
    InvalidHeader,

    InvalidFrame,
    InvalidPreface,
    FrameTooBig,
    FrameTooSmall,
    StreamDoesntExist,

    MissingHeaders,

    NotSupported,
}

impl fmt::Display for HttpError{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self{
            Self::Io(io_err)=>write!(f,"I/O Error: {}",io_err),
            Self::ConnectionClosed=>write!(f,"Connection is closed"),
            Self::HeadersSent=>write!(f,"Headers already sent"),
            Self::InvalidHeader=>write!(f,"Cannot use this header"),
            Self::Invalid=>write!(f,"Invalid invocation"),
            Self::InvalidFrame=>write!(f, "Invalid frame"),
            Self::InvalidPreface=>write!(f, "Invalid preface"),
            Self::FrameTooBig=>write!(f, "Frame payload too big"),
            Self::FrameTooSmall=>write!(f, "Frame head too small"),
            Self::StreamDoesntExist=>write!(f, "Stream doesnt exist"),
            Self::MissingHeaders=>write!(f, "Header(s) missing"),
            Self::NotSupported=>write!(f, "Operation not supported"),
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