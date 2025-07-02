use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::{collections::HashMap};

use crate::client::HttpClient;
use crate::traits::HttpSocket;

pub struct Http1Socket{
    closed: bool,
    head_closed: bool,

    buff: Vec<u8>,
    headers: HashMap<String,Vec<String>>,
    tcp_socket: tokio::net::TcpStream,

    pub status: u16,
    pub status_msg: String,

    pub client: HttpClient,
}

impl HttpSocket for Http1Socket{
    fn new(bufsize: usize, socket: tokio::net::TcpStream, addr: std::net::SocketAddr)->Self{
        let mut s= Self { 
            closed: false,
            head_closed: false,

            tcp_socket: socket, 
            buff: vec![0_u8; bufsize], 
            headers: HashMap::new(), 

            status: 200,
            status_msg: "OK".to_owned(),

            client: HttpClient {
                read: false,
                path: String::new(),
                method: String::new(),
                version: String::new(),
                host: String::new(),
                headers: HashMap::new(),
                info: addr,
            }
        };
        // s.headers.insert("Connection".to_owned(), vec!["close".to_owned()]);
        s
    }
}

impl Http1Socket{
    pub fn set_header(&mut self, name: &str, value: &str)->bool{
        if self.head_closed { return false };
        match name.to_lowercase().as_str(){
            "connection" | "content-length" | "transfer-encoding" => {
                return false
            },
            _ => (),
        };
        if let Some(vec)=self.headers.get_mut(name){
            vec.push(value.to_owned());
        } else {
            self.headers.insert(name.to_owned(), vec![value.to_owned()]);
        };
        true
    }
    pub fn get_headers_as_string(&self)->String{
        let mut tot=String::new();
        for(h,ve)in &self.headers{
            for v in ve{
                tot+=&format!("{}: {}\r\n",h,v);
            }
        }
        tot
    }
    pub fn remove_header(&mut self, name: &str)->Option<Vec<String>>{
        if self.head_closed { return None };
        self.headers.remove(name)
    }

    pub async fn send_head(&mut self)->std::io::Result<()>{
        if self.head_closed { return Err(std::io::Error::new(std::io::ErrorKind::Other,"already wrote head")) };

        self.headers.insert("Connection".to_owned(), vec!["close".to_owned()]);
        
        let headers = self.get_headers_as_string();
        let head = format!("HTTP/1.1 {} {}\r\n{}\r\n",self.status,&self.status_msg,headers);

        self.tcp_socket.write_all(head.as_bytes()).await?;

        self.head_closed=true;
        Ok(())
    }
}