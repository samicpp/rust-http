use std::io;
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

impl Http1Socket{
    fn get_headers_as_string(&self)->String{
        let mut tot=String::new();
        for(h,ve)in &self.headers{
            for v in ve{
                tot+=&format!("{}: {}\r\n",h,v);
            }
        }
        tot
    }
    fn read_available(&mut self)->std::io::Result<usize>{
        let mut buff=Vec::<u8>::new();
        let r=self.tcp_socket.try_read_buf(&mut buff)?;
        self.buff.extend_from_slice(&buff);
        Ok(r)
    }
    pub async fn update_client(&mut self)->std::io::Result<()>{
        if self.closed { return Err(io::Error::new(io::ErrorKind::ConnectionAborted,"connection isnt open")) };
        
        let size = self.read_available()?;
        let slice = &self.buff[..size];
        let string = match str::from_utf8(slice) { Ok(s)=>s, Err(_)=>"" };
        let parts = string.split("\r\n\r\n").collect::<Vec<&str>>();

        if parts.len()<1 { return Err(io::Error::new(io::ErrorKind::Other, "invalid client data")) };

        let mut headraw=parts[0].split("\r\n").collect::<Vec<&str>>();

        if headraw.len()<2 { return Err(io::Error::new(io::ErrorKind::Other, "invalid client data")) };

        let head=headraw.remove(0).split(" ").collect::<Vec<&str>>();
        let headers = headraw;

        self.client.method=head[0].to_owned();
        self.client.path=head[1].to_owned();
        self.client.version=head[2].to_owned();

        self.client.headers.clear();

        for sheader in headers {
            let harr = sheader.split(": ").collect::<Vec<&str>>();
            if harr.len()<2 { continue };
            let k=harr[0].to_lowercase(); let v=harr[1];

            if let Some(ve)=self.client.headers.get_mut(&k){
                ve.push(v.to_owned());
            } else {
                self.client.headers.insert(k.clone(), vec![v.to_owned()]);
            }

            if k=="host"{
                self.client.host=v.to_owned();
            }
        }

        self.client.body.clear();
        self.client.body.extend_from_slice( if let Some(bod)=parts.get(1) { bod.as_bytes() } else { "".as_bytes() } );

        self.client.read=true;

        Ok(())
    }
}

impl HttpSocket for Http1Socket{
    fn new(socket: tokio::net::TcpStream, addr: std::net::SocketAddr)->Self{
        /*let mut s=*/ Self { 
            closed: false,
            head_closed: false,

            tcp_socket: socket, 
            buff: vec![0_u8; 0], 
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
                body: Vec::new(),
                info: addr,
            }
        }
        // s.headers.insert("Connection".to_owned(), vec!["close".to_owned()]);
        // s
    }

    fn set_header(&mut self, name: &str, value: &str)->bool{
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
    fn remove_header(&mut self, name: &str)->Option<Vec<String>>{
        if self.head_closed { return None };
        self.headers.remove(name)
    }
    
    async fn get_client(&mut self)->io::Result<&HttpClient> {
        self.update_client().await?;
        Ok(&self.client)
    }

    async fn send_head(&mut self)->std::io::Result<()>{
        if self.head_closed { return Err(std::io::Error::new(std::io::ErrorKind::Other,"already wrote head")) };

        self.headers.insert("Connection".to_owned(), vec!["close".to_owned()]);
        
        let headers = self.get_headers_as_string();
        let head = format!("HTTP/1.1 {} {}\r\n{}\r\n",self.status,&self.status_msg,headers);

        self.tcp_socket.write_all(head.as_bytes()).await?;

        self.head_closed=true;
        Ok(())
    }
    async fn write(&mut self, bytes: &[u8])->std::io::Result<()> {
        /* placeholder */ Ok(())
    }
    async fn close(&mut self, bytes: &[u8])->io::Result<()>{
        if !self.head_closed{
            self.headers.insert("Content-Length".to_owned(), vec![bytes.len().to_string()]);
            self.send_head().await?;
            self.tcp_socket.write_all(bytes).await?;
            self.closed=true;
        }
        Ok(())
    }
}