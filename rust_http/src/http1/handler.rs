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
    async fn read_available(&mut self)->std::io::Result<usize>{
        let mut buff=[0u8; 1024];
        let mut r:usize=0;
        loop{
            self.tcp_socket.readable().await?;
            let res=self.tcp_socket.try_read(&mut buff);
            // dbg!(&res);
            match res{
                Ok(0)=>break,
                Ok(n)=>{
                    self.buff.extend_from_slice(&buff[..n]);
                    r+=n;
                    // buff.clear();
                },
                Err(e) if e.kind()==io::ErrorKind::WouldBlock=>break,
                Err(e)=>return Err(e),
            };
        };
        // self.buff.extend_from_slice(&buff);
        Ok(r)
    }
    pub async fn update_client(&mut self)->std::io::Result<()>{
        if self.closed { return Err(io::Error::new(io::ErrorKind::ConnectionAborted,"connection isnt open")); };
        
        let _size = self.read_available().await?;
        let slice = &self.buff;//[..size];
        // let string = match str::from_utf8(slice) { Ok(s)=>s, Err(_)=>"" };
        // let parts = string.split("\r\n\r\n").collect::<Vec<&str>>();

        // if parts.len()<1 { return Err(io::Error::new(io::ErrorKind::Other, "invalid client data")) };
        if slice.is_empty() { return Err(io::Error::new(io::ErrorKind::InvalidData,"could read client bytes")) }

        let mut head_part=Vec::<u8>::new();
        let mut body_part=Vec::<u8>::new();

        if let Some(seperator)=slice.windows(4).position(|window| window == b"\r\n\r\n"){
            let bod_start=seperator+4;
            head_part.extend_from_slice(&slice[..seperator]);
            body_part.extend_from_slice(&slice[bod_start..]);
        } else {
            head_part.extend_from_slice(&slice);
        };

        let head_part=head_part;
        let body_part=body_part;

        let head_string=match str::from_utf8(&head_part){Ok(s)=>s, Err(_)=>""};


        let mut headraw=head_string.split("\r\n").collect::<Vec<&str>>();

        // dbg!(self.buff.len());
        // dbg!(slice.len());
        // dbg!(head_part.len()); 
        // dbg!(body_part.len()); 
        // dbg!(&head_string); 
        // dbg!(&headraw); 

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
        let body=body_part;//if let Some(bod)=parts.get(1) { bod.as_bytes() } else { eprintln!("no body {:?}",parts); "".as_bytes() };
        // self.client.body.extend_from_slice(body);
        if self.client.headers.contains_key("content-length"){
            self.client.body.extend_from_slice(&body);
        } else if let Some(v)=self.client.headers.get("transfer-encoding"){
            let mut i: usize=0; let mut size: usize=0; let mut read: usize=0;
            let mut hex=String::new(); let mut buff=Vec::<u8>::new();
            while i<body.len(){
                if size>read{
                    buff.push(body[i+read]);
                    read+=1;
                    continue;
                } else if size==read {
                    i+=size+2;
                    hex="".to_owned()  ;
                    read=0;
                }

                let cur=&body[i];

                if *cur==b'\r'{
                    if let Ok(nsize)=usize::from_str_radix(&hex, 16){
                        size=nsize;
                        i+=1;
                    } else {
                        break;
                    };
                } else {
                    hex+=&(cur.to_owned() as char).to_string();
                }
                i+=1;
            }
            
        }

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
        self.tcp_socket.shutdown().await?;
        Ok(())
    }
}