// use std::fmt::UpperHex;
use std::io;
use tokio::{io::AsyncWriteExt, /*net::TcpSocket*/};
use std::{collections::HashMap};
use async_compression::tokio::write::GzipEncoder;
use base64::{engine::general_purpose, Engine as _};
use sha1::{Sha1, Digest};

use crate::{common::{ 
    Compression, HttpClient, HttpConstructor, HttpError, HttpResult, HttpSocket, HttpType, Stream
}, http2::{Http2FrameSettings, Http2Session}};
use crate::websocket::{WebSocket,MAGIC};
// use crate::common::HttpSocket;

pub const H2C_UPGRADE: &'static [u8] = b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n";
pub const WS_UPGRADE: &'static [u8] = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ";

pub struct Http1Socket<S:Stream>{
    closed: bool,
    head_closed: bool,

    buff: Vec<u8>,
    headers: HashMap<String,Vec<String>>,
    socket: S,
    compression: Compression,

    pub status: u16,
    pub status_msg: String,

    pub client: HttpClient,
}

impl<'a,S:Stream> Http1Socket<S>{
    fn get_headers_as_string(&self)->String{
        let mut tot=String::new();
        for(h,ve)in &self.headers{
            for v in ve{
                tot+=&format!("{}: {}\r\n",h,v);
            }
        }
        tot
    }
    /*async fn read_available(&mut self)->std::io::Result<usize>{
        let mut buff=[0u8; 1024];
        let mut r:usize=0;
        loop{
            self.socket.readable().await?;
            let res=self.socket.try_read(&mut buff);
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
    }*/
    async fn read_new(&mut self)->io::Result<usize>{
        let read=self.socket.read_all().await?;
        self.buff.extend_from_slice(&read);
        Ok(read.len())
    }
    pub async fn update_client(&mut self)->std::io::Result<()>{
        if self.closed { return Err(io::Error::new(io::ErrorKind::ConnectionAborted,"connection isnt open")); };
        
        let _size = self.read_new().await?;
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
        } else if let Some(_)=self.client.headers.get("transfer-encoding"){
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
    async fn write_chunk(&mut self, buff: &[u8])->io::Result<usize>{
        let mut w: Vec<u8>=Vec::new();
        let s=format!("{:X}",buff.len());
        let sep=b"\r\n";
        let sb=s.as_bytes();
        w.extend_from_slice(sb);
        w.extend_from_slice(sep);
        w.extend_from_slice(buff);
        w.extend_from_slice(sep);
        self.socket.write_all(&w).await?;
        Ok(w.len())
    }
    fn _get_compression(&self)->Compression{ self.compression.clone() }
    
    pub async fn h2c(mut self)->HttpResult<Http2Session<'a,S>>{
        let client=self.client;
        let settings=if let Some(s)=client.headers.get("http2-settings"){
            // let s=(&s[0]).to_owned();
            let dec = general_purpose::STANDARD.decode(&s[0]);
            if let Ok(p)=dec{
                if let Some(s)=Http2FrameSettings::parse(&p){ s }
                else {
                    Http2FrameSettings::default()
                }
            } else {
                Http2FrameSettings::default()
            }
        } else {
            Http2FrameSettings::default()
        };
        
        self.socket.write_all(H2C_UPGRADE).await?;
        let ses=Http2Session::new(self.socket, client.info.clone());
        ses.add_stream(1, client, settings).await?;
        Ok(ses)
    }
    pub async fn websocket(mut self)->HttpResult<WebSocket<S>>{
        let wskey=if let Some(k)=self.client.headers.get("sec-websocket-key"){ &k[0] }else{ return Err(HttpError::MissingHeaders) };
        
        let mut decreskey=wskey.as_bytes().to_vec();
        decreskey.extend_from_slice(MAGIC);

        let mut sh=Sha1::new();
        sh.update(&decreskey);
        let shares=sh.finalize();

        let reskey=general_purpose::STANDARD.encode(shares);

        let totres=[WS_UPGRADE,reskey.as_bytes(),b"\r\n\r\n"].concat();

        self.socket.write_all(&totres).await?;

        Ok(WebSocket::new(self.socket, self.client.info.clone()))
    }
}

impl<S:Stream> HttpSocket for Http1Socket<S>{
    type Stream = S;
    fn set_header(&mut self, name: &str, value: &str)->HttpResult<()>{
        if self.head_closed { return Err(HttpError::HeadersSent) };
        match name.to_lowercase().as_str(){
            "connection" | "content-length" | "transfer-encoding" => {
                return Err(HttpError::InvalidHeader)
            },
            _ => (),
        };
        if let Some(vec)=self.headers.get_mut(name){
            vec.push(value.to_owned());
        } else {
            self.headers.insert(name.to_owned(), vec![value.to_owned()]);
        };
        Ok(())
    }
    fn remove_header(&mut self, name: &str)->HttpResult<Vec<String>>{
        if self.head_closed { return Err(HttpError::HeadersSent) };
        if let Some(removed)=self.headers.remove(name){
            Ok(removed)
        } else {
            Err(HttpError::InvalidHeader)
        }
    }
    fn set_compression(&mut self, new_compression: Compression)->HttpResult<()>{ 
        if !self.head_closed{
            self.compression = new_compression; 
            Ok(())
        } else {
            Err(HttpError::HeadersSent)
        }
    }
    fn set_status(&mut self, status: u16, msg: String)->HttpResult<()> {
        self.status=status;
        self.status_msg=msg;
        Ok(())
    }

    async fn read_client(&mut self)->HttpResult<&HttpClient> {
        self.update_client().await?;
        Ok(&self.client)
    }
    async fn get_client(&mut self)->HttpResult<&HttpClient> {
        // self.update_client().await?;
        Ok(&self.client)
    }

    async fn send_head(&mut self)->HttpResult<()>{
        if self.head_closed { return Err(HttpError::HeadersSent) };

        self.headers.insert("Connection".to_owned(), vec!["close".to_owned()]);
        
        let headers = self.get_headers_as_string();
        let head = format!("HTTP/1.1 {} {}\r\n{}\r\n",self.status,&self.status_msg,headers);

        self.socket.write_all(head.as_bytes()).await?;

        self.head_closed=true;
        Ok(())
    }
    async fn write(&mut self, bytes: &[u8])->HttpResult<()> {
        if bytes.is_empty(){
            return Err(HttpError::Invalid)
        }
        if !self.head_closed{
            self.headers.insert("Transfer-Encoding".to_owned(), vec!["chunked".to_owned()]);
            // self.headers.remove("Content-Length");
            self.send_head().await?;
        };
        self.write_chunk(bytes).await?;
        Ok(())
    }
    async fn close(&mut self, bytes: &[u8])->HttpResult<()>{
        if !self.head_closed{
            match self.compression{
                Compression::Plain=>{
                    self.headers.insert("Content-Length".to_owned(), vec![bytes.len().to_string()]);
                    self.send_head().await?;
                    self.socket.write_all(bytes).await?;
                    self.closed=true;
                },
                Compression::Gzip=>{
                    self.headers.insert("Content-Encoding".to_owned(), vec!["gzip".to_owned()]);
                    let mut enc=GzipEncoder::new(Vec::new());
                    enc.write_all(bytes).await?;
                    enc.shutdown().await?;
                    let inner = enc.get_ref();
                    self.headers.insert("Content-Length".to_owned(), vec![inner.len().to_string()]);
                    self.send_head().await?;
                    self.socket.write_all(inner).await?;
                    self.closed=true;
                },
            }
        } else {
            if !bytes.is_empty(){
                self.write(bytes).await?;
            }
            self.socket.write_all(b"0\r\n\r\n").await?;
        }
        self.socket.shutdown().await?;
        Ok(())
    }

    // #[allow(refining_impl_trait)]
    async fn websocket(self)->HttpResult<WebSocket<S>> {
        self.websocket().await
    }
    fn r#type(&self)->HttpType{
        HttpType::Http1
    }
    fn get_http1(self)->Http1Socket<Self::Stream> {
        self
    }
    // fn get_http2(&self)->std::sync::Arc<Http2Session<Self::Stream>> {
    //     panic!("cannot cast http1 to http2")
    // }
}

impl<S:Stream> HttpConstructor<S> for Http1Socket<S>{
    fn new(socket: S, addr: std::net::SocketAddr)->Self{
        /*let mut s=*/ Self { 
            closed: false,
            head_closed: false,

            socket: socket, 
            buff: vec![0_u8; 0], 
            headers: HashMap::new(), 
            compression: Compression::Plain,

            status: 200,
            status_msg: "OK".to_owned(),

            client: HttpClient {
                info: addr,
                ..Default::default()
            }
        }
        // s.headers.insert("Connection".to_owned(), vec!["close".to_owned()]);
        // s
    }
}
