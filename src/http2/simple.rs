use std::{collections::HashMap, sync::Arc, /*time::Duration*/};

use crate::{common::{HttpClient, HttpError, HttpResult, HttpSocket, HttpType, HttpVersion, Stream}, http2::Http2Session};

pub struct Http2Handler<S:Stream>{ // simplified handler, not the actual session
    pub stream_id: u32,
    pub session: Arc<Http2Session<S>>,
    
    client: HttpClient,
    headers: HashMap<String,Vec<String>>,

    head_closed: bool,
    status: u16,
    closed: bool,
}

impl<S:Stream> Http2Handler<S>{
    pub fn new(stream_id: u32, session: Arc<Http2Session<S>>)->Self{
        let info = session.addr.clone();
        Self { 
            stream_id, session,
            client: HttpClient {
                version: HttpVersion::Http2,
                version_string: "HTTP/2".to_owned(),
                info,
                ..Default::default()
            },
            headers: HashMap::new(),
            head_closed: false,
            status: 200,
            closed: false,
        }
    }
}

#[async_trait::async_trait]
impl<S:Stream> HttpSocket for Http2Handler<S>{
    type Stream = S;
    fn set_header(&mut self, name: &str, value: &str)->HttpResult<()>{
        if self.head_closed { return Err(HttpError::HeadersSent) };
        let name=name.to_lowercase();
        if name.starts_with(":"){ return Err(HttpError::InvalidHeader) };
        match name.as_str(){
            "connection" | "content-length" | "transfer-encoding" => {
                return Err(HttpError::InvalidHeader)
            },
            _ => (),
        };
        self.headers.insert(name.to_owned(), vec![value.to_owned()]);
        Ok(())
    }
    fn add_header(&mut self, name: &str, value: &str)->HttpResult<()>{
        if self.head_closed { return Err(HttpError::HeadersSent) };
        let name=name.to_lowercase();
        if name.starts_with(":"){ return Err(HttpError::InvalidHeader) };
        match name.as_str(){
            "connection" | "content-length" | "transfer-encoding" => {
                return Err(HttpError::InvalidHeader)
            },
            _ => (),
        };
        if let Some(vec)=self.headers.get_mut(&name){
            vec.push(value.to_owned());
        } else {
            self.headers.insert(name.to_owned(), vec![value.to_owned()]);
        };
        Ok(())
    }
    fn remove_header(&mut self, name: &str)->HttpResult<Vec<String>>{
        if self.head_closed { return Err(HttpError::HeadersSent) };
        if let Some(removed)=self.headers.remove(&name.to_lowercase()){
            Ok(removed)
        } else {
            Err(HttpError::InvalidHeader)
        }
    }
    fn set_compression(&mut self, _new_compression: crate::common::Compression)->HttpResult<()>{ 
        Ok(())
    }
    fn set_status(&mut self, status: u16, _msg: String)->HttpResult<()> {
        self.status=status;
        Ok(())
    }
    
    async fn read_client(&mut self)->Result<&HttpClient, HttpError>{ 
        // let mut streams=self.session.streams.lock().await;
        let stream=if let Some(s)=self.session.streams.get_mut(&self.stream_id){ s } else{ return Err(HttpError::StreamDoesntExist) };
        
        for (n,v) in &stream.headers{
            let name=String::from_utf8_lossy(&n).to_string();
            let value=String::from_utf8_lossy(&v).to_string();
            if name == ":method" { self.client.method = value }
            // else if name == ":scheme" {  }
            else if name == ":authority" { self.client.host = value }
            else if name == ":path" { self.client.path = value }
            else if !name.starts_with(":"){
                if let Some(hsv)=self.client.headers.get_mut(&name){
                    hsv.push(value)
                } else {
                    self.client.headers.insert(name, vec![value]);
                }
            }
        }
        self.client.read = stream.end_headers;

        Ok(&self.client)
    }
    async fn get_client(&mut self)->Result<&HttpClient, HttpError>{ 
        Ok(&self.client)
    }

    async fn send_head(&mut self)->HttpResult<()>{ 
        if self.closed { return Err(HttpError::ConnectionClosed) };
        if self.head_closed { return Err(HttpError::HeadersSent) };

        // self.headers.insert(":status".to_owned(), vec![self.status.to_string()]);
        let string_stat=self.status.to_string();
        let mut headers: Vec<(&[u8], &[u8])>=vec![(b":status",string_stat.as_bytes())];
        for (name,values) in &self.headers{
            for value in values { 
                headers.push((name.as_bytes(),value.as_bytes()))
            }
        };

        // let head=self.session.hpack_encode(headers).await;
        self.session.send_headers(false, self.stream_id, headers).await?;
        Ok(())
    }
    async fn close(&mut self, bytes: &[u8])->HttpResult<()>{ 
        if self.closed { return Err(HttpError::ConnectionClosed) };
        if !self.head_closed { 
            self.headers.insert("content-length".to_owned(), vec![bytes.len().to_string()]);
            self.send_head().await? 
        };

        self.session.send_data(true, self.stream_id, bytes).await?;
        Ok(())
    }
    async fn write(&mut self, bytes: &[u8])->HttpResult<()>{ 
        if self.closed { return Err(HttpError::ConnectionClosed) };
        if !self.head_closed { self.send_head().await? };
        
        self.session.send_data(false, self.stream_id, bytes).await?;
        Ok(())
    }

    async fn websocket(self)->HttpResult<crate::websocket::WebSocket<S>> {
        self.session.send_rst_stream(self.stream_id, 0xd).await?;
        Err(HttpError::NotSupported)
    }
    
    fn get_type(&self)->HttpType{
        HttpType::Http2
    }
    fn get_http1(self)->crate::http1::Http1Socket<Self::Stream> {
        panic!("cannot convert http2 to http1")
    }
    // fn get_http2_stream(self)->Http2Handler<'a, Self::Stream> {
    //     self
    // }
}

