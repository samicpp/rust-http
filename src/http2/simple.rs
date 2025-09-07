use std::{collections::HashMap, sync::Arc};

use crate::{common::{HttpClient, HttpError, HttpResult, HttpSocket, Stream}, http2::Http2Session};

pub struct Http2Socket<'a,S:Stream>{ // simplified handler, not the actual session
    pub stream_id: u32,
    pub session: Arc<Http2Session<'a,S>>,
    
    client: HttpClient,
    headers: HashMap<String,Vec<String>>,

    head_closed: bool,
    status: u16,
    closed: bool,
}

impl<'a,S:Stream> Http2Socket<'a,S>{
    pub fn new(stream_id: u32, session: Arc<Http2Session<'a,S>>)->Self{
        Self { 
            stream_id, session,
            client: HttpClient::default(),
            headers: HashMap::new(),
            head_closed: false,
            status: 200,
            closed: false,
        }
    }
}

impl<'a,S:Stream> HttpSocket for Http2Socket<'a,S>{
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
    
    async fn read_client<'_a>(&'_a mut self)->Result<&'_a HttpClient, HttpError>{ 
        let mut streams=self.session.streams.lock().await;
        let stream=if let Some(s)=streams.get_mut(&self.stream_id){ s } else{ return Err(HttpError::StreamDoesntExist) };
        self.client=stream.client.clone();
        Ok(&self.client)
    }
    async fn get_client<'_a>(&'_a mut self)->Result<&'_a HttpClient, HttpError>{ 
        Ok(&self.client)
    }

    async fn send_head(&mut self)->HttpResult<()>{ 
        if self.closed { return Err(HttpError::ConnectionClosed) };
        if self.head_closed { return Err(HttpError::HeadersSent) };

        self.headers.insert(":status".to_owned(), vec![self.status.to_string()]);
        let mut headers=Vec::new();
        for (name,values) in &self.headers{
            for value in values { 
                headers.push((name.as_bytes(),value.as_bytes()))
            }
        };

        // let head=self.session.hpack_encode(headers).await;
        self.session.send_headers(false, true, self.stream_id, headers).await?;
        Ok(())
    }
    async fn close(&mut self, bytes: &[u8])->HttpResult<()>{ 
        if self.closed { return Err(HttpError::ConnectionClosed) };
        if !self.head_closed { self.send_head().await? };
        self.session.send_data(true, self.stream_id, bytes).await?;
        self.closed=true;
        Ok(())
    }
    async fn write(&mut self, bytes: &[u8])->HttpResult<()>{ 
        if self.closed { return Err(HttpError::ConnectionClosed) };
        if !self.head_closed { self.send_head().await? };
        self.session.send_data(false, self.stream_id, bytes).await?;
        Ok(())
    }
}

