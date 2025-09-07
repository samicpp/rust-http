use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool,Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;

use crate::http2::core::*;
use crate::http2::create;
use crate::common::{HttpClient, HttpConstructor, HttpError, HttpResult, HttpSocket, Stream};

// use hpack;

pub const MAGIC: &'static [u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"; // 0x505249202a20485454502f322e300d0a0d0a534d0d0a0d0a => 1969475691506423790601731136945089829455854996988862532874
const EMPTY: &'static [u8] = &[];

pub struct Http2Session<'a,S:Stream>{
    netr: Mutex<ReadHalf<S>>, netw: Mutex<WriteHalf<S>>,
    pub addr: SocketAddr,
    
    hpackd: Mutex<hpack::Decoder<'a>>, 
    hpacke: Mutex<hpack::Encoder<'a>>,

    pub settings: Mutex<Http2FrameSettings>,
    pub goaway: AtomicBool,
    pub window_size: Mutex<u32>,

    pub streams: Mutex<HashMap<u32,Http2Stream>>,
}

pub struct Http2Stream{
    pub stream_id: u32,
    pub settings: Http2FrameSettings,
    pub client: HttpClient,
    pub end_stream: bool,
    pub end_headers: bool,
    pub closed: bool,
    pub window_size: u32,
    // session: &'a Http2Session<'a,S>,
}

pub struct Http2Socket<'a,S:Stream>{ // simplified handler, not the actual session
    pub stream_id: u32,
    pub session: Arc<Http2Session<'a,S>>,
    
    client: HttpClient,
    headers: HashMap<String,Vec<String>>,

    head_closed: bool,
    status: u16,
    closed: bool,
}

impl<'a,S:Stream> Http2Session<'a,S>{
    fn new(socket: S, addr: SocketAddr, initial_settings: Http2FrameSettings)->Self {
        let (read,write)=tokio::io::split(socket);
        let ws= if let Some(s)=initial_settings.initial_window_size{s}else{4096};
        Self { 
            netr: Mutex::new(read), netw: Mutex::new(write), 
            addr:addr, 
            
            hpackd: Mutex::new(hpack::Decoder::new()), 
            hpacke: Mutex::new(hpack::Encoder::new()),

            settings: Mutex::new(initial_settings),
            streams: Mutex::new(HashMap::new()),
            goaway: AtomicBool::new(false),
            window_size: Mutex::new(ws),
        }
    }
    // fn with_settings(socket: S, addr: SocketAddr, settings: Http2FrameSettings)->Self{ }

    async fn read_all(&self)->std::io::Result<Vec<u8>>{
        let mut reader=self.netr.lock().await;
        let mut buf=[0u8; 4096];
        let mut total = Vec::new();
        loop{
            let n=reader.read(&mut buf).await?;
            if n==0{ break };
            total.extend_from_slice(&buf[..n]);
            if n<buf.len(){
                break
            }
        }
        Ok(total)
    }
    async fn write(&self,buf:&[u8])->std::io::Result<()>{
        let mut net=self.netw.lock().await;
        net.write_all(buf).await?;
        Ok(())
    }
    pub async fn flush(&self)->std::io::Result<()>{
        let mut net=self.netw.lock().await;
        net.flush().await?;
        Ok(())
    }
    pub async fn hpack_encode(&self, headers: Vec<(&[u8], &[u8])>)->Vec<u8>{
        let mut he=self.hpacke.lock().await;
        he.encode(headers)
    }
    pub async fn hpack_decode(&self, buf: &[u8])->Result<Vec<(Vec<u8>, Vec<u8>)>, hpack::decoder::DecoderError>{
        let mut hd=self.hpackd.lock().await;
        hd.decode(buf)
    }
    pub async fn incoming_frames(&self)->HttpResult<Vec<Http2Frame>>{
        // let mut reader=self.netr.lock().await;
        let mut buff=self.read_all().await?;
        let mut frames=Vec::new();
        loop{
            match Http2Frame::parse(buff){
                Some((frame,nbuff))=>{
                    buff=nbuff;
                    frames.push(frame);
                },
                None=>break,
            }
        }
        Ok(frames)
    }
    pub async fn init(&self)->HttpResult<Vec<Http2Frame>>{
        let mut buff=self.read_all().await?;
        if buff.len()<24 { return Err(HttpError::InvalidPreface) };
        let mut rest=buff.split_off(24);
        let matching=buff.iter().zip(MAGIC).map(|(a,b)|a==b).count();
        if matching!=24{ return Err(HttpError::InvalidPreface) };

        let mut frames=Vec::new();
        loop{
            match Http2Frame::parse(rest){
                Some((frame,nbuff))=>{
                    rest=nbuff;
                    frames.push(frame);
                },
                None=>break,
            }
        }
        Ok(frames)
    }
    // pub async fn read_one(&self)

    pub async fn handle_frames(&self,frames:Vec<Http2Frame>)->HttpResult<Vec<u32>>{
        let mut new_streams=Vec::new();
        for frame in frames{
            match frame.ftype{
                Http2FrameType::Ping=>{
                    if !frame.flags.acknowledge{
                        self.send_pong(frame.get_payload()).await?;
                    }
                },
                Http2FrameType::Headers=>{
                    // TODO: mix these two
                    let mut streams=self.streams.lock().await;
                    if let Some(stream)=streams.get_mut(&frame.stream_id){
                        // let mut client = stream.client.lock().await;
                        let h=self.hpack_decode(frame.get_padding()).await;
                        if let Ok(headers)=h{
                            for (n,v) in headers{
                                let name=String::from_utf8_lossy(&n).to_string();
                                let value=String::from_utf8_lossy(&v).to_string();
                                if name == ":method" { stream.client.method = value }
                                // else if name == ":scheme" {  }
                                else if name == ":authority" { stream.client.host = value }
                                else if name == ":path" { stream.client.path = value }
                                else{
                                    if let Some(hsv)=stream.client.headers.get_mut(&name){
                                        hsv.push(value)
                                    } else {
                                        stream.client.headers.insert(name, vec![value]);
                                    }
                                }
                            }
                        } else { 
                            // protocol error
                        };
                        stream.end_headers=frame.flags.end_headers;
                        stream.end_stream=frame.flags.end_stream;
                    } else {
                        let mut client=HttpClient::default();
                        let h=self.hpack_decode(frame.get_padding()).await;
                        if let Ok(headers)=h{
                            for (n,v) in headers{
                                let name=String::from_utf8_lossy(&n).to_string();
                                let value=String::from_utf8_lossy(&v).to_string();
                                if name == ":method" { client.method = value }
                                // else if name == ":scheme" {  }
                                else if name == ":authority" { client.host = value }
                                else if name == ":path" { client.path = value }
                                else{
                                    if let Some(hsv)=client.headers.get_mut(&name){
                                        hsv.push(value)
                                    } else {
                                        client.headers.insert(name, vec![value]);
                                    }
                                }
                            }
                        } else { 
                            // protocol error
                        };
                        let window_size=if let Some(s)=self.settings.lock().await.initial_window_size{s}else{0};
                        let stream = Http2Stream {
                            stream_id: frame.stream_id,
                            settings: Http2FrameSettings::default(),
                            client: client,
                            end_headers: false,
                            end_stream: false,
                            closed: false,
                            window_size,
                        };
                        new_streams.push(frame.stream_id);
                        streams.insert(frame.stream_id, stream);
                    }
                },
                Http2FrameType::Data=>{
                    let mut streams=self.streams.lock().await;
                    if let Some(stream)=streams.get_mut(&frame.stream_id){
                        stream.client.body.extend_from_slice(frame.get_payload());
                        stream.end_stream=frame.flags.end_stream;
                    } else {
                        // protocol error
                    }
                },
                Http2FrameType::Settings=>{
                    if !frame.flags.acknowledge{
                        let mut streams=self.streams.lock().await;
                        // TODO: also mix these two
                        if frame.stream_id==0{
                            let mut cset=self.settings.lock().await;
                            if let Some(settings)=frame.settings{
                                if let Some(v)=settings.header_table_size{ cset.header_table_size=Some(v) }
                                if let Some(v)=settings.enable_push{ cset.enable_push=Some(v) }
                                if let Some(v)=settings.max_concurrent_streams{ cset.max_concurrent_streams=Some(v) }
                                // if let Some(v)=settings.initial_window_size{ cset.initial_window_size=Some(v) }
                                if let Some(v)=settings.max_frame_size{ cset.max_frame_size=Some(v) }
                                if let Some(v)=settings.max_header_list_size{ cset.max_header_list_size=Some(v) }

                                if let Some(v)=settings.initial_window_size{ 
                                    let mut s=self.window_size.lock().await;
                                    if streams.len()==0{
                                        *s=v;
                                    } else {
                                        let old=if let Some(i)=cset.initial_window_size{i}else{0};
                                        let diff=v as i64 - old as i64;
                                        let news=s.clone() as i64 + diff;
                                        if news<0 { 
                                            *s=0 
                                        } else if news>16777215 {
                                            // protocol error
                                            *s=16777215
                                        } else {
                                            *s=(news&0x00FF_FFFF) as u32 // final safety
                                        }
                                        for stream in streams.values_mut(){
                                            let cs=stream.window_size as i64;
                                            let news=cs+diff;
                                            if news<0{
                                                stream.window_size=0
                                            } else if news>16777215{
                                                // protocol error
                                                stream.window_size=16777215
                                            } else {
                                                stream.window_size=(news&0x00FF_FFFF) as u32
                                            }
                                        }
                                    }
                                    cset.initial_window_size=Some(v); 
                                };
                                // if let Some(size)=settings.header_table_size{
                                //     let hpacke=self.hpacke.lock().await;
                                //     hpack::Encoder::new
                                // };

                                drop(streams);
                                self.send_ackset(0).await?;
                            } else {
                                // protocol error
                            }
                        } else if let Some(stream)=streams.get_mut(&frame.stream_id){
                            if let Some(settings)=frame.settings{
                                if let Some(v)=settings.header_table_size{ stream.settings.header_table_size=Some(v) }
                                if let Some(v)=settings.enable_push{ stream.settings.enable_push=Some(v) }
                                if let Some(v)=settings.max_concurrent_streams{ stream.settings.max_concurrent_streams=Some(v) }
                                if let Some(v)=settings.initial_window_size{ stream.settings.initial_window_size=Some(v) }
                                if let Some(v)=settings.max_frame_size{ stream.settings.max_frame_size=Some(v) }
                                if let Some(v)=settings.max_header_list_size{ stream.settings.max_header_list_size=Some(v) }
                            };
                            drop(streams);
                            self.send_ackset(frame.stream_id).await?;
                        } else {
                            // protocol error
                        }
                    }
                },
                Http2FrameType::RstStream=>{
                    let mut streams=self.streams.lock().await;
                    if let Some(stream)=streams.get_mut(&frame.stream_id){
                        stream.end_headers=true;
                        stream.end_stream=true;
                        stream.closed=true;
                    } else {
                        // protocol error
                    }
                },
                Http2FrameType::Goaway=>{
                    self.goaway.store(true, Ordering::SeqCst);
                },
                Http2FrameType::WindowUpdate=>{
                    let mut streams=self.streams.lock().await;
                    if frame.payload.len()==4{
                        let data=frame.get_payload();
                        let us=(data[3]as u32)<<24 | (data[2]as u32)<<16 | (data[1]as u32)<<8 | (data[0] as u32);

                        if frame.stream_id==0{
                            let mut s=self.window_size.lock().await;
                            let ns=s.clone() as u64 + us as u64;
                            if ns>16777215{
                                // protocol error
                                *s=16777215
                            } else {
                                *s=(ns&0x00FF_FFFF) as u32
                            }
                        } else if let Some(stream)=streams.get_mut(&frame.stream_id){
                            let ns=stream.window_size as u64 + us as u64;
                            if ns>16777215{
                                // protocol error
                                stream.window_size=16777215
                            } else {
                                stream.window_size=(ns&0x00FF_FFFF) as u32
                            }
                        } else {
                            // protocol error
                        }
                    }
                },
                Http2FrameType::Continuation=>{},
                Http2FrameType::Priority=>{},
                Http2FrameType::PushPromise=>{},
                Http2FrameType::Unknown(_u)=>{},
                // _=>()
            }
        };
        Ok(new_streams)
    }

    pub async fn send_data(&self,last: bool,stream_id: u32,payload: &[u8])->HttpResult<Option<Vec<u8>>>{
        let mut cws=self.window_size.lock().await;
        let mut streams=self.streams.lock().await;
        let stream=if let Some(stream)=streams.get_mut(&stream_id){stream}else{ return Err(HttpError::StreamDoesntExist) };
        if stream.end_stream { return Err(HttpError::ConnectionClosed) };
        if stream.closed { return Err(HttpError::ConnectionClosed) };
        let sws=stream.window_size;
        let min=if cws.clone()<sws{cws.clone()}else{sws} as usize;
        if payload.len()<min{
            let fb=create::raw_frame(stream_id, 0, if last{1}else{0}, payload, EMPTY)?;
            self.write(&fb).await?;
            stream.window_size-=payload.len() as u32;
            *cws-=payload.len() as u32;
            Ok(None)
        } else {
            let pl=&payload[..min];
            let rest=&payload[min..];
            let fb=create::raw_frame(stream_id, 0, 0, pl, EMPTY)?;
            self.write(&fb).await?;
            stream.window_size-=pl.len() as u32; // sync safety instead of setting to 0
            *cws-=pl.len() as u32;
            Ok(Some(rest.to_vec()))
        }
    }
    pub async fn send_headers(&self,last: bool,end_head: bool,stream_id: u32,headers: Vec<(&[u8], &[u8])>)->HttpResult<()>{
        let mut streams=self.streams.lock().await;
        let stream=if let Some(stream)=streams.get_mut(&stream_id){stream}else{ return Err(HttpError::StreamDoesntExist) };
        if stream.end_headers { return Err(HttpError::HeadersSent) };
        if stream.closed { return Err(HttpError::ConnectionClosed) };
        if end_head { stream.end_headers=true };
        if last { stream.end_stream=true };
        drop(streams);
        let fl=if last{1}else{0}|if end_head{4}else{0};
        let fl=fl as u8;
        let hb=self.hpack_encode(headers).await;
        let fb = create::raw_frame(stream_id, 1, fl, &hb, EMPTY)?;
        self.write(&fb).await?;
        Ok(())
    }
    pub async fn send_settings(&self,stream_id: u32,settings: Http2FrameSettings)->HttpResult<()>{
        if stream_id!=0{
            let streams=self.streams.lock().await;
            if !streams.contains_key(&stream_id){ return Err(HttpError::StreamDoesntExist) };
            drop(streams);
        };
        let sb=settings.to_buff();
        let fb = create::raw_frame(stream_id, 4, 0, &sb, EMPTY)?;
        self.write(&fb).await?;
        Ok(())
    }
    pub async fn send_ackset(&self,stream_id: u32)->HttpResult<()>{
        if stream_id!=0{
            let streams=self.streams.lock().await;
            if !streams.contains_key(&stream_id){ return Err(HttpError::StreamDoesntExist) };
            drop(streams);
        };
        let fb = create::raw_frame(stream_id, 4, 1, EMPTY, EMPTY)?;
        self.write(&fb).await?;
        Ok(())
    }
    pub async fn send_ping(&self)->HttpResult<()>{
        let payload=[0,0,0,0,0,0,0,1];  // length 8
        let fb = create::raw_frame(0, 6, 0, &payload, EMPTY)?;
        self.write(&fb).await?;
        Ok(())
    }
    pub async fn send_pong(&self,payload: &[u8])->HttpResult<()>{
        let fb = create::raw_frame(0, 6, 1, payload, EMPTY)?;
        self.write(&fb).await?;
        Ok(())
    }
}

impl<'a,S:Stream> HttpConstructor<S> for Http2Session<'a,S>{
    fn new(socket: S, addr: SocketAddr)->Self {
        Self::new(socket,addr,Http2FrameSettings::default())
    }
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
