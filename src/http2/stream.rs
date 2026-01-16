use std::collections::{VecDeque};
use std::net::SocketAddr;
// use std::pin::Pin;
// use std::sync::{Arc};
use std::sync::atomic::{AtomicBool,Ordering};
use std::cmp;
// use whirlwind::ShardMap;
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use std::sync::Mutex as SMutex;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;

use crate::http2::core::*;
use crate::http2::create;
use crate::common::{HttpConstructor, HttpError, HttpResult, /*HttpSocket,*/ Stream};

// use hpack;

pub const MAGIC: &'static [u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"; // 0x505249202a20485454502f322e300d0a0d0a534d0d0a0d0a => 1969475691506423790601731136945089829455854996988862532874
const EMPTY: &'static [u8] = &[];

// TODO: read and send big header continuation frames & add integrated flow control in send_data & send window size update on client read 

pub struct Http2Session<S:Stream>{
    netr: Mutex<ReadHalf<S>>, netw: Mutex<WriteHalf<S>>,
    pub addr: SocketAddr,
    
    pub hpackd: SMutex<hpack::decoder::Decoder<'static>>, 
    pub hpacke: SMutex<hpack::encoder::Encoder<'static>>,

    pub settings: Mutex<Http2FrameSettings>,
    pub goaway: AtomicBool,
    pub window_size: Mutex<u32>,

    pub streams: DashMap<u32,Http2Stream>,
    
    que: Mutex<VecDeque<Http2Frame>>,
}

#[derive(Debug,Clone)]
pub struct Http2Stream{
    pub stream_id: u32,
    // pub client: HttpClient,
    pub body: Vec<u8>,
    pub head: Vec<u8>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,

    pub end_stream: bool,
    pub end_headers: bool,

    pub self_end_stream: bool,
    pub self_end_headers: bool,

    pub closed: bool,
    pub window_size: u32,

    pub valid: bool,
    // session: &'a Http2Session<'a,S>,
}

impl Http2Stream{
    pub fn empty()->Self{
        Self {
            stream_id: 0,
            body: Vec::new(),
            head: Vec::new(),
            headers: Vec::new(),
            end_stream: false,
            end_headers: false,
            closed: false,
            window_size: 0,
            valid: false,
            self_end_headers: false,
            self_end_stream: false,
        }
    }
}

impl<S:Stream> Http2Session<S>{
    pub fn new(socket: S, addr: SocketAddr, initial_settings: Http2FrameSettings)->Self {
        let (read,write)=tokio::io::split(socket);
        let ws= if let Some(s)=initial_settings.initial_window_size{s}else{16384};
        let hts= if let Some(s)=initial_settings.header_table_size{s}else{4096};
        let mut dec=hpack::decoder::Decoder::new();
        dec.set_max_table_size(hts as usize);
        Self { 
            netr: Mutex::new(read), netw: Mutex::new(write), 
            addr:addr, 
            
            hpackd: SMutex::new(dec), 
            hpacke: SMutex::new(hpack::encoder::Encoder::new()),

            settings: Mutex::new(initial_settings),
            streams: DashMap::new(),
            goaway: AtomicBool::new(false),
            window_size: Mutex::new(ws),
            que: Mutex::new(VecDeque::new()),
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
        net.write_all(&buf).await?;
        Ok(())
    }
    pub async fn flush(&self)->std::io::Result<()>{
        let mut net=self.netw.lock().await;
        net.flush().await?;
        Ok(())
    }
    // pub async fn hpack_encode(&self, headers: Vec<(&[u8], &[u8])>)->Vec<u8>{
    //     let mut he=self.hpacke.lock().await;
    //     he.encode(headers)
    // }
    pub async fn hpack_decode(&self, buf: &[u8])->Result<Vec<(Vec<u8>, Vec<u8>)>, hpack::decoder::DecoderError>{
        let mut hd=if let Ok(l)=self.hpackd.lock(){l}else{return Err(hpack::decoder::DecoderError::Invalid)};
        hd.decode(buf)
    }
    pub async fn incoming_frames(&self)->HttpResult<Vec<Http2Frame>>{
        // let mut reader=self.netr.lock().await;
        let mut que=self.que.lock().await;
        if que.is_empty(){
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
        } else {
            let frames = que.clone().into_iter().collect();
            que.clear();
            Ok(frames)
        }
    }
    
    pub async fn init(&self)->HttpResult<()>{
        let mut pref = [0u8; 24];
        let mut reader = self.netr.lock().await;

        reader.read_exact(&mut pref).await?;
        if pref!=MAGIC { Err(HttpError::InvalidPreface) }
        else { Ok(()) }
    }
    
    // pub async fn read_one(&self)
    async fn readone(&self)->Option<Http2Frame>{
        let mut reader=self.netr.lock().await;
        let mut buf=[0u8; 9];
        let mut full = Vec::new();
        reader.read_exact(&mut buf).await.ok()?;

        let len=(buf[0] as u32)<<16 | (buf[1] as u32)<<8 | buf[2] as u32;
        let len=len as usize;

        let mut buff = vec![0u8; len];
        reader.read_exact(&mut buff).await.ok()?;

        full.extend_from_slice(&buf);
        full.extend_from_slice(&buff);
        
        let (frame,_)=Http2Frame::parse(full)?;
        Some(frame)
    }
    pub async fn read_one(&self)->Option<Http2Frame>{
        let mut que=self.que.lock().await;
        if !que.is_empty(){ 
            que.pop_front()
        } else {
            self.readone().await
        }
    }

    pub async fn add_stream(&self, stream: Http2Stream)->HttpResult<()>{
        if self.streams.contains_key(&stream.stream_id) { return Err(HttpError::Invalid) };
        // let window_size=if let Some(s)=self.settings.lock().await.initial_window_size{s}else{0};
        self.streams.insert(stream.stream_id, stream);
        Ok(())
    }
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
                    if self.streams.contains_key(&frame.stream_id) { 
                        // rfc error
                    } else {
                        let window_size=if let Some(s)=self.settings.lock().await.initial_window_size{s}else{0};
                        let mut stream = Http2Stream {
                            stream_id: frame.stream_id,
                            end_headers: frame.flags.end_headers,
                            end_stream: frame.flags.end_stream,
                            valid: true,
                            window_size,
                            ..Http2Stream::empty()
                        };
                        stream.head.extend_from_slice(frame.get_payload());
                        
                        if stream.end_headers{
                            match self.hpack_decode(&stream.head).await{
                                Ok(d)=>stream.headers=d,
                                Err(_)=>stream.valid=false,
                            }
                        }
                        
                        new_streams.push(frame.stream_id);
                        self.streams.insert(frame.stream_id, stream);
                    }
                },
                Http2FrameType::Continuation=>{
                    if let Some(mut stream)=self.streams.get_mut(&frame.stream_id) { 
                        stream.head.extend_from_slice(frame.get_payload());
                        stream.end_headers=frame.flags.end_headers;
                        stream.end_stream=frame.flags.end_stream;

                        if stream.end_headers{
                            match self.hpack_decode(&stream.head).await{
                                Ok(d)=>stream.headers=d,
                                Err(_)=>stream.valid=false,
                            }
                        }
                    } else {
                        // rfc error
                    }
                },
                Http2FrameType::Data=>{
                    let p=frame.get_payload();

                    if let Some(mut stream)=self.streams.get_mut(&frame.stream_id){
                        stream.body.extend_from_slice(p);
                        stream.end_stream=frame.flags.end_stream;
                    } else {
                        // protocol error
                    }

                    self.send_window_update(frame.stream_id, p.len() as u32).await?;
                    self.send_window_update(0, p.len() as u32).await?;
                },
                Http2FrameType::Settings=>{
                    if !frame.flags.acknowledge&&frame.stream_id==0{
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
                                if self.streams.len()==0{
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
                                    // for stream in self.streams.values_mut(){
                                    //     let cs=stream.window_size as i64;
                                    //     let news=cs+diff;
                                    //     if news<0{
                                    //         stream.window_size=0
                                    //     } else if news>16777215{
                                    //         // protocol error
                                    //         stream.window_size=16777215
                                    //     } else {
                                    //         stream.window_size=(news&0x00FF_FFFF) as u32
                                    //     }
                                    // }
                                }
                                cset.initial_window_size=Some(v); 
                            };
                            if let Some(size)=settings.header_table_size{
                                if let Ok(mut enc)=self.hpacke.lock(){
                                    enc.header_table.dynamic_table.set_max_table_size(size as usize);
                                }
                            };

                            self.send_ackset().await?;
                        } else {
                            // protocol error
                        }
                    } else {
                        // rfc error
                    }
                },
                Http2FrameType::RstStream=>{
                    if let Some(mut stream)=self.streams.get_mut(&frame.stream_id){
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
                        } else if let Some(mut stream)=self.streams.get_mut(&frame.stream_id){
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
                Http2FrameType::Priority=>{},
                Http2FrameType::PushPromise=>{
                    // rfc error
                },
                Http2FrameType::Unknown(_u)=>{},
                // _=>()
            }
        };
        Ok(new_streams)
    }

    pub async fn send_data(&self,last: bool,stream_id: u32,payload: &[u8])->HttpResult<()>{
        let mut cws=self.window_size.lock().await;
        let mfs=self.settings.lock().await.max_frame_size.unwrap_or(16384) as usize;
        
        // let mut streams=self.streams.lock().await;
        let isws={
            let mut stream=if let Some(stream)=self.streams.get_mut(&stream_id){stream}else{ return Err(HttpError::StreamDoesntExist) };
            if stream.self_end_stream { return Err(HttpError::StreamClosed) };
            if stream.closed { return Err(HttpError::ConnectionClosed) };
            stream.self_end_stream = last;
            stream.window_size
        };
        // drop(streams);
        
        
        if payload.is_empty()&&last {
            let fb=create::raw_frame(stream_id, 0, 1, EMPTY, EMPTY)?;
            self.write(&fb).await?;
            return Ok(());
        } else if payload.is_empty(){
            return Err(HttpError::Invalid);
        }
        
        // let mut done = false;
        let mut min=cmp::min(isws,cmp::min(mfs as u32,*cws));//if *cws<mfs as u32 { *cws } else { mfs as u32 };
        let mut sent = 0;
        loop{
            if min==0{
                
            } else if payload.len()-sent > min as usize {
                
                let slice = &payload[sent..sent+min as usize];
                let fb = create::raw_frame(stream_id, 0, 0, slice, EMPTY)?;
                self.write(&fb).await?;
                
                let mut stream = if let Some(stream)=self.streams.get_mut(&stream_id){stream}else{ return Err(HttpError::StreamDoesntExist) };
                *cws -= slice.len() as u32;
                sent += slice.len();
                stream.window_size -= slice.len() as u32;

            } else if payload.len()-sent < min as usize {
                
                let slice = &payload[sent..];
                let fb = create::raw_frame(stream_id, 0, if last{1}else{0}, slice, EMPTY)?;
                self.write(&fb).await?;

                let mut stream = if let Some(stream)=self.streams.get_mut(&stream_id){stream}else{ return Err(HttpError::StreamDoesntExist) };
                *cws -= slice.len() as u32;
                stream.window_size -= slice.len() as u32;
                // sent += slice.len();
                println!("sent last part");
                return Ok(());

            }

            match self.readone().await{
                Some(frame)=>{
                    match frame.ftype{
                        Http2FrameType::Headers => {
                            let mut que = self.que.lock().await;
                            que.push_back(frame);
                        },
                        _ => { 
                            self.handle_frames(vec![frame]).await?;
                        },
                    }
                },
                None=>return Err(HttpError::Invalid),
            }
            
            let stream = if let Some(stream)=self.streams.get_mut(&stream_id){stream}else{ return Err(HttpError::StreamDoesntExist) };
            min=cmp::min(stream.window_size,cmp::min(mfs as u32,*cws));
        }
    }
    pub async fn send_headers(&self,last: bool,stream_id: u32,headers: Vec<(&[u8], &[u8])>)->HttpResult<()>{
        {
            let mut stream=if let Some(stream)=self.streams.get_mut(&stream_id){stream}else{ return Err(HttpError::StreamDoesntExist) };
            if stream.self_end_headers { return Err(HttpError::HeadersSent) }
            else if stream.closed||stream.self_end_stream { return Err(HttpError::ConnectionClosed) }
            else {
                stream.self_end_headers=true;
                stream.self_end_stream=last;
            };
        }
        let mfs=self.settings.lock().await.max_frame_size.unwrap_or(16384) as usize;
        let fl: u8=if last{5}else{4};
        let hb = {
            let mut he=if let Ok(l)=self.hpacke.lock(){l}else{return Err(HttpError::Invalid)};
            he.encode(headers)
        };
        
        if hb.len()<mfs{
            let fb = create::raw_frame(stream_id, 1, fl, &hb, EMPTY)?;
            // let mut netw = self.netw.lock().await; // "future is not `Send` as this value is used across an await"
            // netw.write(&fb).await;
            self.write(&fb).await?;
            Ok(())
        } else {
            let mut index = 0;

            while hb.len()-index>mfs{
                let fb = create::raw_frame(stream_id, 9, 0, &hb[index..index+mfs], EMPTY)?;
                self.write(&fb).await?;
                index+=mfs;
            }

            let fb = create::raw_frame(stream_id, 9, fl, &hb[index..index+mfs], EMPTY)?;
            self.write(&fb).await?;
            Ok(())
        }
    }
    pub async fn send_settings(&self,settings: Http2FrameSettings)->HttpResult<()>{
        let sb=settings.to_buff();
        let fb = create::raw_frame(0, 4, 0, &sb, EMPTY)?;
        self.write(&fb).await?;
        Ok(())
    }
    pub async fn send_ackset(&self)->HttpResult<()>{
        let fb = create::raw_frame(0, 4, 1, EMPTY, EMPTY)?;
        self.write(&fb).await?;
        Ok(())
    }
    pub async fn send_ping(&self, payload: &[u8; 8])->HttpResult<()>{
        // let payload=[0,0,0,0,0,0,0,1];  // length 8
        let fb = create::raw_frame(0, 6, 0, payload, EMPTY)?;
        self.write(&fb).await?;
        Ok(())
    }
    pub async fn send_pong(&self,payload: &[u8])->HttpResult<()>{
        let fb = create::raw_frame(0, 6, 1, payload, EMPTY)?;
        self.write(&fb).await?;
        Ok(())
    }
    pub async fn send_goaway(&self, stream_id: u32, error_code: u32, debug: &[u8])->HttpResult<()>{
        let mut buf: Vec<u8>=vec![
            (stream_id>>24)as u8,(stream_id>>16)as u8,(stream_id>>8)as u8,stream_id as u8,
            (error_code>>24)as u8,(error_code>>16)as u8,(error_code>>8)as u8,error_code as u8,
        ];
        buf.extend_from_slice(debug);
        let fb=create::raw_frame(0, 7, 0, &buf, EMPTY)?;
        self.write(&fb).await?;
        self.goaway.swap(false, Ordering::SeqCst);
        Ok(())
    }
    pub async fn send_rst_stream(&self, stream_id: u32, error_code: u32)->HttpResult<()>{
        let buf: Vec<u8>=vec![
            (error_code>>24)as u8,(error_code>>16)as u8,(error_code>>8)as u8,error_code as u8,
        ];
        let fb=create::raw_frame(stream_id, 7, 0, &buf, EMPTY)?;
        self.write(&fb).await?;
        self.goaway.swap(false, Ordering::SeqCst);
        Ok(())
    }
    pub async fn send_window_update(&self, stream_id: u32, size: u32)->HttpResult<()>{
        let buf: Vec<u8>=vec![
            (size>>24)as u8,(size>>16)as u8,(size>>8)as u8,size as u8,
        ];
        let fb=create::raw_frame(stream_id, 7, 0, &buf, EMPTY)?;
        self.write(&fb).await?;
        self.goaway.swap(false, Ordering::SeqCst);
        Ok(())
    }
}

impl<S:Stream> HttpConstructor<S> for Http2Session<S>{
    fn new(socket: S, addr: SocketAddr)->Self {
        Self::new(socket,addr,Http2FrameSettings::default())
    }
}
