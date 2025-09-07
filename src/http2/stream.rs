use std::collections::HashMap;
use std::net::SocketAddr;
// use std::sync::{Arc};
use std::sync::atomic::{AtomicBool,Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;

use crate::http2::core::*;
use crate::http2::create;
use crate::common::{HttpClient, HttpConstructor, HttpError, HttpResult, /*HttpSocket,*/ Stream};

// use hpack;

pub const MAGIC: &'static [u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"; // 0x505249202a20485454502f322e300d0a0d0a534d0d0a0d0a => 1969475691506423790601731136945089829455854996988862532874
const EMPTY: &'static [u8] = &[];

pub struct Http2Session<'a,S:Stream>{
    netr: Mutex<ReadHalf<S>>, netw: Mutex<WriteHalf<S>>,
    pub addr: SocketAddr,
    
    pub hpackd: Mutex<hpack::decoder::Decoder<'a>>, 
    pub hpacke: Mutex<hpack::encoder::Encoder<'a>>,

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

impl<'a,S:Stream> Http2Session<'a,S>{
    fn new(socket: S, addr: SocketAddr, initial_settings: Http2FrameSettings)->Self {
        let (read,write)=tokio::io::split(socket);
        let ws= if let Some(s)=initial_settings.initial_window_size{s}else{16384};
        let hts= if let Some(s)=initial_settings.header_table_size{s}else{4096};
        let mut dec=hpack::decoder::Decoder::new();
        dec.set_max_table_size(hts as usize);
        Self { 
            netr: Mutex::new(read), netw: Mutex::new(write), 
            addr:addr, 
            
            hpackd: Mutex::new(dec), 
            hpacke: Mutex::new(hpack::encoder::Encoder::new()),

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

    pub async fn add_stream(&self, stream_id: u32, client: HttpClient, settings: Http2FrameSettings)->HttpResult<()>{
        let mut streams=self.streams.lock().await;
        if streams.contains_key(&stream_id){ return Err(HttpError::Invalid) };
        let window_size=if let Some(s)=self.settings.lock().await.initial_window_size{s}else{0};
        let stream = Http2Stream {
            stream_id: stream_id,
            settings: settings,
            client: client,
            end_headers: false,
            end_stream: false,
            closed: false,
            window_size,
        };
        streams.insert(stream_id, stream);
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
                    // TODO: mix these two
                    let mut streams=self.streams.lock().await;
                    if let Some(stream)=streams.get_mut(&frame.stream_id){
                        // let mut client = stream.client.lock().await;
                        let pay=frame.get_payload();
                        let pay=if frame.flags.priority{&pay[5..]}else{pay};
                        let h=self.hpack_decode(pay).await;
                        if let Ok(headers)=h{
                            for (n,v) in headers{
                                let name=String::from_utf8_lossy(&n).to_string();
                                let value=String::from_utf8_lossy(&v).to_string();
                                if name == ":method" { stream.client.method = value }
                                // else if name == ":scheme" {  }
                                else if name == ":authority" { stream.client.host = value }
                                else if name == ":path" { stream.client.path = value }
                                else if !name.starts_with(":"){
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
                        let mut client=HttpClient {
                            version: "HTTP/2".to_owned(),
                            info: self.addr.clone(),
                            read: true,
                            ..Default::default()
                        };
                        let pay=frame.get_payload();
                        let pay=if frame.flags.priority{&pay[5..]}else{pay};
                        let h=self.hpack_decode(pay).await;
                        if let Ok(headers)=h{
                            for (n,v) in headers{
                                let name=String::from_utf8_lossy(&n).to_string();
                                let value=String::from_utf8_lossy(&v).to_string();
                                if name == ":method" { client.method = value }
                                // else if name == ":scheme" {  }
                                else if name == ":authority" { client.host = value }
                                else if name == ":path" { client.path = value }
                                else if !name.starts_with(":"){
                                    if let Some(hsv)=client.headers.get_mut(&name){
                                        hsv.push(value)
                                    } else {
                                        client.headers.insert(name, vec![value]);
                                    }
                                }
                            }
                        } else { 
                            // protocol error
                            // self.send_goaway(frame.stream_id, 1, b"header index out of bounds").await?;
                            // self.send_rst_stream(frame.stream_id, 1).await?;
                            // h.unwrap();
                            // println!("why {:?}",h);
                            // let hpackd=self.hpackd.lock().await;
                            // let sz=hpackd.header_table.dynamic_table.get_max_table_size();
                            // println!("max table size {}",sz);
                            client.read=false;
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
                                if let Some(size)=settings.header_table_size{
                                    let mut enc: tokio::sync::MutexGuard<'_, hpack::encoder::Encoder<'a>>=self.hpacke.lock().await;
                                    enc.header_table.dynamic_table.set_max_table_size(size as usize);

                                    // let mut nhpackd=hpack::decoder::Decoder::new();
                                    // hpack::decoder::Decoder::set_max_table_size(&mut nhpackd, size as usize);
                                    // *hpackdg=nhpackd;
                                    // hpack::Encoder::new
                                };

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
        if stream.end_headers { return Err(HttpError::HeadersSent) }
        else if stream.closed||stream.end_stream { return Err(HttpError::ConnectionClosed) }
        else {
            stream.end_headers=end_head;
            stream.end_stream=last;
        };
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
}

impl<'a,S:Stream> HttpConstructor<S> for Http2Session<'a,S>{
    fn new(socket: S, addr: SocketAddr)->Self {
        Self::new(socket,addr,Http2FrameSettings::default())
    }
}
