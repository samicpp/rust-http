use std::net::SocketAddr;
use std::sync::{Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;

use crate::http2::core::*;
use crate::common::{HttpConstructor, HttpError, HttpResult, Stream};

// use hpack;

pub const MAGIC: &'static [u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"; // 0x505249202a20485454502f322e300d0a0d0a534d0d0a0d0a => 1969475691506423790601731136945089829455854996988862532874

pub struct Http2Session<'a,S:Stream>{
    netr: Mutex<ReadHalf<S>>, netw: Mutex<WriteHalf<S>>,
    pub addr: SocketAddr,
    
    hpackd: Mutex<hpack::Decoder<'a>>, 
    hpacke: Mutex<hpack::Encoder<'a>>,

    pub settings: Http2FrameSettings,
}

pub struct Http2Stream{
    pub settings: Http2FrameSettings,
}

impl<'a,S:Stream> Http2Session<'a,S>{
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
    pub async fn write(&self,buf:&[u8])->std::io::Result<()>{
        let mut net=self.netw.lock().await;
        net.write_all(buf).await?;
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


}

impl<'a,S:Stream> HttpConstructor<S> for Http2Session<'a,S>{
    fn new(socket: S, addr: SocketAddr)->Self {
        let (read,write)=tokio::io::split(socket);
        Self { 
            netr: Mutex::new(read), netw: Mutex::new(write), 
            addr:addr, 
            
            hpackd: Mutex::new(hpack::Decoder::new()), 
            hpacke: Mutex::new(hpack::Encoder::new()),

            settings: Default::default(),
        }
    }
}
