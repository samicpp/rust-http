use std::net::SocketAddr;
use crate::common::{HttpConstructor, HttpResult, Stream};
use crate::websocket::{parsing::*};
use tokio::io::{AsyncWriteExt,AsyncReadExt};

pub const MAGIC:&'static [u8]=b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// #[derive(Debug)]
pub struct WebSocket<S:Stream+Send+Sync>{
    tcp: S,  // named tcp, even though not strictly tcp
    pub addr: SocketAddr,
}

impl<S:Stream+Send+Sync> WebSocket<S>{
    async fn read_all(&mut self)->std::io::Result<Vec<u8>>{
        let mut buf=[0u8; 4096];
        let mut total = Vec::new();
        loop{
            let n=self.tcp.read(&mut buf).await?;
            if n==0{ break };
            total.extend_from_slice(&buf[..n]);
            if n<buf.len(){
                break
            }
        }
        Ok(total)
    }
    fn create_frame(fin:bool,opcode:u8,payload:&[u8])->Vec<u8>{
        let mut b=vec![
            if fin{ 0x80 } else { 0x0 } | (opcode&0xF),
            0//if payload.len()>127 { 127 } else if payload.len()>126 { 126 } else { payload.len() as u8 },
        ];
        if payload.len()<126{
            b[1]=payload.len() as u8;
        } else if payload.len() <= u16::MAX as usize{
            b[1]=126;
            let el=vec![
                (payload.len()>>8)as u8, payload.len() as u8, 
            ];
            b.extend_from_slice(&el);
        } else { 
            b[1]=127;
            let el=vec![
                (payload.len()>>56)as u8, (payload.len()>>48)as u8, (payload.len()>>40)as u8, (payload.len()>>32)as u8, 
                (payload.len()>>24)as u8, (payload.len()>>16)as u8, (payload.len()>>8)as u8, payload.len() as u8, 
            ]; 
            // to (payload.len() as u64).to_be_bytes()
            b.extend_from_slice(&el);
        };
        b.extend_from_slice(payload);
        b
    }
    pub fn incoming(&mut self)->impl Future<Output = HttpResult<Vec<WebSocketFrame>>>{
        async move {
            let mut b=self.read_all().await?;
            let mut frames=vec![];
            loop{
                match WebSocketFrame::parse(b){
                    None=>break,
                    Some((frame,r)) if r.is_empty()=>{
                        frames.push(frame);
                        break
                    },
                    Some((frame,rest))=>{
                        frames.push(frame);
                        b=rest;
                    },
                }
            }
            Ok(frames)
        }
    }
    pub async fn send_text(&mut self,text: &[u8])->HttpResult<()>{
        let fb=Self::create_frame(true, 1, text);
        self.tcp.write_all(&fb).await?;
        Ok(())
    }
    pub async fn send_binary(&mut self, bin: &[u8])->HttpResult<()>{
        let fb=Self::create_frame(true, 2, bin);
        self.tcp.write_all(&fb).await?;
        Ok(())
    }
    pub async fn send_ping(&mut self)->HttpResult<()>{
        let fb=Self::create_frame(true, 9, &[0,0,0,1]);
        self.tcp.write_all(&fb).await?;
        Ok(())
    }
    pub async fn send_pong(&mut self, pay: &[u8])->HttpResult<()>{
        let fb=Self::create_frame(true, 10, pay);
        self.tcp.write_all(&fb).await?;
        Ok(())
    }
    pub async fn send_close(&mut self, status: u16,reason: &[u8])->HttpResult<()>{
        let mut b=vec![ (status<<8) as u8, status as u8 ];
        b.extend_from_slice(reason);
        let fb=Self::create_frame(true, 8, &b);
        self.tcp.write_all(&fb).await?;
        Ok(())
    }
}

impl<S:Stream> HttpConstructor<S> for WebSocket<S>{
    fn new(socket: S, addr: std::net::SocketAddr)->Self {
        Self { 
            tcp: socket, addr 
        }
    }
}
