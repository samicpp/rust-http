// https://datatracker.ietf.org/doc/html/rfc6455

use std::{ops::Range};

// 5.2 #autoid-23
#[derive(Debug,Clone)]
pub struct WebSocketFrame{
    pub raw: Vec<u8>,

    // byte 0
    pub fin: u8,  // cant use name final
    pub rsv: u8,
    pub opcode: u8,

    // byte 1
    pub length: u8,
    pub mask: bool,

    pub ext_length: u64,
    pub mask_key: Range<usize>,
    
    pub act_length: u64,

    pub payload: Range<usize>,
    pub unmasked: Vec<u8>,

    pub ftype: WebSocketFrameType,
}

// 11.8 #autoid-82
#[derive(Debug,Clone,Copy)]
pub enum WebSocketFrameType{
                          // -+--------+-------------------------------------+-----------|
    Continuation,         //  | 0      | Continuation Frame                  | RFC 6455  |
                          // -+--------+-------------------------------------+-----------|
    Text,                 //  | 1      | Text Frame                          | RFC 6455  |
                          // -+--------+-------------------------------------+-----------|
    Binary,               //  | 2      | Binary Frame                        | RFC 6455  |
                          // -+--------+-------------------------------------+-----------|
    ConnectionClose,      //  | 8      | Connection Close Frame              | RFC 6455  |
                          // -+--------+-------------------------------------+-----------|
    Ping,                 //  | 9      | Ping Frame                          | RFC 6455  |
                          // -+--------+-------------------------------------+-----------|
    Pong,                 //  | 10     | Pong Frame                          | RFC 6455  |
                          // -+--------+-------------------------------------+-----------|
    Other(u8),
}

impl WebSocketFrame{
    pub fn empty()->Self{
        Self { 
            raw: Vec::new(), 
            fin: 0, 
            rsv: 0, 
            opcode: 0, 
            length: 0, 
            mask: false, 
            ext_length: 0, 
            mask_key: 0..0,
            act_length: 0,
            payload: 0..0,
            unmasked: Vec::new(), 
            ftype: WebSocketFrameType::Other(0),
        }
    }
    pub fn parse(mut buf: Vec<u8>)->Option<(Self,Vec<u8>)>{
        // let mut wsf = Self{
        //     raw: buf.to_vec(),
        //     ..Self::empty()
        // };
        let f=buf.get(0)?;
        let s=buf.get(1)?;
        let mut offset=2;

        let fin=f&0x80;
        let rsv=f&0b01110000; // 0x70;
        let opcode=f&0xF;
        let mask=(s&0x80)!=0;
        let length=s&0x7F;
        let ext_length=if length==126{
            offset+=2;
            (*buf.get(2)? as u64)<<8 | *buf.get(3)? as u64
        } else if length==127 {
            offset+=8;
            (*buf.get(2)? as u64)<<56 | (*buf.get(2)? as u64)<<48 | (*buf.get(2)?as u64)<<40 | (*buf.get(2)? as u64)<<32 |
            (*buf.get(2)? as u64)<<24 | (*buf.get(2)? as u64)<<16 | (*buf.get(2)? as u64)<<8 | *buf.get(9)? as u64
        } else {
            0
        };

        let act_length=if ext_length!=0{ ext_length }else{ length as u64 };
        let mask_key=if mask && buf.len()>=offset+4 { 
            let r=offset..offset+4;
            offset+=4; 
            r
        } else if buf.len()<offset+4 { 
            return None
        } else { 0..0 };
        
        let mask_slice=&buf[mask_key.clone()];
        let payload=if buf.len()>=offset+act_length as usize { 
            offset..offset+act_length as usize
        } else { 
            return None
        };

        let unmasked=if mask{
            let mut unmasked=(&buf[payload.clone()]).to_vec();
            for i in 0..payload.len(){
                unmasked[i] ^= mask_slice[i%4];
            };
            unmasked
        } else {
            Vec::new()
        };

        use WebSocketFrameType::*;
        let ftype=match opcode{
            0=>Continuation,
            1=>Text,
            2=>Binary,
            8=>ConnectionClose,
            9=>Ping,
            10=>Pong,
            o=>Other(o),
        };

        let tl=offset+act_length as usize;
        let rest=buf.split_off(tl);

        Some((Self { 
            raw: buf, 
            fin, 
            rsv, 
            opcode, 
            length, 
            mask, 
            ext_length,
            act_length,
            mask_key,
            payload,
            unmasked,
            ftype,
            // ..Self::empty()
        },rest))
    }

    pub fn get_payload(&self)->&[u8]{
        if self.mask{
            &self.unmasked
        } else {
            &self.raw[self.payload.clone()]
        }
    }
}

impl Default for WebSocketFrame{ fn default() -> Self { Self::empty() } }

