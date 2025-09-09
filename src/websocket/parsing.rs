// https://datatracker.ietf.org/doc/html/rfc6455

use std::{ops::Range};

// 5.2 #section-5.2
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
        }
    }
    pub fn parse(buf: Vec<u8>)->Option<Self>{
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
            (buf.get(2)?<<8) as u64 | *buf.get(3)? as u64
        } else if length==127 {
            offset+=8;
            (buf.get(2)?<<56) as u64 | (buf.get(3)?<<48) as u64 | (buf.get(4)?<<40) as u64 | (buf.get(5)?<<32) as u64 |
            (buf.get(6)?<<24) as u64 | (buf.get(7)?<<16) as u64 | (buf.get(8)?<<8) as u64 | *buf.get(9)? as u64
        } else {
            0
        };
        let act_length=if ext_length!=0{ ext_length }else{ length as u64 };
        let mask_key=if mask && buf.len()>=offset+4 { 
            let r=offset..offset+4;
            offset+=4; 
            r
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

        Some(Self { 
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
            // ..Self::empty()
        })
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

