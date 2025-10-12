use crate::common::{HttpResult,HttpError};
use crate::http2::core::*;


pub mod create{
    use super::*;

    pub fn raw_frame(stream_id: u32, frame_type: u8, flags: u8, payload: &[u8], padding: &[u8])->HttpResult<Vec<u8>>{
        if payload.len()>0xffffff || padding.len()>256 { return Err(HttpError::FrameTooBig) }

        let stream_id=stream_id&2147483647; // 0x7FFF_FFFF
        let mut buff = Vec::new();
        let length = payload.len()+if !padding.is_empty(){ 1+padding.len() } else { 0 };

        let head = [
            (length >> 16) as u8,  //0xff0000
            (length >> 8) as u8,   //0x00ff00
            length as u8,          //0x0000ff
        
            frame_type,
            flags,

            (stream_id >> 24) as u8,   // 0xff000000
            (stream_id >> 16) as u8,   // 0x00ff0000
            (stream_id >> 8) as u8,    // 0x0000ff00
            stream_id as u8,           // 0x000000ff
        ];
        

        buff.extend_from_slice(&head);
        if !padding.is_empty(){ buff.push(padding.len() as u8) };
        buff.extend_from_slice(payload);
        buff.extend_from_slice(padding);
 
        Ok(buff)
    }
    pub fn from_frame(frame: Http2Frame)->HttpResult<Vec<u8>>{
        // let padding = frame.get_padding().to_vec();
        // let payload = frame.get_payload().clone().to_vec();
        raw_frame(frame.stream_id, frame.type_int, frame.flags_int, frame.get_payload(), frame.get_padding())
    }
}
