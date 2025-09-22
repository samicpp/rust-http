// https://httpwg.org/specs/rfc7540.html
// https://datatracker.ietf.org/doc/html/rfc7540
use std::{ops::Range, u32};

// TODO: properly parse priority data

// 4.1 #FrameHeader #autoid-13
#[derive(Debug,Clone)]
pub struct Http2Frame{
    pub raw: Vec<u8>,

    pub length: u32,
    pub payload: Range<usize>,
    pub stream_id: u32,

    pub ftype: Http2FrameType,
    pub type_int: u8,

    pub flags: Http2FrameFlags,
    pub flags_int: u8,

    pub pad_length: u8,
    pub padding: Range<usize>,

    pub settings: Option<Http2FrameSettings>,
}

// 11.2 #iana-frames #autoid-88
#[derive(Debug,Clone,Copy,PartialEq)]
pub enum Http2FrameType{
    Data,          // 0x0 Section 6.1  |
    Headers,       // 0x1 Section 6.2  |
    Priority,      // 0x2 Section 6.3  |
    RstStream,     // 0x3 Section 6.4  |
    Settings,      // 0x4 Section 6.5  |
    PushPromise,   // 0x5 Section 6.6  |
    Ping,          // 0x6 Section 6.7  |
    Goaway,        // 0x7 Section 6.8  |
    WindowUpdate,  // 0x8 Section 6.9  |
    Continuation,  // 0x9 Section 6.10 |
    Unknown(u8),
}

// 6 #FrameTypes #autoid-34
#[derive(Debug,Clone,Copy)]
pub struct Http2FrameFlags{
    pub acknowledge: bool, // 0x1   |  Settings, Ping

    pub end_stream: bool,  // 0x1   |  Headers, Data
    pub end_headers: bool, // 0x4   |  Headers, PushPromise, Continuation
    pub padded: bool,      // 0x8   |  Headers, Data, PushPromise
    pub priority: bool,    // 0x20  |  Headers
}

// 6.5 #SETTINGS #section-6.1
#[derive(Debug,Clone,Copy)]
pub struct Http2FrameSettings{
    pub header_table_size: Option<u32>,       // both    |  4,096     |  u32
    pub enable_push: Option<u32>,             // client  |  1         |  1 or 0
    pub max_concurrent_streams: Option<u32>,  // both    |  no limit  |  i32
    pub initial_window_size: Option<u32>,     // both    |  65,535    |  <2^31-1
    pub max_frame_size: Option<u32>,          // both    |  16,384    |  <16,777,215
    pub max_header_list_size: Option<u32>,    // both    |  no limit  |  u32
}

impl Http2Frame{
    pub fn get_payload<'a>(&'a self)->&'a [u8]{
        &self.raw[self.payload.clone()]
    }
    pub fn get_padding<'a>(&'a self)->&'a [u8]{
        &self.raw[self.padding.clone()]
    }

    pub fn empty()->Self{
        Self { 
            raw: Vec::new(), 
            length: 0, 
            payload: 0..0, 
            stream_id: 0, 
            ftype: Http2FrameType::Data, 
            type_int: 0, 
            flags: Http2FrameFlags::empty(), 
            flags_int: 0b00000000, 
            pad_length: 0, 
            padding: 0..0, 
            settings: None,
        }
    }

    pub fn parse(buff: Vec<u8>)->Option<(Self,Vec<u8>)>{
        if buff.len()<9 { return None };

        let length: u32 = (buff[0] as u32) << 16 | (buff[1] as u32) << 8 | (buff[2] as u32);
        let type_int = buff[3];
        let flags_int = buff[4];
        let stream_id = ((buff[5] as u32) << 24 | (buff[6] as u32) << 16 | (buff[7] as u32) << 8 | (buff[8] as u32)) & 0x7FFF_FFFF;

        let ftype = {
            use Http2FrameType::*;
            match type_int{
                0=>Data,
                1=>Headers,
                2=>Priority,
                3=>RstStream,
                4=>Settings,
                5=>PushPromise,
                6=>Ping,
                7=>Goaway,
                8=>WindowUpdate,
                9=>Continuation,
                u=>Unknown(u),
            }
        };
        let flags = Http2FrameFlags{
            acknowledge: (flags_int & 1) != 0,
            end_stream: (flags_int & 1) != 0,
            end_headers: (flags_int & 4) != 0,
            padded: (flags_int & 8) != 0,
            priority: (flags_int & 32) != 0,
            // ..Default::default()
        };

        let pad_length = if flags.padded && buff.len()>=10{
            buff[9]
        } else { 0 };

        let payload_start: usize = if flags.padded { 10 } else { 9 };
        let payload_end: usize = if buff.len()>=(length as usize+payload_start) { length as usize + payload_start } else { payload_start };
        let payload = Range{ start: payload_start, end: payload_end };

        let padding = if flags.padded{
            let padding_start = payload_end;
            let padding_end: usize = if buff.len()>=(pad_length as usize + padding_start) { pad_length as usize + padding_start } else { padding_start };
            Range{ start: padding_start, end: padding_end }
        } else { 0..0 };

        let settings=if ftype==Http2FrameType::Settings{Http2FrameSettings::parse(&buff[payload.clone()])}else{None};

        let mut buff=buff;
        let flen=9+length+pad_length as u32+if pad_length!=0{1}else{0};
        let flen=flen as usize;
        // println!("9+{length}+{pad_length} as u32+if {pad_length}!=0{{1}}else{{0}}");
        // println!("frame size {flen}");
        // println!("dump:\n{}",String::from_utf8_lossy(&buff));
        // if buff.len()<flen { return None };
        let remain=if buff.len()<flen { Vec::new() } else { buff.split_off(flen) };

        Some((Self {
            raw: buff,
            length,
            type_int,
            flags_int,
            stream_id,
            ftype,
            flags,
            pad_length,
            payload,
            padding,
            settings,
            // ..Default::default()
        },remain))
    }
}

impl Http2FrameFlags{
    pub fn empty()->Self{
        Self {
            acknowledge: false,
            end_stream: false,
            end_headers: false,
            padded: false,
            priority: false,
        }
    }
}

impl Http2FrameSettings{
    pub fn empty()->Self{
        Self { 
            header_table_size: None, 
            enable_push: None, 
            max_concurrent_streams: None, 
            initial_window_size: None, 
            max_frame_size: None, 
            max_header_list_size: None 
        }
    }
    pub fn parse(buf: &[u8])->Option<Self>{
        if buf.len()%6!=0 { return None };
        let buff=buf.chunks(6);
        let mut opt=Self::empty();
        for chunk in buff{
            let name=(chunk[0] as u16)<<8 | chunk[1] as u16;
            let value=(chunk[2] as u32)<<24 | (chunk[3] as u32)<<16 | (chunk[4] as u32)<<8 | chunk[5] as u32;
            match name{
                1=>opt.header_table_size=Some(value),
                2=>opt.enable_push=Some(value),
                3=>opt.max_concurrent_streams=Some(value),
                4=>opt.initial_window_size=Some(value),
                5=>opt.max_frame_size=Some(value),
                6=>opt.max_header_list_size=Some(value),
                _=>(), // invalid setting
            }
        }
        Some(opt)
    }
    pub fn to_buff(&self)->Vec<u8>{
        let mut buff=Vec::new();
        if let Some(v)=self.header_table_size{
            // let v=self.header_table_size;
            let buf=vec![0,1,(v>>24)as u8,(v>>16)as u8,(v>>8)as u8,v as u8];
            buff.extend_from_slice(&buf);
        } if let Some(v)=self.enable_push{
            let buf=vec![0,2,(v>>24)as u8,(v>>16)as u8,(v>>8)as u8,v as u8];
            buff.extend_from_slice(&buf);
        } if let Some(v)=self.max_concurrent_streams{
            let buf=vec![0,3,(v>>24)as u8,(v>>16)as u8,(v>>8)as u8,v as u8];
            buff.extend_from_slice(&buf);
        } if let Some(v)=self.initial_window_size{
            let buf=vec![0,4,(v>>24)as u8,(v>>16)as u8,(v>>8)as u8,v as u8];
            buff.extend_from_slice(&buf);
        } if let Some(v)=self.max_frame_size{
            let buf=vec![0,5,(v>>24)as u8,(v>>16)as u8,(v>>8)as u8,v as u8];
            buff.extend_from_slice(&buf);
        } if let Some(v)=self.max_header_list_size{
            let buf=vec![0,6,(v>>24)as u8,(v>>16)as u8,(v>>8)as u8,v as u8];
            buff.extend_from_slice(&buf);
        }
        buff
    }
}

impl Default for Http2Frame{ fn default() -> Self { Self::empty() } }
impl Default for Http2FrameFlags{ fn default() -> Self { Self::empty() } }
impl Default for Http2FrameSettings{
    fn default() -> Self {
        Self { 
            header_table_size: Some(4096), 
            enable_push: Some(1), 
            max_concurrent_streams: Some(i32::MAX as u32), 
            initial_window_size: Some(u16::MAX as u32), 
            max_frame_size: Some(16384), 
            max_header_list_size: Some(u32::MAX),
        }
    }
}
