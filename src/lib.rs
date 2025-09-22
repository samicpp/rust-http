// use tokio::net::TcpListener;
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{Duration, sleep};
// use std::thread;
use std::sync::Arc;

use crate::common::HttpSocket;
use crate::common::HttpConstructor;
use crate::http2::Http2FrameSettings;
use crate::http2::Http2Handler;
use crate::http2::{stream::Http2Session, Http2FrameType};

// pub mod structs;
// pub mod traits;
pub mod http1;
pub mod http2;
pub mod http3;
pub mod websocket;
pub mod listener;
pub mod common;

#[allow(dead_code)]
async fn http1_serve() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:1024").await?;

    println!("http://localhost:1024");

    loop {
        let (socket, addr) = listener.accept().await?;

        println!("client connected");

        tokio::spawn(async move {
            let mut hand=http1::handler::Http1Socket::new(socket, addr);
            
            let _=hand.set_header("Content-Type", "text/html");
            // let r=hand.update_client().await;
            if let Err(err)=hand.update_client().await{
                eprintln!("client reading error: {:?}",err);
            };
            //dbg!(&hand.client);
            if let Err(err)=hand.write(b"<input/><br/>").await{
                eprintln!("writing error: {:?}",err);
            };
            sleep(Duration::from_millis(1500)).await;
            if let Err(err)=hand.close(format!("Hello, world at {}",hand.client.path).as_bytes()).await{
                eprintln!("closing error: {:?}",err);
            };
            println!("client said: {}",str::from_utf8(&hand.client.body).unwrap());
        });

        // tokio::spawn(async move {
        //     let mut buf = vec![0; 1 * 1024_usize.pow(2)]; // 1 mb
        //    
        //     let n = match socket.read(&mut buf).await {
        //         // socket closed
        //         Ok(0) => return,
        //         Ok(n) => n,
        //         Err(e) => {
        //             eprintln!("failed to read from socket; err = {:?}", e);
        //             return;
        //         }
        //     };
        //
        //     // Write the data back
        //     let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n").await;
        //     let _ = socket.write_all(&buf[0..n]).await;
        //     let _ = socket.shutdown().await;
        //    
        // });

    }
}

#[allow(dead_code)]
async fn http2_frame_dump(){
    let listener = tokio::net::TcpListener::bind("0.0.0.0:2048").await.expect("could not bind to port 2048");
    println!("\x1b[35mhttp://localhost:2048\x1b[0m");
    loop{
        let (tcp, addr)=listener.accept().await.expect("error during tcp acceptor");
        tokio::spawn(async move {
            println!("\x1b[33mtcp connection accepted from {}\x1b[0m",addr);
            let h2=Http2Session::new(tcp,addr);
            let mut f=h2.init().await.expect("failed to call init");
            // println!("\x1b[32minit frames\x1b[0m");
            // dbg!(&f);
            // let sock=http2::stream::Http2Socket::new(1, &h2);
            loop{
                if f.is_empty(){ println!("\x1b[31mconnection likely closed\x1b[0m"); break }
                println!("\x1b[32mreceived frames\x1b[0m");
                dbg!(&f);
                for frame in f{
                    match frame.ftype{
                        Http2FrameType::Headers=>{
                            println!("parsing headers");
                            let hraw=frame.get_payload();
                            let res=h2.hpack_decode(hraw).await.expect("couldnt parse hpack");
                            res.iter().for_each(|(name,value)|{
                                let ns=String::from_utf8_lossy(&name);
                                let vs=String::from_utf8_lossy(&value);
                                println!("{ns}: {vs}");
                            });
                        },
                        _=>(),
                    }
                }
                f=h2.incoming_frames().await.expect("error reading frames");
            }
        });
    }
}

// const SETTINGS:Http2FrameSettings=Http2FrameSettings{
//     header_table_size: None,
//     enable_push: None,
//     max_concurrent_streams: None,
//     initial_window_size: None,
//     max_frame_size: None,
//     max_header_list_size: None,
// };

#[allow(dead_code)]
async fn http2_serve(){
    let listener = tokio::net::TcpListener::bind("0.0.0.0:4096").await.expect("could not bind to port 4096");
    println!("\x1b[35mhttp://localhost:4096\x1b[0m");
    // unsafe{*(std::ptr::null() as *const u8)};
    loop{
        let (tcp, addr)=listener.accept().await.expect("error during tcp acceptor");
        tokio::spawn(async move {
            println!("\x1b[33mtcp connection accepted from {}\x1b[0m",addr);
            let h2=Arc::new(Http2Session::new(tcp,addr));
            let mut f=h2.init().await.expect("failed to call init");
            h2.send_settings(0, Http2FrameSettings::empty()).await.expect("failed to send own settings");
            // println!("\x1b[32minit frames\x1b[0m");
            // dbg!(&f);
            // let sock=http2::stream::Http2Socket::new(1, &h2);
            loop{
                if f.is_empty(){ println!("\x1b[31mconnection likely closed\x1b[0m"); break }
                // println!("\x1b[32mreceived frames\x1b[0m");
                for frame in &f{
                    if frame.flags.acknowledge { continue }
                    println!("type = \x1b[34m{:?}\x1b[0m",frame.ftype);
                    println!("frame = {frame:?}")
                };
                //dbg!(&f);
                let new=h2.handle_frames(f).await.expect("could not handle frames");
                // println!("opened streams: {}",new.len());
                h2.flush().await.expect("failed to flush");
                for stream_id in new{
                    println!("responding to stream {stream_id}");
                    let mut handle=Http2Handler::new(stream_id, Arc::clone(&h2));
                    tokio::spawn(async move{
                        let c=handle.read_client().await.expect("failed to read client");
                        dbg!(c);
                        handle.close(b"bytes\n").await.expect("failed to close");
                    });

                    // // manual
                    // h2.send_headers(false, true, stream_id, vec![(b":status",b"200")]).await.expect("failed to send headers");
                    // h2.send_data(true, stream_id, b"payload").await.expect("failed to send data");
                };
                f=h2.incoming_frames().await.expect("error reading frames");
            }
        });
    }
}

#[allow(dead_code)]
async fn http_upgrade(){
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8192").await.expect("failed to listen to 8192");

    println!("\x1b[33mhttp://localhost:8192\x1b[0m");

    loop {
        let (socket, addr) = listener.accept().await.expect("failed to accept tcp connection");

        println!("\x1b[32mclient connected\x1b[0m");

        tokio::spawn(async move {
            let mut hand=http1::handler::Http1Socket::new(socket, addr);
            
            let client=hand.read_client().await.expect("couldnt read client");
            
            match client.headers.get("upgrade"){
                Some(u) if u[0]=="h2c" =>{
                    let h2=hand.h2c().await.expect("failed to h2c upgrade");
                    let mut f=h2.init().await.expect("failed to call init");
                    h2.send_settings(0, Http2FrameSettings::empty()).await.expect("failed to send own settings");
                    
                    let _new=h2.handle_frames(f.clone()).await.expect("could not process frames");
                    h2.send_headers(false, true, 1, vec![(b":status",b"200")]).await.expect("failed to h2 send headers");
                    h2.send_data(true, 1, b"http2 upgrade succesfull\n").await.expect("failed to send h2 data");
                    
                    loop{
                        if f.len()==0{ println!("\x1b[31mhttp2 connection closed\x1b[0m"); break };
                        f=h2.incoming_frames().await.expect("error reading frames");
                    }
                },
                Some(u)=>{
                    println!("other upgrade request '{}'",u[0]);
                    hand.status=400;
                    hand.close(b"upgrade not implemented\n").await.expect("couldnt close connection");
                },
                None=>{
                    println!("client did not attempt upgrade");
                    hand.close(b"client did not attempt upgrade\n").await.expect("couldnt close connection");
                },
            }
        });
    }
}

#[cfg(test)]
mod test{
    use super::*;
    //use crate::http2;

    #[test]
    fn frame_read_test(){
        let frame_data: Vec<u8>=vec![0,0,11 , 0,0 , 0,0,0,1 , 104,101,108,108,111,32,119,111,114,108,100];
        // let frame=http2::Http2Frame::parse(frame_data.clone());
        match http2::Http2Frame::parse(frame_data.clone()){
            None=>panic!("test failed"),
            Some((frame,rem))=>{
                // let mut all=true;
                println!("\x1b[36mframe dump\x1b[0m");
                dbg!(&frame);
                dbg!(&rem);
                
                assert!(rem.len()==0,"remaining data for frame that shouldnt");
                assert!(frame.length==11,"incorrect length");
                assert!(frame.payload.len()==11,"incorrect payload length");
                
                assert!(frame.stream_id==1,"incorrect stream id");
                
                assert!(frame.ftype==http2::Http2FrameType::Data,"incorrect frame type");
                assert!(frame.type_int==0,"incorrect type int");

                assert!(frame.flags_int==0,"incorrect flags int");
                assert!(
                    !(frame.flags.acknowledge&&frame.flags.end_headers&&frame.flags.end_stream&&frame.flags.padded&&frame.flags.priority),
                    "incorrect frame flags"
                );

                assert!(frame.pad_length==0,"incorrect padding length");
                assert!(frame.padding.len()==0,"incorrect padding");

                assert!(str::from_utf8(frame.get_payload()).expect("could parse payload")=="hello world","invalid payload data");
            },
        }
    }
    
    #[test]
    fn create_frame_test(){
        let expected: Vec<u8> = vec![0,0,11 , 0,0 , 0,0,0,1 , 104,101,108,108,111,32,119,111,114,108,100];
        let frame=http2::create::raw_frame(1, 0, 0, "hello world".as_bytes(), &[]).expect("failed to create frame");
        let matching=frame.iter().zip(&expected).map(|(a,b)|a==b).count();
        
        println!("\x1b[35mframe dump\x1b[0m");
        dbg!(&frame);

        assert!(matching==expected.len(),"frame has a different length");
    }

    #[test]
    fn create_frame_from_frame_test(){
        let source: Vec<u8> = vec![0,0,11 , 0,0 , 0,0,0,1 , 104,101,108,108,111,32,119,111,114,108,100];
        let (frame,_) = http2::Http2Frame::parse(source.clone()).expect("couldnt parse");
        let back = http2::create::from_frame(frame).expect("could convert frame to buffer");

        let tot=back.iter().zip(&source).map(|(a,b)|a==b).count();
        assert!(tot==source.len(),"back different length than source");
    }

    #[test]
    fn create_settings_frame_test(){
        let ss=http2::Http2FrameSettings::default();
        let sb=ss.to_buff();
        let fb = http2::create::raw_frame(0, 4, 0, &sb, b"").expect("failed to create raw frame");
        println!("default settings frame dump {}",fb.len());
        dbg!(&fb);
        let (fr,rs)=http2::Http2Frame::parse(fb).expect("failed to parse frame");
        assert!(rs.len()==0,"rest buffer on single frame");
        dbg!(&fr);
    }

    #[test]
    #[ignore = "wont end"]
    fn http1_serve_test(){
        tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("could not build tokio runtime")
        .block_on(http1_serve());
    }

    #[test]
    #[ignore = "wont end"]
    fn http2_frame_dump_test(){
        tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("could not build tokio runtime")
        .block_on(http2_frame_dump());
    }

    #[test]
    #[ignore = "wont end"]
    fn http2_serve_test(){
        tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("could not build tokio runtime")
        .block_on(http2_serve());
    }

    #[test]
    #[ignore = "wont end"]
    fn upgrade_test(){
        tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("could not build tokio runtime")
        .block_on(http_upgrade());
    }
}