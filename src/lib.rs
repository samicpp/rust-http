// use tokio::net::TcpListener;
// use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{Duration, sleep};
// use std::thread;

use crate::common::HttpSocket;
use crate::common::HttpConstructor;

// pub mod structs;
// pub mod traits;
pub mod http1;
pub mod http2;
pub mod websocket;
pub mod listener;
pub mod common;

#[allow(dead_code)]
#[tokio::main]
async fn main_test() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:4096").await?;

    println!("http://localhost:4096");

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

#[cfg(test)]
mod test{
    use crate::http2::stream::Http2Session;

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
                
                assert!(rem.len()!=0,"remaining data for frame that shouldnt");
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
    #[ignore = "wont end"]
    fn http1_serve_test(){
        std::thread::spawn(move||{
            super::main_test().unwrap();
        }).join().unwrap();
    }

    #[test]
    #[ignore = "wont end"]
    fn http2_frame_dump(){
        tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("could not build tokio runtime")
        .block_on(async{
            let listener = tokio::net::TcpListener::bind("0.0.0.0:8192").await.expect("could not bind to port 8192");
            println!("\x1b[35mhttp://localhost:8192\x1b[0m");
            loop{
                let (tcp, addr)=listener.accept().await.expect("error during tcp acceptor");
                tokio::spawn(async move {
                    println!("\x1b[33mtcp connection accepted from {}\x1b[0m",addr);
                    let h2=Http2Session::new(tcp,addr);
                    let f=h2.init().await.expect("failed to call init");
                    println!("\x1b[32minit frames\x1b[0m");
                    dbg!(&f);

                    loop{
                        let f=h2.incoming_frames().await.expect("error reading frames");
                        println!("\x1b[32mreceived frames\x1b[0m");
                        dbg!(&f);
                    }
                });
            }
        });
    }
}