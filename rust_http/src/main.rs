use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use std::thread;

use crate::traits::HttpSocket;

mod client;
mod http1;
mod traits;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:4096").await?;

    println!("http://localhost:4096");

    loop {
        let (socket, addr) = listener.accept().await?;

        println!("client connected");

        tokio::spawn(async move {
            let mut hand=http1::handler::Http1Socket::new(socket, addr);
            
            // let r=hand.update_client().await;
            if let Err(err)=hand.update_client().await{
                eprintln!("client reading error: {:?}",err);
            };
            //dbg!(&hand.client);
            if let Err(err)=hand.close(format!("Hello, world at {}",hand.client.path).as_bytes()).await{
                eprintln!("client writing error: {:?}",err);
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
