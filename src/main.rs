use std::net::{TcpListener, TcpStream};
//use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncRead, AsyncWrite};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:7878").await.unwrap();

    for stream in listener.accept() {
        match stream{
            Ok(st)=>{
                dbg!(st)
            },
            Err(e)=>continue,
        };
        //dbg!(stream);
    }
}