// use std::{io, net::TcpStream};
// use std::future::Future;
// use crate::common::{HttpSocket, Stream};
// use crate::http1::handler::Http1Socket;

// use tokio::net::TcpListener;
// // use tokio::io::{AsyncReadExt, AsyncWriteExt};

// #[allow(unreachable_code)]
// pub async fn http_listener<F, O, Fut>(address: &str, listener: F)->io::Result<()>
// where F: Fn(Http1Socket<TcpStream>)->Fut + Send + Clone + Sync + 'static, Fut: Future<Output = O> + Send + 'static
// {
//     let server = TcpListener::bind(address).await?;
//     loop{
//         let (socket, addr) = server.accept().await?;
//         let listener=listener.clone();
//         tokio::spawn(async move{
//             let hand=Http1Socket::new(socket,addr);
//             listener(hand).await;
//         });
//     }
    
//     Ok(())
// }