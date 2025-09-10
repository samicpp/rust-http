use std::{io};
use std::future::Future;
use crate::common::{HttpConstructor, HttpSocket, /*Stream*/};
// use crate::http1::handler::Http1Socket;

use tokio::net::{TcpListener, TcpStream};
// use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[allow(unreachable_code)]
pub async fn http_listener<'a, F, S, O, Fut>(address: &str, listener: F)->io::Result<()>
where F: Fn(S)->Fut + Send + Clone + Sync + 'static,
    S: HttpSocket+HttpConstructor<TcpStream>,
    Fut: Future<Output = O> + Send + 'static
{
    let server = TcpListener::bind(address).await?;
    loop{
        let (socket, addr) = server.accept().await?;
        let listener=listener.clone();
        tokio::spawn(async move{
            let hand=S::new(socket,addr);
            listener(hand).await;
        });
    }
    
    Ok(())
}
