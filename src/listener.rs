use std::io;
use std::future::Future;
use crate::{traits::HttpSocket};

use tokio::net::TcpListener;
// use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[allow(unreachable_code)]
pub async fn http_listener<S, F, O, Fut>(address: &str, listener: F)->io::Result<()>
where F: Fn(S)->Fut + Send + Clone + Sync + 'static, S: HttpSocket, Fut: Future<Output = O> + Send + 'static
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