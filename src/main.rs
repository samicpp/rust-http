use tokio::net::{TcpListener,TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            // In a loop, read data from the socket and write the data back.
            loop {
                let n = match socket.read(&mut buf).await {
                    // socket closed
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                let rd=&buf[0..n];
                let str=std::str::from_utf8(rd).unwrap();
                println!("got something; {}",str);


                let hf=handler(&socket, &rd, &str);
                tokio::spawn(hf);
                //let _=hf.await;
                // Write the data back
                /*if let Err(e) = socket.write_all(rd).await {
                    eprintln!("failed to write to socket; err = {:?}", e);
                    return;
                }*/
            }
        });
    }
}


async fn handler(socket: &TcpStream, buff: &[u8], str: &str)->Result<()+Send,Box<dyn std::error::Error>>{
    //println!("handler called; str={}",str);
    dbg!(&socket,&buff,&str);
    if let Err(e)=socket.write_all(buff).await{eprintln!("had error {:?}",e);}else{println!("wrote back succesfully");};
    
    Ok(())
}