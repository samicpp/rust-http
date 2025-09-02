use std::net::SocketAddr;

use crate::http2::core::*;
use crate::common::{Stream, HttpConstructor};

pub struct Http2Stream<S:Stream>{
    net: S, addr: SocketAddr,
}

impl<S:Stream> HttpConstructor<S> for Http2Stream<S>{
    fn new(socket: S, addr: SocketAddr)->Self {
        Http2Stream { net: socket, addr }
    }
}
