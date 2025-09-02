use std::net::SocketAddr;

use crate::http2::core::*;
use crate::common::{Stream, HttpConstructor};

pub struct Http2Session<S:Stream>{
    net: S, addr: SocketAddr,
}

impl<S:Stream> HttpConstructor<S> for Http2Session<S>{
    fn new(socket: S, addr: SocketAddr)->Self {
        Self { net: socket, addr }
    }
}
