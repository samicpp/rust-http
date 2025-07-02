pub struct HttpClient{
    pub read: bool,

    pub path: String,
    pub method: String,
    pub version: String,

    pub host: String,
    pub headers: std::collections::HashMap<String,Vec<String>>,

    pub info: std::net::SocketAddr,
}