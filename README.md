# rust-http
HTTP framework in rust


## TODO
1. [x] Custom Result enums instead of using std:\:io::result
2. [x] make stream argument type work for tls, tcp, QUIC with a trait (QUIC not tested yet)
3. [x] remove `fn new()` from trait `HttpSocket`
4. [x] implement HTTP/2
5. [x] implement WebSocket
6. [x] refactoring of traits structs and enums
7. [x] make a stream trait to make socket structs generic

## Credits
This repo makes use of [mlalic/hpack](https://github.com/mlalic/hpack-rs)
