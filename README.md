# rust-http
HTTP framework in rust


## Todo::Enhancements
- [x] Custom Result enums instead of using std:\:io::result
- [x] make stream argument type work for any stream
- [x] remove `fn new()` from trait `HttpSocket`
- [x] refactoring of traits structs and enums
- [x] make a stream trait to make socket structs generic
- [x] rewrite HTTP/2 implementation to be more like `samicpp/java-http`
- [x] rewrite HTTP/1.1 implementation to be more like `samicpp/dotnet-http`
- [ ] rewrite HTTP/2 implementation to be more like `samicpp/dotnet-http`

## Todo::Features
- [x] implement HTTP/1.1
- [x] implement WebSocket
- [x] implement HTTP/2
- [ ] implement HTTP/3
- [ ] implement QUIC

## Credits
This repo makes use of [mlalic/hpack](https://github.com/mlalic/hpack-rs)
