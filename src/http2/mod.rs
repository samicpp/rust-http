pub mod core;
pub mod stream;
pub mod utils;
pub mod simple;

pub use core::*;
pub use utils::*;
pub use stream::*;
pub use simple::*;

// TODO: rewrite entire h2 implementation to be more like `samicpp/java-http`