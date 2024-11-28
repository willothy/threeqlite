#![allow(async_fn_in_trait)]


pub mod error;
pub mod handle;
pub mod vfs;
pub mod wal;

#[tokio::main]
async fn main() -> Result<(), crate::error::Error> {
    todo!()
}
