use std::sync::Arc;

use snafu::prelude::*;
use snafu::Snafu;
use sqlite_vfs::{DatabaseHandle, Vfs};

struct Inner {
    s3: aws_sdk_s3::Client,
}

#[derive(Clone)]
pub struct ThreeQLite {
    inner: Arc<Inner>,
}

#[derive(Clone)]
pub struct Handle {
    storage: ThreeQLite,
    obj_key: String,
}

pub struct WalIndex {}

impl sqlite_vfs::wip::WalIndex for WalIndex {
    fn enabled() -> bool {
        false
    }

    fn map(&mut self, region: u32) -> Result<[u8; 32768], std::io::Error> {
        todo!()
    }

    fn lock(
        &mut self,
        locks: std::ops::Range<u8>,
        lock: sqlite_vfs::wip::WalIndexLock,
    ) -> Result<bool, std::io::Error> {
        todo!()
    }

    fn delete(self) -> Result<(), std::io::Error> {
        todo!()
    }
}

impl DatabaseHandle for Handle {
    type WalIndex = WalIndex;

    fn size(&self) -> Result<u64, std::io::Error> {
        todo!()
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        todo!()
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error> {
        todo!()
    }

    fn sync(&mut self, data_only: bool) -> Result<(), std::io::Error> {
        todo!()
    }

    fn set_len(&mut self, size: u64) -> Result<(), std::io::Error> {
        todo!()
    }

    fn lock(&mut self, lock: sqlite_vfs::LockKind) -> Result<bool, std::io::Error> {
        todo!()
    }

    fn reserved(&mut self) -> Result<bool, std::io::Error> {
        todo!()
    }

    fn current_lock(&self) -> Result<sqlite_vfs::LockKind, std::io::Error> {
        todo!()
    }

    fn wal_index(&self, readonly: bool) -> Result<Self::WalIndex, std::io::Error> {
        todo!()
    }
}

impl Vfs for ThreeQLite {
    type Handle = Handle;

    fn open(
        &self,
        db: &str,
        opts: sqlite_vfs::OpenOptions,
    ) -> Result<Self::Handle, std::io::Error> {
        todo!()
    }

    fn delete(&self, db: &str) -> Result<(), std::io::Error> {
        todo!()
    }

    fn exists(&self, db: &str) -> Result<bool, std::io::Error> {
        todo!()
    }

    fn temporary_name(&self) -> String {
        todo!()
    }

    fn random(&self, buffer: &mut [i8]) {
        todo!()
    }

    fn sleep(&self, duration: std::time::Duration) -> std::time::Duration {
        todo!()
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    todo!()
}
