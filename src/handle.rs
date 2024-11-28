use sqlite_vfs::DatabaseHandle;

use crate::{vfs::ThreeQLite, wal::WalIndex};

#[derive(Clone)]
pub struct Handle {
    pub storage: ThreeQLite,
    pub obj_key: String,
}

impl DatabaseHandle for Handle {
    type WalIndex = WalIndex;
    type Error = crate::error::Error;

    async fn size(&self) -> Result<u64, sqlite_vfs::error::Error<Self::Error>> {
        todo!()
    }

    async fn read_exact_at(
        &mut self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(), sqlite_vfs::error::Error<Self::Error>> {
        todo!()
    }

    async fn write_all_at(
        &mut self,
        buf: &[u8],
        offset: u64,
    ) -> Result<(), sqlite_vfs::error::Error<Self::Error>> {
        todo!()
    }

    async fn sync(&mut self, data_only: bool) -> Result<(), sqlite_vfs::error::Error<Self::Error>> {
        todo!()
    }

    async fn set_len(&mut self, size: u64) -> Result<(), sqlite_vfs::error::Error<Self::Error>> {
        todo!()
    }

    async fn lock(
        &mut self,
        lock: sqlite_vfs::LockKind,
    ) -> Result<bool, sqlite_vfs::error::Error<Self::Error>> {
        todo!()
    }

    async fn reserved(&mut self) -> Result<bool, sqlite_vfs::error::Error<Self::Error>> {
        todo!()
    }

    async fn current_lock(
        &self,
    ) -> Result<sqlite_vfs::LockKind, sqlite_vfs::error::Error<Self::Error>> {
        todo!()
    }

    async fn wal_index(
        &self,
        readonly: bool,
    ) -> Result<Self::WalIndex, sqlite_vfs::error::Error<Self::Error>> {
        todo!()
    }
}
