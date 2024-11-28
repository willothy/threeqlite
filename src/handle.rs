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

    async fn size(&mut self) -> Result<u64, sqlite_vfs::error::Error<Self::Error>> {
        let size = self.storage.inner.get_database_size().await;
        match size {
            Ok(size) => Ok(size as u64),
            Err(e) => Err(sqlite_vfs::error::Error::External {
                cause: crate::error::Error::FailedToGetDatabaseSize {
                    msg: e.to_string(),
                },
            }),
        }
    }

    async fn read_exact_at(
        &mut self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(), sqlite_vfs::error::Error<Self::Error>> {
        let data = self.storage.inner.read_exact_at(offset as usize, buf.len()).await;
        match data {
            Ok(data) => {
                buf.copy_from_slice(&data);
                Ok(())
            }
            Err(e) => Err(sqlite_vfs::error::Error::External {
                cause: crate::error::Error::Whatever {
                    message: e.to_string(),
                    source: None,
                },
            }),
        }
    }

    async fn write_all_at(
        &mut self,
        buf: &[u8],
        offset: u64,
    ) -> Result<(), sqlite_vfs::error::Error<Self::Error>> {
        let data = self.storage.inner.write_at(offset as usize, buf).await;
        match data {
            Ok(_) => Ok(()),
            Err(e) => Err(sqlite_vfs::error::Error::External {
                cause: crate::error::Error::Whatever {
                    message: e.to_string(),
                    source: None,
                },
            }),
        }
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
