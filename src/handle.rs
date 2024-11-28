use sqlite_vfs::DatabaseHandle;

use crate::{error::Error, vfs::ThreeQLite, wal::WalIndex};

#[derive(Clone)]
pub struct Handle {
    pub storage: ThreeQLite,
    pub obj_key: String,
}

impl DatabaseHandle for Handle {
    type WalIndex = WalIndex;
    type Error = crate::error::Error;

    async fn size(&mut self) -> Result<u64, sqlite_vfs::error::Error<Self::Error>> {
        let size = self.storage.inner.write().await.get_database_size().await;
        match size {
            Ok(size) => Ok(size as u64),
            Err(e) => Err(sqlite_vfs::error::Error::External {
                cause: crate::error::Error::FailedToGetDatabaseSize { msg: e.to_string() },
            }),
        }
    }

    async fn read_exact_at(
        &mut self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(), sqlite_vfs::error::Error<Self::Error>> {
        let data = self
            .storage
            .inner
            .write()
            .await
            .read_exact_at(offset as usize, buf.len())
            .await;
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
        let data = self
            .storage
            .inner
            .write()
            .await
            .write_at(offset as usize, buf)
            .await;
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

    async fn sync(
        &mut self,
        _data_only: bool,
    ) -> Result<(), sqlite_vfs::error::Error<Self::Error>> {
        Ok(())
    }

    async fn set_len(&mut self, size: u64) -> Result<(), sqlite_vfs::error::Error<Self::Error>> {
        let mut inner = self.storage.inner.write().await;

        inner.request_write_lock().await.unwrap();

        let obj = inner
            .s3
            .get_object()
            .bucket(&inner.bucket)
            .key(&self.obj_key)
            .send()
            .await
            .map_err(|e| Error::from_aws(e))?;

        let bytes = obj.body.collect().await.unwrap();

        let mut bytes = bytes.to_vec();

        if bytes.len() < size as usize {
            bytes.extend(std::iter::repeat(0).take(bytes.len() - size as usize));
        } else {
            bytes.truncate(size as usize);
        }

        let res = inner
            .s3
            .put_object()
            .bucket(&inner.bucket)
            .key(&self.obj_key)
            .body(bytes.into())
            .send()
            .await;

        inner.release_write_lock().await.unwrap();

        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::from_aws(e).into()),
        }
    }

    async fn lock(
        &mut self,
        lock: sqlite_vfs::LockKind,
    ) -> Result<bool, sqlite_vfs::error::Error<Self::Error>> {
        match lock {
            sqlite_vfs::LockKind::None => todo!(),
            sqlite_vfs::LockKind::Shared => todo!(),
            sqlite_vfs::LockKind::Reserved => todo!(),
            sqlite_vfs::LockKind::Pending => todo!(),
            sqlite_vfs::LockKind::Exclusive => todo!(),
        }
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
