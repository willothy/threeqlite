use std::{sync::Arc, time::Duration};

use aws_sdk_s3::types::ObjectLockLegalHoldStatus;
use serde::{Deserialize, Serialize};
use snafu::whatever;
use sqlite_vfs::Vfs;

use crate::handle::Handle;

struct Inner {
    s3: aws_sdk_s3::Client,
    metadata_lock: S3FileLock,
    metadata_filename: String,
    // bucket: String,
    // lock_file: String,
    // current_lock: Arc<std::sync::Mutex<Option<Vec<u8>>>>,
}

pub trait Lock {
    async fn request_lock(&self) -> Vec<u8>;
    async fn release_lock(&self) -> Result<(), snafu::Whatever>;
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Metadata {
    None,
    Writer(Vec<u8>),
    Reader(ReaderMetadata),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReaderMetadata {
    readers: Vec<Vec<u8>>,
    write_request: Option<Vec<u8>>,
}

pub struct S3FileLock {
    pub s3: aws_sdk_s3::Client,
    pub bucket: String,
    pub lock_file: String,
    pub current_lock: std::sync::Mutex<Option<Vec<u8>>>,
}

impl Lock for S3FileLock {
    async fn request_lock(&self) -> Vec<u8> {
        let lock_uuid = uuid::Uuid::new_v4().to_bytes_le();
        loop {
            match self
                .s3
                .get_object_legal_hold()
                .bucket(self.bucket.clone())
                .key(self.lock_file.clone())
                .send()
                .await
            {
                Ok(lock_status) => {
                    if let Some(status) = lock_status.legal_hold {
                        if let Some(status) = status.status {
                            if status == ObjectLockLegalHoldStatus::Off {
                                match self
                                    .s3
                                    .put_object()
                                    .bucket(&self.bucket)
                                    .key(&self.lock_file)
                                    .body(lock_uuid.to_vec().into())
                                    .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
                                    .send()
                                    .await
                                {
                                    Ok(_) => {
                                        let vect = lock_uuid.to_vec();
                                        *self.current_lock.lock().unwrap() = Some(vect.clone());
                                        return vect;
                                    }
                                    Err(_) => {

                                        // retry
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_) => {}
            }
        }
    }

    async fn release_lock(&self) -> Result<(), snafu::Whatever> {
        match self
            .s3
            .put_object()
            .bucket(&self.bucket)
            .key(&self.lock_file)
            .body(vec![].into())
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::Off)
            .send()
            .await
        {
            Ok(_) => {
                *self.current_lock.lock().unwrap() = None;
                Ok(())
            }
            Err(e) => whatever!("Error releasing lock: {}", e),
        }
    }
}

impl Inner {
    pub async fn write_metadata(&self, meta: Metadata) -> Result<(), snafu::Whatever> {
        let bytes = bincode::serialize(&meta).unwrap();
        match self
            .s3
            .put_object()
            .bucket(&self.metadata_lock.bucket)
            .key(&self.metadata_lock.lock_file)
            .body(bytes.into())
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => whatever!("Error writing metadata: {}", e),
        }
    }

    pub async fn read_metadata(&self) -> Result<Metadata, snafu::Whatever> {
        match self
            .s3
            .get_object()
            .bucket(&self.metadata_lock.bucket)
            .key(&self.metadata_lock.lock_file)
            .send()
            .await
        {
            Ok(obj) => {
                if let Some(bytes) = obj.body.bytes() {
                    Ok(bincode::deserialize(bytes).unwrap_or(Metadata::None))
                } else {
                    Ok(Metadata::None)
                }
            }
            Err(e) => whatever!("Error reading metadata: {}", e),
        }
    }

    pub async fn read_metadata_until(
        &self,
        until: Box<dyn Fn(Metadata) -> bool>,
        poll_delay: std::time::Duration,
    ) -> Result<Metadata, snafu::Whatever> {
        loop {
            let metadata = self.read_metadata().await?;
            if until(metadata.clone()) {
                return Ok(metadata);
            }
            tokio::time::sleep(poll_delay).await;
        }
    }
    pub async fn request_write_lock(&self) -> Result<Vec<u8>, snafu::Whatever> {
        let lock_uuid = uuid::Uuid::new_v4().to_bytes_le();

        loop {
            let _ = self.metadata_lock.request_lock().await;

            let current_metadata = self.read_metadata().await?;
            match current_metadata {
                Metadata::None => {
                    let metadata = Metadata::Writer(lock_uuid.to_vec());
                    self.write_metadata(metadata).await?;
                }
                Metadata::Writer(writer) => {
                    if writer == lock_uuid.to_vec() {
                        return Ok(lock_uuid.to_vec());
                    }
                    // for debugging, so the lock is not competed over
                    self.metadata_lock.release_lock().await?;
                    // read file until it is free
                    self.read_metadata_until(
                        Box::new(|meta| {
                            if let Metadata::None = meta {
                                true
                            } else {
                                false
                            }
                        }),
                        Duration::from_millis(10),
                    )
                    .await?;
                    // loop {
                    //     if let Metadata::None = self.read_metadata().await? {
                    //         break;
                    //     }
                    //     std::thread::sleep(std::time::Duration::from_millis(10));
                    // }
                    continue;

                    // metadata = Metadata::Writer(lock_uuid.to_vec());
                }
                Metadata::Reader(read_metadata) => {
                    if read_metadata.readers.is_empty()
                        && read_metadata
                            .write_request
                            .clone()
                            .is_none_or(|v| v == lock_uuid.to_vec())
                    {
                        Metadata::Writer(lock_uuid.to_vec());
                    } else if read_metadata.write_request.is_none() {
                        Metadata::Reader(ReaderMetadata {
                            readers: read_metadata.readers,
                            write_request: Some(lock_uuid.to_vec()),
                        });
                    }

                    // metadata = Metadata::Writer(lock_uuid.to_vec());
                }
            };
        }
        todo!()
    }
}

#[derive(Clone)]
pub struct ThreeQLite {
    inner: Arc<Inner>,
}

impl Vfs for ThreeQLite {
    type Handle = Handle;

    async fn open(
        &self,
        db: &str,
        opts: sqlite_vfs::OpenOptions,
    ) -> Result<Self::Handle, std::io::Error> {
        todo!()
    }

    async fn delete(&self, db: &str) -> Result<(), std::io::Error> {
        todo!()
    }

    async fn exists(&self, db: &str) -> Result<bool, std::io::Error> {
        todo!()
    }

    async fn temporary_name(&self) -> String {
        todo!()
    }

    async fn random(&self, buffer: &mut [i8]) {
        todo!()
    }

    fn sleep(&self, duration: std::time::Duration) -> std::time::Duration {
        todo!()
    }
}
