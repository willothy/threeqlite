use std::sync::Arc;

use aws_sdk_s3::types::ObjectLockLegalHoldStatus;
use sqlite_vfs::Vfs;

use crate::handle::Handle;

struct Inner {
    s3: aws_sdk_s3::Client,
    bucket: String,
    lock_file: String,
    current_lock: Arc<std::sync::Mutex<Option<Vec<u8>>>>,
}

pub trait Lock {
    async fn request_lock(&self) -> Result<Vec<u8>, std::io::Error>;
}

struct S3FileLock {
    s3: aws_sdk_s3::Client,
    bucket: String,
    lock_file: String,
    current_lock: std::sync::Mutex<Option<Vec<u8>>>,
}


impl Lock for S3FileLock {
    async fn request_lock(&self) -> Result<Vec<u8>, std::io::Error> {
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
                                        return Ok(vect);
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
}


impl Inner {
    pub async fn request_lock(&self) -> Result<Vec<u8>, std::io::Error> {
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
                                        return Ok(vect);
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
