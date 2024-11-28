use std::{sync::Arc, time::Duration};

use aws_config::BehaviorVersion;
use aws_sdk_s3::types::ObjectLockLegalHoldStatus;
use base64::Engine;
use rand::{Rng as _, RngCore};
use serde::{Deserialize, Serialize};
use snafu::whatever;
use sqlite_vfs::{OpenAccess, OpenKind, Vfs};
use tokio::sync::RwLock;

use crate::{error::Error, handle::Handle};

#[derive(Clone)]
pub struct Inner {
    pub s3: aws_sdk_s3::Client,
    pub metadata_lock: S3FileLock,
    pub metadata_filename: String,
    pub current_lock: Option<Vec<u8>>,
    pub bucket: String,
    pub db_filename: String,
    // bucket: String,
    // lock_file: String,
    // current_lock: Arc<std::sync::Mutex<Option<Vec<u8>>>>,
}

pub trait Lock {
    async fn request_lock(&mut self) -> Vec<u8>;
    async fn release_lock(&mut self) -> Result<(), snafu::Whatever>;
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

#[derive(Clone)]
pub struct S3FileLock {
    pub s3: aws_sdk_s3::Client,
    pub bucket: String,
    pub lock_file: String,
    pub current_lock: Option<Vec<u8>>,
}

fn prepare_md5(uuid: &[u8; 16]) -> String {
    base64::prelude::BASE64_STANDARD.encode(md5::compute(uuid).as_ref())
}

impl Lock for S3FileLock {
    async fn request_lock(&mut self) -> Vec<u8> {
        let lock_uuid = uuid::Uuid::new_v4().to_bytes_le();
        let mut i = 0;
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
                    tracing::info!("Requesting lock ({i}): {lock_status:?}");
                    i += 1;

                    match lock_status.legal_hold.and_then(|status| status.status) {
                        Some(status) => {
                            if status == ObjectLockLegalHoldStatus::Off {
                                match self
                                    .s3
                                    .put_object()
                                    .bucket(&self.bucket)
                                    .key(&self.lock_file)
                                    .body(lock_uuid.to_vec().into())
                                    .content_md5(prepare_md5(&lock_uuid))
                                    .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
                                    .send()
                                    .await
                                {
                                    Ok(_) => {
                                        tracing::info!("put ok");
                                        // this might be unnecessary, but double checking for now
                                        {
                                            let val = self
                                                .s3
                                                .get_object()
                                                .bucket(self.bucket.clone())
                                                .key(self.lock_file.clone())
                                                .send()
                                                .await;
                                            if let Ok(obj) = val {
                                                if let Some(bytes) = obj.body.bytes() {
                                                    if bytes == lock_uuid.to_vec()
                                                        && obj.object_lock_legal_hold_status
                                                            == Some(ObjectLockLegalHoldStatus::On)
                                                    {
                                                        let vect = lock_uuid.to_vec();
                                                        self.current_lock = Some(vect.clone());
                                                        return vect;
                                                    } else {
                                                        panic!("1")
                                                    }
                                                } else {
                                                    panic!("2")
                                                }
                                            } else {
                                                panic!("3")
                                            }
                                        }

                                        // let vect = lock_uuid.to_vec();
                                        // self.current_lock = Some(vect.clone());
                                        // return vect;
                                    }
                                    Err(e) => {
                                        tracing::info!("put err: {e:?}");
                                    }
                                }
                            } else {
                                let val = self
                                    .s3
                                    .get_object()
                                    .bucket(self.bucket.clone())
                                    .key(self.lock_file.clone())
                                    .send()
                                    .await;
                                if let Ok(obj) = val {
                                    if let Some(bytes) = obj.body.bytes() {
                                        if bytes == lock_uuid.to_vec()
                                            && obj.object_lock_legal_hold_status
                                                == Some(ObjectLockLegalHoldStatus::On)
                                        {
                                            let vect = lock_uuid.to_vec();
                                            self.current_lock = Some(vect.clone());
                                            return vect;
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            panic!("bruhx skehfal.ksehfkaehfk");
                        }
                    }

                    // if lock_status
                    //     .legal_hold
                    //     .and_then(|status| status.status)
                    //     .is_some_and(|status| status == ObjectLockLegalHoldStatus::Off)
                    // {
                    // } else {
                    //     panic!("bruhx skehfal.ksehfkaehfk");
                    // }
                }
                Err(legal_status_error) => {
                    tracing::info!("err bruh");
                    // check if file exists
                    match self
                        .s3
                        .get_object()
                        .bucket(self.bucket.clone())
                        .key(self.lock_file.clone())
                        .send()
                        .await
                    {
                        Err(get_obj_err) => {
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
                                    self.current_lock = Some(vect.clone());
                                    return vect;
                                }
                                Err(_) => {
                                    panic!("Error creating lock file: {:?}", get_obj_err)
                                }
                            }
                        }
                        _ => {
                            panic!("Error getting legal hold status / but also it seems to exist ok: {:?}", legal_status_error)
                        }
                    }
                }
            }
            // for debugging, remove this later
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn release_lock(&mut self) -> Result<(), snafu::Whatever> {
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
                self.current_lock = None;
                Ok(())
            }
            Err(e) => whatever!("Error releasing lock: {}", e),
        }
    }
}

impl Inner {
    pub async fn read_exact_at(
        &mut self,
        offset: usize,
        len: usize,
    ) -> Result<Vec<u8>, snafu::Whatever> {
        let _ = self.request_read_lock().await;
        let data = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(&self.db_filename)
            .range(format!("bytes={}-{}", offset, offset + len))
            .send()
            .await;
        let _ = self.release_read_lock().await;

        match data {
            Ok(obj) => {
                if let Some(bytes) = obj.body.bytes() {
                    Ok(bytes.to_vec())
                } else {
                    whatever!("Error reading data: no bytes")
                }
            }
            Err(e) => whatever!("Error reading data: {}", e),
        }
    }

    pub async fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<(), snafu::Whatever> {
        let _ = self.request_write_lock().await;
        match self
            .s3
            .put_object()
            .bucket(&self.metadata_lock.bucket)
            .write_offset_bytes(offset as i64)
            .key(&self.db_filename)
            .body(data.to_vec().into())
            .send()
            .await
        {
            Ok(_) => {
                let _ = self.release_write_lock().await;
                Ok(())
            }
            Err(e) => {
                let _ = self.release_write_lock().await;
                whatever!("Error writing data: {}", e)
            }
        }
    }

    pub async fn get_database_size(&mut self) -> Result<i64, snafu::Whatever> {
        let _ = self.request_read_lock().await;
        let size = self
            .s3
            .get_object()
            .bucket(&self.metadata_lock.bucket)
            .key(&self.metadata_lock.lock_file)
            .send()
            .await;
        let _ = self.release_read_lock().await;

        match size {
            Ok(obj) => {
                if let Some(size) = obj.content_length {
                    Ok(size)
                } else {
                    whatever!("Error getting database size: no content length")
                }
            }
            Err(e) => whatever!("Error getting database size: {}", e),
        }
    }

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

    pub async fn release_read_lock(&mut self) -> Result<(), snafu::Whatever> {
        let _ = self.metadata_lock.request_lock().await;
        if let Metadata::Reader(read_metadata) = self.read_metadata().await? {
            let lock_uuid = self.current_lock.clone().unwrap();
            let new_readers = read_metadata
                .readers
                .into_iter()
                .filter(|v| v != &lock_uuid)
                .collect();

            self.write_metadata(Metadata::Reader(ReaderMetadata {
                readers: new_readers,
                write_request: read_metadata.write_request,
            }))
            .await?;
            self.metadata_lock.release_lock().await?;
            self.current_lock = None;
            // Ok(())
            Ok(())
        } else {
            self.metadata_lock.release_lock().await?;
            self.current_lock = None;
            whatever!("Error releasing read lock, no reader metadata found")
        }
    }

    pub async fn release_write_lock(&mut self) -> Result<(), snafu::Whatever> {
        let _ = self.metadata_lock.request_lock().await;
        if let Metadata::Writer(lock_uuid) = self.read_metadata().await? {
            if let Some(current_lock) = self.current_lock.clone() {
                if current_lock == lock_uuid {
                    self.write_metadata(Metadata::None).await?;
                    self.metadata_lock.release_lock().await?;
                    self.current_lock = None;
                    return Ok(());
                }
            }
        }
        self.metadata_lock.release_lock().await?;
        self.current_lock = None;
        whatever!("Error releasing write lock, no writer metadata found")
    }

    pub async fn request_read_lock(&mut self) -> Result<(), snafu::Whatever> {
        let lock_uuid = uuid::Uuid::new_v4().to_bytes_le();

        loop {
            let _ = self.metadata_lock.request_lock().await;

            let ready_for_reader = |meta: &Metadata| match meta {
                Metadata::None => true,
                Metadata::Writer(_) => false,
                Metadata::Reader(read_metadata) => read_metadata.write_request.is_none(),
            };

            let meta = self.read_metadata().await?;
            if ready_for_reader(&meta) {
                let mut current_readers = match meta {
                    Metadata::None => vec![],
                    Metadata::Writer(_) => vec![],
                    Metadata::Reader(read_metadata) => read_metadata.readers,
                };
                current_readers.push(lock_uuid.to_vec());
                self.write_metadata(Metadata::Reader(ReaderMetadata {
                    readers: current_readers,
                    write_request: None,
                }))
                .await?;
                break;
            }
        }
        self.current_lock = Some(lock_uuid.to_vec());
        Ok(())
    }

    pub async fn request_write_lock(&mut self) -> Result<(), snafu::Whatever> {
        let lock_uuid = uuid::Uuid::new_v4().to_bytes_le();

        let ready_for_writer = |meta: Metadata| match meta {
            Metadata::None => true,
            Metadata::Writer(_) => false,
            Metadata::Reader(read_metadata) => read_metadata.write_request.is_none(),
        };

        loop {
            let _ = self.metadata_lock.request_lock().await;

            let current_metadata = self.read_metadata().await?;

            if ready_for_writer(current_metadata.clone()) {
                self.write_metadata(Metadata::Writer(lock_uuid.to_vec()))
                    .await?;
                break;
            } else if let Metadata::Reader(read_metadata) = current_metadata {
                if read_metadata.readers.is_empty()
                    && read_metadata
                        .write_request
                        .clone()
                        .is_none_or(|v| v == lock_uuid.to_vec())
                {
                    self.write_metadata(Metadata::Writer(lock_uuid.to_vec()))
                        .await?;
                    break;
                } else if read_metadata.write_request.is_none() {
                    self.write_metadata(Metadata::Reader(ReaderMetadata {
                        readers: read_metadata.readers,
                        write_request: Some(lock_uuid.to_vec()),
                    }))
                    .await?;
                    break;
                    // return Ok(lock_uuid.to_vec());
                }
            }

            self.metadata_lock.release_lock().await?;

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.current_lock = Some(lock_uuid.to_vec());
        Ok(())
    }
}

#[derive(Clone)]
pub struct ThreeQLite {
    pub inner: Arc<RwLock<Inner>>,
}

impl ThreeQLite {
    pub async fn new() -> Self {
        let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let s3 = aws_sdk_s3::Client::new(&sdk_config);

        let bucket = "threeqlite".to_owned();

        Self {
            inner: Arc::new(RwLock::new(Inner {
                s3: s3.clone(),
                metadata_lock: S3FileLock {
                    s3,
                    bucket: bucket.clone(),
                    lock_file: "lockfile".to_owned(),
                    current_lock: None,
                },
                metadata_filename: "metadata".to_owned(),
                current_lock: None,
                bucket: bucket.clone(),
                db_filename: "test.db".to_owned(),
            })),
        }
    }
}

impl Vfs for ThreeQLite {
    type Handle = Handle;
    type Error = crate::error::Error;

    async fn open(
        &self,
        db: &str,
        opts: sqlite_vfs::OpenOptions,
    ) -> Result<Self::Handle, sqlite_vfs::error::Error<Self::Error>> {
        let sqlite_vfs::OpenOptions { kind, access, .. } = opts;

        match kind {
            OpenKind::MainDb => {}
            OpenKind::MainJournal => unimplemented!(),
            OpenKind::TempDb => unimplemented!(),
            OpenKind::TempJournal => unimplemented!(),
            OpenKind::TransientDb => unimplemented!(),
            OpenKind::SubJournal => unimplemented!(),
            OpenKind::SuperJournal => unimplemented!(),
            OpenKind::Wal => unimplemented!(),
        }

        match access {
            OpenAccess::Read => {
                // TODO: Throw if doesn't exist
            }
            OpenAccess::Write => {
                // TODO: Throw if doesn't exist
            }
            OpenAccess::Create => {
                // TODO: Ensure created
            }
            OpenAccess::CreateNew => {
                // TODO: Ensure created, throw if already exists
            }
        }

        Ok(Handle {
            storage: self.clone(),
            obj_key: db.to_owned(),
        })
    }

    async fn delete(&self, db: &str) -> Result<(), sqlite_vfs::error::Error<Self::Error>> {
        // if let Err(e) = self.inner.read().await.s3.get_object().send().await {
        //     return Err(Error::from_aws(e).into());
        // }

        Ok(())
    }

    async fn exists(&self, db: &str) -> Result<bool, sqlite_vfs::error::Error<Self::Error>> {
        Ok(true)
    }

    async fn temporary_name(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    async fn random(&self, buffer: &mut [i8]) {
        rand::thread_rng().fill(buffer);
    }

    fn sleep(&self, duration: std::time::Duration) -> std::time::Duration {
        std::thread::sleep(duration.clone());
        duration
    }
}
