use std::sync::Arc;

use sqlite_vfs::Vfs;

use crate::handle::Handle;

struct Inner {
    s3: aws_sdk_s3::Client,
}

#[derive(Clone)]
pub struct ThreeQLite {
    inner: Arc<Inner>,
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
