use sqlite_vfs::DatabaseHandle;

pub struct WalIndex {}

impl sqlite_vfs::wip::WalIndex for WalIndex {
    fn enabled() -> bool {
        false
    }

    fn map<Handle: DatabaseHandle>(
        &mut self,
        region: u32,
    ) -> Result<[u8; 32768], sqlite_vfs::error::Error<Handle::Error>> {
        todo!()
    }

    fn lock<Handle: DatabaseHandle>(
        &mut self,
        locks: std::ops::Range<u8>,
        lock: sqlite_vfs::wip::WalIndexLock,
    ) -> Result<bool, sqlite_vfs::error::Error<Handle::Error>> {
        todo!()
    }

    fn delete<Handle: DatabaseHandle>(self) -> Result<(), sqlite_vfs::error::Error<Handle::Error>> {
        todo!()
    }
}
