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
