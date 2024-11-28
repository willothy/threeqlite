#![allow(clippy::question_mark)]
//! Create a custom SQLite virtual file system by implementing the [Vfs] trait and registering it
//! using [register].

pub mod io;
pub mod vfs;
pub mod state;

use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::{c_void, CStr, CString};
use std::io::ErrorKind;
use std::mem::{size_of, ManuallyDrop, MaybeUninit};
use std::ops::Range;
use std::os::raw::{c_char, c_int};
use std::pin::Pin;
use std::ptr::null_mut;
use std::slice;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A file opened by [Vfs].
pub trait DatabaseHandle: Sync {
    /// An optional trait used to store a WAL (write-ahead log).
    type WalIndex: wip::WalIndex;

    /// Return the current size in bytes of the database.
    async fn size(&self) -> Result<u64, std::io::Error>;

    /// Reads the exact number of byte required to fill `buf` from the given `offset`.
    async fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error>;

    /// Attempts to write an entire `buf` starting from the given `offset`.
    async fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error>;

    /// Make sure all writes are committed to the underlying storage. If `data_only` is set to
    /// `true`, only the data and not the metadata (like size, access time, etc) should be synced.
    async fn sync(&mut self, data_only: bool) -> Result<(), std::io::Error>;

    /// Set the database file to the specified `size`. Truncates or extends the underlying storage.
    async fn set_len(&mut self, size: u64) -> Result<(), std::io::Error>;

    /// Lock the database. Returns whether the requested lock could be acquired.
    /// Locking sequence:
    /// - The lock is never moved from [LockKind::None] to anything higher than [LockKind::Shared].
    /// - A [LockKind::Pending] is never requested explicitly.
    /// - A [LockKind::Shared] is always held when a [LockKind::Reserved] lock is requested
    async fn lock(&mut self, lock: LockKind) -> Result<bool, std::io::Error>;

    /// Unlock the database.
    async fn unlock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        self.lock(lock).await
    }

    /// Check if the database this handle points to holds a [LockKind::Reserved],
    /// [LockKind::Pending] or [LockKind::Exclusive] lock.
    async fn reserved(&mut self) -> Result<bool, std::io::Error>;

    /// Return the current [LockKind] of the this handle.
    async fn current_lock(&self) -> Result<LockKind, std::io::Error>;

    /// Change the chunk size of the database to `chunk_size`.
    async fn set_chunk_size(&self, _chunk_size: usize) -> Result<(), std::io::Error> {
        Ok(())
    }

    /// Check if the underlying data of the handle got moved or deleted. When moved, the handle can
    /// still be read from, but not written to anymore.
    async fn moved(&self) -> Result<bool, std::io::Error> {
        Ok(false)
    }

    async fn wal_index(&self, readonly: bool) -> Result<Self::WalIndex, std::io::Error>;
}

/// A virtual file system for SQLite.
pub trait Vfs: Sync {
    /// The file returned by [Vfs::open].
    type Handle: DatabaseHandle;

    /// Open the database `db` (of type `opts.kind`).
    async fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, std::io::Error>;

    /// Delete the database `db`.
    async fn delete(&self, db: &str) -> Result<(), std::io::Error>;

    /// Check if a database `db` already exists.
    async fn exists(&self, db: &str) -> Result<bool, std::io::Error>;

    /// Generate and return a path for a temporary database.
    async fn temporary_name(&self) -> String;

    /// Populate the `buffer` with random data.
    async fn random(&self, buffer: &mut [i8]);

    /// Sleep for `duration`. Return the duration actually slept.
    async fn sleep(&self, duration: Duration) -> Duration;

    /// Check access to `db`. The default implementation always returns `true`.
    async fn access(&self, _db: &str, _write: bool) -> Result<bool, std::io::Error> {
        Ok(true)
    }

    /// Retrieve the full pathname of a database `db`.
    async fn full_pathname<'a>(&self, db: &'a str) -> Result<Cow<'a, str>, std::io::Error> {
        Ok(db.into())
    }
}

pub mod wip {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(u16)]
    pub enum WalIndexLock {
        None = 1,
        Shared,
        Exclusive,
    }

    pub trait WalIndex: Sync {
        fn enabled() -> bool {
            true
        }

        fn map(&mut self, region: u32) -> Result<[u8; 32768], std::io::Error>;
        fn lock(&mut self, locks: Range<u8>, lock: WalIndexLock) -> Result<bool, std::io::Error>;
        fn delete(self) -> Result<(), std::io::Error>;

        fn pull(&mut self, _region: u32, _data: &mut [u8; 32768]) -> Result<(), std::io::Error> {
            Ok(())
        }

        fn push(&mut self, _region: u32, _data: &[u8; 32768]) -> Result<(), std::io::Error> {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenOptions {
    /// The object type that is being opened.
    pub kind: OpenKind,

    /// The access an object is opened with.
    pub access: OpenAccess,

    /// The file should be deleted when it is closed.
    delete_on_close: bool,
}

/// The object type that is being opened.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpenKind {
    MainDb,
    MainJournal,
    TempDb,
    TempJournal,
    TransientDb,
    SubJournal,
    SuperJournal,
    Wal,
}

/// The access an object is opened with.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpenAccess {
    /// Read access.
    Read,

    /// Write access (includes read access).
    Write,

    /// Create the file if it does not exist (includes write and read access).
    Create,

    /// Create the file, but throw if it it already exist (includes write and read access).
    CreateNew,
}

/// The access an object is opened with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockKind {
    /// No locks are held. The database may be neither read nor written. Any internally cached data
    /// is considered suspect and subject to verification against the database file before being
    /// used. Other processes can read or write the database as their own locking states permit.
    /// This is the default state.
    None,

    /// The database may be read but not written. Any number of processes can hold
    /// [LockKind::Shared] locks at the same time, hence there can be many simultaneous readers. But
    /// no other thread or process is allowed to write to the database file while one or more
    /// [LockKind::Shared] locks are active.
    Shared,

    /// A [LockKind::Reserved] lock means that the process is planning on writing to the database
    /// file at some point in the future but that it is currently just reading from the file. Only a
    /// single [LockKind::Reserved] lock may be active at one time, though multiple
    /// [LockKind::Shared] locks can coexist with a single [LockKind::Reserved] lock.
    /// [LockKind::Reserved] differs from [LockKind::Pending] in that new [LockKind::Shared] locks
    /// can be acquired while there is a [LockKind::Reserved] lock.
    Reserved,

    /// A [LockKind::Pending] lock means that the process holding the lock wants to write to the
    /// database as soon as possible and is just waiting on all current [LockKind::Shared] locks to
    /// clear so that it can get an [LockKind::Exclusive] lock. No new [LockKind::Shared] locks are
    /// permitted against the database if a [LockKind::Pending] lock is active, though existing
    /// [LockKind::Shared] locks are allowed to continue.
    Pending,

    /// An [LockKind::Exclusive] lock is needed in order to write to the database file. Only one
    /// [LockKind::Exclusive] lock is allowed on the file and no other locks of any kind are allowed
    /// to coexist with an [LockKind::Exclusive] lock. In order to maximize concurrency, SQLite
    /// works to minimize the amount of time that [LockKind::Exclusive] locks are held.
    Exclusive,
}


/// Register a virtual file system ([Vfs]) to SQLite.
pub fn register<F: DatabaseHandle, V: Vfs<Handle = F>>(
    name: &str,
    vfs: V,
    as_default: bool,
) -> Result<(), RegisterError> {
    let io_methods = sqlite3_sys::sqlite3_io_methods {
        iVersion: 2,
        xClose: Some(io::close::<V, F>),
        xRead: Some(io::read::<V, F>),
        xWrite: Some(io::write::<V, F>),
        xTruncate: Some(io::truncate::<V, F>),
        xSync: Some(io::sync::<V, F>),
        xFileSize: Some(io::file_size::<V, F>),
        xLock: Some(io::lock::<V, F>),
        xUnlock: Some(io::unlock::<V, F>),
        xCheckReservedLock: Some(io::check_reserved_lock::<V, F>),
        xFileControl: Some(io::file_control::<V, F>),
        xSectorSize: Some(io::sector_size::<F>),
        xDeviceCharacteristics: Some(io::device_characteristics::<V, F>),
        xShmMap: Some(io::shm_map::<V, F>),
        xShmLock: Some(io::shm_lock::<V, F>),
        xShmBarrier: Some(io::shm_barrier::<V, F>),
        xShmUnmap: Some(io::shm_unmap::<V, F>),
        xFetch: None,
        xUnfetch: None,
    };
    let name = CString::new(name).map_err(|e| RegisterError::Nul(e))?;
    let name_ptr = name.as_ptr();
    let ptr = Box::into_raw(Box::new(State {
        name,
        vfs: Arc::new(vfs),
        #[cfg(any(feature = "syscall", feature = "loadext"))]
        parent_vfs: unsafe { sqlite3_sys::sqlite3_vfs_find(std::ptr::null_mut()) },
        io_methods,
        last_error: Default::default(),
        next_id: 0,
    }));
    let vfs = Box::into_raw(Box::new(sqlite3_sys::sqlite3_vfs {
        #[cfg(not(feature = "syscall"))]
        iVersion: 2,
        #[cfg(feature = "syscall")]
        iVersion: 3,
        szOsFile: size_of::<FileState<V, F>>() as i32,
        mxPathname: MAX_PATH_LENGTH as i32, // max path length supported by VFS
        pNext: null_mut(),
        zName: name_ptr,
        pAppData: ptr as _,
        xOpen: Some(vfs::open::<F, V>),
        xDelete: Some(vfs::delete::<V>),
        xAccess: Some(vfs::access::<V>),
        xFullPathname: Some(vfs::full_pathname::<V>),
        xDlOpen: Some(vfs::dlopen::<V>),
        xDlError: Some(vfs::dlerror::<V>),
        xDlSym: Some(vfs::dlsym::<V>),
        xDlClose: Some(vfs::dlclose::<V>),
        xRandomness: Some(vfs::randomness::<V>),
        xSleep: Some(vfs::sleep::<V>),
        xCurrentTime: Some(vfs::current_time::<V>),
        xGetLastError: Some(vfs::get_last_error::<V>),
        xCurrentTimeInt64: Some(vfs::current_time_int64::<V>),

        #[cfg(not(feature = "syscall"))]
        xSetSystemCall: None,
        #[cfg(not(feature = "syscall"))]
        xGetSystemCall: None,
        #[cfg(not(feature = "syscall"))]
        xNextSystemCall: None,

        #[cfg(feature = "syscall")]
        xSetSystemCall: Some(vfs::set_system_call::<V>),
        #[cfg(feature = "syscall")]
        xGetSystemCall: Some(vfs::get_system_call::<V>),
        #[cfg(feature = "syscall")]
        xNextSystemCall: Some(vfs::next_system_call::<V>),
    }));

    let result = unsafe { sqlite3_sys::sqlite3_vfs_register(vfs, as_default as i32) };
    if result != sqlite3_sys::SQLITE_OK {
        return Err(RegisterError::Register(result));
    }

    // TODO: return object that allows to unregister (and cleanup the memory)?

    Ok(())
}

// TODO: add to [Vfs]?
const MAX_PATH_LENGTH: usize = 512;

#[repr(C)]
struct FileState<V, F: DatabaseHandle> {
    base: sqlite3_sys::sqlite3_file,
    ext: MaybeUninit<FileExt<V, F>>,
}

#[repr(C)]
struct FileExt<V, F: DatabaseHandle> {
    vfs: Arc<V>,
    vfs_name: CString,
    db_name: String,
    file: F,
    delete_on_close: bool,
    /// The last error; shared with the VFS.
    last_error: Arc<Mutex<Option<(i32, std::io::Error)>>>,
    /// The last error number of this file/connection (not shared with the VFS).
    last_errno: i32,
    wal_index: Option<(F::WalIndex, bool)>,
    wal_index_regions: HashMap<u32, Pin<Box<[u8; 32768]>>>,
    wal_index_locks: HashMap<u8, wip::WalIndexLock>,
    has_exclusive_lock: bool,
    id: usize,
    chunk_size: Option<usize>,
    persist_wal: bool,
    powersafe_overwrite: bool,
}

impl<V> State<V> {
    fn set_last_error(&mut self, no: i32, err: std::io::Error) -> i32 {
        // log::error!("{} ({})", err, no);
        *(self.last_error.lock().unwrap()) = Some((no, err));
        no
    }
}

impl<V, F: DatabaseHandle> FileExt<V, F> {
    fn set_last_error(&mut self, no: i32, err: std::io::Error) -> i32 {
        // log::error!("{} ({})", err, no);
        *(self.last_error.lock().unwrap()) = Some((no, err));
        self.last_errno = no;
        no
    }
}

impl OpenOptions {
    fn from_flags(flags: i32) -> Option<Self> {
        Some(OpenOptions {
            kind: OpenKind::from_flags(flags)?,
            access: OpenAccess::from_flags(flags)?,
            delete_on_close: flags & sqlite3_sys::SQLITE_OPEN_DELETEONCLOSE > 0,
        })
    }

    fn to_flags(&self) -> i32 {
        self.kind.to_flags()
            | self.access.to_flags()
            | if self.delete_on_close {
                sqlite3_sys::SQLITE_OPEN_DELETEONCLOSE
            } else {
                0
            }
    }
}

impl OpenKind {
    fn from_flags(flags: i32) -> Option<Self> {
        match flags {
            flags if flags & sqlite3_sys::SQLITE_OPEN_MAIN_DB > 0 => Some(Self::MainDb),
            flags if flags & sqlite3_sys::SQLITE_OPEN_MAIN_JOURNAL > 0 => Some(Self::MainJournal),
            flags if flags & sqlite3_sys::SQLITE_OPEN_TEMP_DB > 0 => Some(Self::TempDb),
            flags if flags & sqlite3_sys::SQLITE_OPEN_TEMP_JOURNAL > 0 => Some(Self::TempJournal),
            flags if flags & sqlite3_sys::SQLITE_OPEN_TRANSIENT_DB > 0 => Some(Self::TransientDb),
            flags if flags & sqlite3_sys::SQLITE_OPEN_SUBJOURNAL > 0 => Some(Self::SubJournal),
            flags if flags & sqlite3_sys::SQLITE_OPEN_SUPER_JOURNAL > 0 => Some(Self::SuperJournal),
            flags if flags & sqlite3_sys::SQLITE_OPEN_WAL > 0 => Some(Self::Wal),
            _ => None,
        }
    }

    fn to_flags(self) -> i32 {
        match self {
            OpenKind::MainDb => sqlite3_sys::SQLITE_OPEN_MAIN_DB,
            OpenKind::MainJournal => sqlite3_sys::SQLITE_OPEN_MAIN_JOURNAL,
            OpenKind::TempDb => sqlite3_sys::SQLITE_OPEN_TEMP_DB,
            OpenKind::TempJournal => sqlite3_sys::SQLITE_OPEN_TEMP_JOURNAL,
            OpenKind::TransientDb => sqlite3_sys::SQLITE_OPEN_TRANSIENT_DB,
            OpenKind::SubJournal => sqlite3_sys::SQLITE_OPEN_SUBJOURNAL,
            OpenKind::SuperJournal => sqlite3_sys::SQLITE_OPEN_SUPER_JOURNAL,
            OpenKind::Wal => sqlite3_sys::SQLITE_OPEN_WAL,
        }
    }
}

impl OpenAccess {
    fn from_flags(flags: i32) -> Option<Self> {
        match flags {
            flags
                if (flags & sqlite3_sys::SQLITE_OPEN_CREATE > 0)
                    && (flags & sqlite3_sys::SQLITE_OPEN_EXCLUSIVE > 0) =>
            {
                Some(Self::CreateNew)
            }
            flags if flags & sqlite3_sys::SQLITE_OPEN_CREATE > 0 => Some(Self::Create),
            flags if flags & sqlite3_sys::SQLITE_OPEN_READWRITE > 0 => Some(Self::Write),
            flags if flags & sqlite3_sys::SQLITE_OPEN_READONLY > 0 => Some(Self::Read),
            _ => None,
        }
    }

    fn to_flags(self) -> i32 {
        match self {
            OpenAccess::Read => sqlite3_sys::SQLITE_OPEN_READONLY,
            OpenAccess::Write => sqlite3_sys::SQLITE_OPEN_READWRITE,
            OpenAccess::Create => {
                sqlite3_sys::SQLITE_OPEN_READWRITE | sqlite3_sys::SQLITE_OPEN_CREATE
            }
            OpenAccess::CreateNew => {
                sqlite3_sys::SQLITE_OPEN_READWRITE
                    | sqlite3_sys::SQLITE_OPEN_CREATE
                    | sqlite3_sys::SQLITE_OPEN_EXCLUSIVE
            }
        }
    }
}

impl LockKind {
    fn from_i32(lock: i32) -> Option<Self> {
        Some(match lock {
            sqlite3_sys::SQLITE_LOCK_NONE => Self::None,
            sqlite3_sys::SQLITE_LOCK_SHARED => Self::Shared,
            sqlite3_sys::SQLITE_LOCK_RESERVED => Self::Reserved,
            sqlite3_sys::SQLITE_LOCK_PENDING => Self::Pending,
            sqlite3_sys::SQLITE_LOCK_EXCLUSIVE => Self::Exclusive,
            _ => return None,
        })
    }

    fn to_i32(self) -> i32 {
        match self {
            Self::None => sqlite3_sys::SQLITE_LOCK_NONE,
            Self::Shared => sqlite3_sys::SQLITE_LOCK_SHARED,
            Self::Reserved => sqlite3_sys::SQLITE_LOCK_RESERVED,
            Self::Pending => sqlite3_sys::SQLITE_LOCK_PENDING,
            Self::Exclusive => sqlite3_sys::SQLITE_LOCK_EXCLUSIVE,
        }
    }
}

impl PartialOrd for LockKind {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.to_i32().partial_cmp(&other.to_i32())
    }
}

impl Default for LockKind {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Default)]
pub struct WalDisabled;

impl wip::WalIndex for WalDisabled {
    fn enabled() -> bool {
        false
    }

    fn map(&mut self, _region: u32) -> Result<[u8; 32768], std::io::Error> {
        Err(std::io::Error::new(ErrorKind::Other, "wal is disabled"))
    }

    fn lock(
        &mut self,
        _locks: Range<u8>,
        _lock: wip::WalIndexLock,
    ) -> Result<bool, std::io::Error> {
        Err(std::io::Error::new(ErrorKind::Other, "wal is disabled"))
    }

    fn delete(self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum RegisterError {
    Nul(std::ffi::NulError),
    Register(i32),
}

impl std::error::Error for RegisterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Nul(err) => Some(err),
            Self::Register(_) => None,
        }
    }
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Nul(_) => f.write_str("interior nul byte in name found"),
            Self::Register(code) => {
                write!(f, "registering sqlite vfs failed with error code: {}", code)
            }
        }
    }
}

impl From<std::ffi::NulError> for RegisterError {
    fn from(err: std::ffi::NulError) -> Self {
        Self::Nul(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_order() {
        assert!(LockKind::None < LockKind::Shared);
        assert!(LockKind::Shared < LockKind::Reserved);
        assert!(LockKind::Reserved < LockKind::Pending);
        assert!(LockKind::Pending < LockKind::Exclusive);
    }
}
