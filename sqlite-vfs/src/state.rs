use std::{
    collections::HashMap,
    ffi::CString,
    io::ErrorKind,
    mem::MaybeUninit,
    pin::Pin,
    sync::{Arc, Mutex},
};

use crate::{wip, DatabaseHandle, Vfs};

pub struct State<V: Vfs> {
    pub name: CString,
    pub vfs: Arc<V>,
    #[cfg(any(feature = "syscall", feature = "loadext"))]
    parent_vfs: *mut sqlite3_sys::sqlite3_vfs,
    pub io_methods: sqlite3_sys::sqlite3_io_methods,
    pub last_error: Arc<Mutex<Option<(i32, crate::error::Error<V::Error>)>>>,
    pub next_id: usize,
}

#[repr(C)]
pub struct FileState<V: Vfs, F: DatabaseHandle> {
    pub base: sqlite3_sys::sqlite3_file,
    pub ext: MaybeUninit<FileExt<V, F>>,
}

#[repr(C)]
pub struct FileExt<V: Vfs, F: DatabaseHandle> {
    pub vfs: Arc<V>,
    pub vfs_name: CString,
    pub db_name: String,
    pub file: F,
    pub delete_on_close: bool,
    /// The last error; shared with the VFS.
    pub last_error: Arc<Mutex<Option<(i32, crate::error::Error<V::Error>)>>>,
    /// The last error number of this file/connection (not shared with the VFS).
    pub last_errno: i32,
    pub wal_index: Option<(F::WalIndex, bool)>,
    pub wal_index_regions: HashMap<u32, Pin<Box<[u8; 32768]>>>,
    pub wal_index_locks: HashMap<u8, wip::WalIndexLock>,
    pub has_exclusive_lock: bool,
    pub id: usize,
    pub chunk_size: Option<usize>,
    pub persist_wal: bool,
    pub powersafe_overwrite: bool,
}

impl<V: Vfs> State<V> {
    pub(crate) fn set_last_error(&mut self, no: i32, err: crate::error::Error<V::Error>) -> i32 {
        // log::error!("{} ({})", err, no);
        *(self.last_error.lock().unwrap()) = Some((no, err));
        no
    }
}

impl<V: Vfs, F: DatabaseHandle> FileExt<V, F> {
    pub(crate) fn set_last_error(&mut self, no: i32, err: crate::error::Error<V::Error>) -> i32 {
        // log::error!("{} ({})", err, no);
        *(self.last_error.lock().unwrap()) = Some((no, err));
        self.last_errno = no;
        no
    }
}

pub(crate) fn null_ptr_error<External>() -> crate::error::Error<External> {
    crate::error::Error::NullPtr
}

pub unsafe fn vfs_state<'a, V: Vfs>(
    ptr: *mut sqlite3_sys::sqlite3_vfs,
) -> Result<&'a mut State<V>, crate::error::Error<V::Error>> {
    let vfs: &mut sqlite3_sys::sqlite3_vfs = ptr.as_mut().ok_or_else(null_ptr_error)?;
    let state = (vfs.pAppData as *mut State<V>)
        .as_mut()
        .ok_or_else(null_ptr_error)?;
    Ok(state)
}

pub unsafe fn file_state<'a, V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    ptr: *mut sqlite3_sys::sqlite3_file,
) -> Result<&'a mut FileExt<V, F>, crate::error::Error<V::Error>> {
    let f = (ptr as *mut FileState<V, F>)
        .as_mut()
        .ok_or_else(null_ptr_error)?;
    let ext = f.ext.assume_init_mut();
    Ok(ext)
}
