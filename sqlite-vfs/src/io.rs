use std::collections::hash_map::Entry;
use std::mem;

use super::*;
use error::Error;
use state::{file_state, null_ptr_error, FileState};
use wip::WalIndex;

#[tokio::main]
async fn close_inner<V: Vfs, F: DatabaseHandle>(file: *mut libsqlite3_sys::sqlite3_file) -> c_int {
    if let Some(f) = unsafe { (file as *mut FileState<V, F>).as_mut() } {
        let ext = unsafe { f.ext.assume_init_mut() };
        if ext.delete_on_close {
            if let Err(err) = Vfs::delete(&*ext.vfs, &*ext.db_name).await {
                return ext.set_last_error(libsqlite3_sys::SQLITE_DELETE, err);
            }
        }

        let ext = mem::replace(&mut f.ext, MaybeUninit::uninit());
        let ext = unsafe { ext.assume_init() }; // extract the value to drop it
        log::trace!("[{}] close ({})", ext.id, ext.db_name);
    }

    // #[cfg(feature = "sqlite_test")]
    // libsqlite3_sys::sqlite3_dec_open_file_count();

    libsqlite3_sys::SQLITE_OK
}

/// Read data from a file.
#[tokio::main]
pub async fn read_inner<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    z_buf: *mut c_void,
    i_amt: c_int,
    i_ofst: libsqlite3_sys::sqlite3_int64,
) -> c_int {
    let state = unsafe {
        match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return libsqlite3_sys::SQLITE_IOERR_CLOSE,
        }
    };
    // log::trace!(
    //     "[{}] read offset={} len={} ({})",
    //     state.id,
    //     i_ofst,
    //     i_amt,
    //     state.db_name
    // );

    let out = unsafe { slice::from_raw_parts_mut(z_buf as *mut u8, i_amt as usize) };
    if let Err(err) = state.file.read_exact_at(out, i_ofst as u64).await {
        if let crate::error::Error::UnexpectedEof = err {
            return libsqlite3_sys::SQLITE_IOERR_SHORT_READ;
        }
        return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_READ, err);
    }

    libsqlite3_sys::SQLITE_OK
}

/// Write data to a file.
#[tokio::main]
pub async unsafe fn write_inner<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    z: *const c_void,
    i_amt: c_int,
    i_ofst: libsqlite3_sys::sqlite3_int64,
) -> c_int {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_IOERR_WRITE,
    };
    log::trace!(
        "[{}] write offset={} len={} ({})",
        state.id,
        i_ofst,
        i_amt,
        state.db_name
    );

    let data = slice::from_raw_parts(z as *mut u8, i_amt as usize);
    let result = state.file.write_all_at(data, i_ofst as u64).await;

    match result {
        Ok(_) => {}
        Err(Error::WriteZero) => {
            return libsqlite3_sys::SQLITE_FULL;
        }
        Err(err) => return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_WRITE, err),
    }

    libsqlite3_sys::SQLITE_OK
}

/// Truncate a file.
#[tokio::main]
pub async unsafe fn truncate_inner<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    size: libsqlite3_sys::sqlite3_int64,
) -> c_int {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_IOERR_FSYNC,
    };

    let size: u64 = if let Some(chunk_size) = state.chunk_size {
        (((size as usize + chunk_size - 1) / chunk_size) * chunk_size) as u64
    } else {
        size as u64
    };

    log::trace!("[{}] truncate size={} ({})", state.id, size, state.db_name);

    // #[cfg(feature = "sqlite_test")]
    // if simulate_io_error() {
    //     return libsqlite3_sys::SQLITE_IOERR_TRUNCATE;
    // }

    if let Err(err) = state.file.set_len(size).await {
        return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_TRUNCATE, err);
    }

    libsqlite3_sys::SQLITE_OK
}

/// Persist changes to a file.
#[tokio::main]
pub async unsafe fn sync_inner<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    flags: c_int,
) -> c_int {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_IOERR_FSYNC,
    };
    log::trace!("[{}] sync ({})", state.id, state.db_name);

    if let Err(err) = state
        .file
        .sync(flags & libsqlite3_sys::SQLITE_SYNC_DATAONLY > 0)
        .await
    {
        return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_FSYNC, err);
    }

    libsqlite3_sys::SQLITE_OK
}

/// Return the current file-size of a file.
#[tokio::main]
pub async unsafe fn file_size_inner<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    p_size: *mut libsqlite3_sys::sqlite3_int64,
) -> c_int {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_IOERR_FSTAT,
    };
    log::trace!("[{}] file_size ({})", state.id, state.db_name);

    if let Err(err) = state.file.size().await.and_then(|n| {
        let p_size: &mut libsqlite3_sys::sqlite3_int64 =
            p_size.as_mut().ok_or_else(null_ptr_error::<V::Error>)?;
        *p_size = n as libsqlite3_sys::sqlite3_int64;
        Ok(())
    }) {
        return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_FSTAT, err);
    }

    // #[cfg(feature = "sqlite_test")]
    // if simulate_io_error() {
    //     return libsqlite3_sys::SQLITE_ERROR;
    // }

    libsqlite3_sys::SQLITE_OK
}

/// Lock a file.
#[tokio::main]
pub async unsafe fn lock_inner<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    e_lock: c_int,
) -> c_int {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_IOERR_LOCK,
    };
    log::trace!("[{}] lock ({})", state.id, state.db_name);

    let lock = match LockKind::from_i32(e_lock) {
        Some(lock) => lock,
        None => return libsqlite3_sys::SQLITE_IOERR_LOCK,
    };
    match state.file.lock(lock).await {
        Ok(true) => {
            state.has_exclusive_lock = lock == LockKind::Exclusive;
            log::trace!("[{}] lock={:?} ({})", state.id, lock, state.db_name);

            // If just acquired a exclusive database lock while not having any exclusive lock
            // on the wal index, make sure the wal index is up to date.
            if state.has_exclusive_lock {
                let has_exclusive_wal_index = state
                    .wal_index_locks
                    .iter()
                    .any(|(_, lock)| *lock == wip::WalIndexLock::Exclusive);

                if !has_exclusive_wal_index {
                    log::trace!(
                        "[{}] acquired exclusive db lock, pulling wal index changes",
                        state.id,
                    );

                    if let Some((wal_index, _)) = state.wal_index.as_mut() {
                        for (region, data) in &mut state.wal_index_regions {
                            if let Err(err) = wal_index.pull::<F>(*region as u32, data) {
                                log::error!(
                                    "[{}] pulling wal index changes failed: {}",
                                    state.id,
                                    err
                                )
                            }
                        }
                    }
                }
            }

            libsqlite3_sys::SQLITE_OK
        }
        Ok(false) => {
            log::trace!(
                "[{}] busy (denied {:?}) ({})",
                state.id,
                lock,
                state.db_name
            );
            libsqlite3_sys::SQLITE_BUSY
        }
        Err(err) => state.set_last_error(libsqlite3_sys::SQLITE_IOERR_LOCK, err),
    }
}

/// Unlock a file.
#[tokio::main]
pub async unsafe fn unlock_inner<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    e_lock: c_int,
) -> c_int {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_IOERR_UNLOCK,
    };
    log::trace!("[{}] unlock ({})", state.id, state.db_name);

    let lock = match LockKind::from_i32(e_lock) {
        Some(lock) => lock,
        None => return libsqlite3_sys::SQLITE_IOERR_UNLOCK,
    };
    match state.file.unlock(lock).await {
        Ok(true) => {
            state.has_exclusive_lock = lock == LockKind::Exclusive;
            log::trace!("[{}] unlock={:?} ({})", state.id, lock, state.db_name);
            libsqlite3_sys::SQLITE_OK
        }
        Ok(false) => libsqlite3_sys::SQLITE_BUSY,
        Err(err) => state.set_last_error(libsqlite3_sys::SQLITE_IOERR_UNLOCK, err),
    }
}

/// Check if another file-handle holds a [LockKind::Reserved] lock on a file.
#[tokio::main]
pub async unsafe fn check_reserved_lock_inner<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    p_res_out: *mut c_int,
) -> c_int {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_IOERR_CHECKRESERVEDLOCK,
    };
    log::trace!("[{}] check_reserved_lock ({})", state.id, state.db_name);

    // #[cfg(feature = "sqlite_test")]
    // if simulate_io_error() {
    //     return libsqlite3_sys::SQLITE_IOERR_CHECKRESERVEDLOCK;
    // }

    if let Err(err) = state.file.reserved().await.and_then(|is_reserved| {
        let p_res_out: &mut c_int = p_res_out.as_mut().ok_or_else(null_ptr_error)?;
        *p_res_out = is_reserved as c_int;
        Ok(())
    }) {
        return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_UNLOCK, err);
    }

    libsqlite3_sys::SQLITE_OK
}

/// File control method. For custom operations on a mem-file.
#[tokio::main]
pub async unsafe fn file_control_inner<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    op: c_int,
    p_arg: *mut c_void,
) -> c_int {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_NOTFOUND,
    };
    log::trace!("[{}] file_control op={} ({})", state.id, op, state.db_name);

    // Docs: https://www.sqlite.org/c3ref/c_fcntl_begin_atomic_write.html
    match op {
        // The following op codes are alreay handled by sqlite before, so no need to handle them
        // in a custom VFS.
        libsqlite3_sys::SQLITE_FCNTL_FILE_POINTER
        | libsqlite3_sys::SQLITE_FCNTL_VFS_POINTER
        | libsqlite3_sys::SQLITE_FCNTL_JOURNAL_POINTER
        | libsqlite3_sys::SQLITE_FCNTL_DATA_VERSION
        | libsqlite3_sys::SQLITE_FCNTL_RESERVE_BYTES => libsqlite3_sys::SQLITE_NOTFOUND,

        // The following op codes are no longer used and thus ignored.
        libsqlite3_sys::SQLITE_FCNTL_SYNC_OMITTED => libsqlite3_sys::SQLITE_NOTFOUND,

        // Used for debugging. Write current state of the lock into (int)pArg.
        libsqlite3_sys::SQLITE_FCNTL_LOCKSTATE => match state.file.current_lock().await {
            Ok(lock) => {
                if let Some(p_arg) = (p_arg as *mut i32).as_mut() {
                    *p_arg = lock as i32;
                }
                libsqlite3_sys::SQLITE_OK
            }
            Err(err) => state.set_last_error(libsqlite3_sys::SQLITE_ERROR, err),
        },

        // Relevant for proxy-type locking. Not implemented.
        libsqlite3_sys::SQLITE_FCNTL_GET_LOCKPROXYFILE
        | libsqlite3_sys::SQLITE_FCNTL_SET_LOCKPROXYFILE => libsqlite3_sys::SQLITE_NOTFOUND,

        // Write last error number into (int)pArg.
        libsqlite3_sys::SQLITE_FCNTL_LAST_ERRNO => {
            if let Some(p_arg) = (p_arg as *mut i32).as_mut() {
                *p_arg = state.last_errno;
            }
            libsqlite3_sys::SQLITE_OK
        }

        // Give the VFS layer a hint of how large the database file will grow to be during the
        // current transaction.
        libsqlite3_sys::SQLITE_FCNTL_SIZE_HINT => {
            let size_hint = match (p_arg as *mut i64)
                .as_ref()
                .cloned()
                .and_then(|s| u64::try_from(s).ok())
            {
                Some(chunk_size) => chunk_size,
                None => {
                    return state.set_last_error(
                        libsqlite3_sys::SQLITE_NOTFOUND,
                        Error::ExpectedArg { name: "size_hint" },
                    );
                }
            };

            // #[cfg(feature = "sqlite_test")]
            // let _benign = simulate_io_error_benign();

            let current = match state.file.size().await {
                Ok(size) => size,
                Err(err) => return state.set_last_error(libsqlite3_sys::SQLITE_ERROR, err),
            };

            if current > size_hint {
                return libsqlite3_sys::SQLITE_OK;
            }

            if let Some(chunk_size) = state.chunk_size {
                let chunk_size = chunk_size as u64;
                let size = ((size_hint + chunk_size - 1) / chunk_size) * chunk_size;
                if let Err(err) = state.file.set_len(size).await {
                    return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_TRUNCATE, err);
                }
            } else if let Err(err) = state.file.set_len(size_hint).await {
                return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_TRUNCATE, err);
            }

            // #[cfg(feature = "sqlite_test")]
            // if simulate_io_error() {
            //     return libsqlite3_sys::SQLITE_IOERR_TRUNCATE;
            // }

            libsqlite3_sys::SQLITE_OK
        }

        // Request that the VFS extends and truncates the database file in chunks of a size
        // specified by the user. Return an error as this is not forwarded to the [Vfs] trait
        // right now.
        libsqlite3_sys::SQLITE_FCNTL_CHUNK_SIZE => {
            let chunk_size = match (p_arg as *mut i32)
                .as_ref()
                .cloned()
                .and_then(|s| usize::try_from(s).ok())
            {
                Some(chunk_size) => chunk_size,
                None => {
                    return state.set_last_error(
                        libsqlite3_sys::SQLITE_NOTFOUND,
                        Error::ExpectedArg { name: "chunk_size" },
                    );
                }
            };

            if let Err(err) = state.file.set_chunk_size(chunk_size).await {
                return state.set_last_error(libsqlite3_sys::SQLITE_ERROR, err);
            }

            state.chunk_size = Some(chunk_size);

            libsqlite3_sys::SQLITE_OK
        }

        // Configure automatic retry counts and intervals for certain disk I/O operations for
        // the windows VFS in order to provide robustness in the presence of anti-virus
        // programs. Not implemented.
        libsqlite3_sys::SQLITE_FCNTL_WIN32_AV_RETRY => libsqlite3_sys::SQLITE_NOTFOUND,

        // Enable or disable the persistent WAL setting.
        libsqlite3_sys::SQLITE_FCNTL_PERSIST_WAL => {
            if let Some(p_arg) = (p_arg as *mut i32).as_mut() {
                if *p_arg < 0 {
                    // query current setting
                    *p_arg = state.persist_wal as i32;
                } else {
                    state.persist_wal = *p_arg == 1;
                }
            };

            libsqlite3_sys::SQLITE_OK
        }

        // Indicate that, unless it is rolled back for some reason, the entire database file
        // will be overwritten by the current transaction. Not implemented.
        libsqlite3_sys::SQLITE_FCNTL_OVERWRITE => libsqlite3_sys::SQLITE_NOTFOUND,

        // Used to obtain the names of all VFSes in the VFS stack.
        libsqlite3_sys::SQLITE_FCNTL_VFSNAME => {
            if let Some(p_arg) = (p_arg as *mut *const c_char).as_mut() {
                let name = ManuallyDrop::new(state.vfs_name.clone());
                *p_arg = name.as_ptr();
            };

            libsqlite3_sys::SQLITE_OK
        }

        // Set or query the persistent "powersafe-overwrite" or "PSOW" setting.
        libsqlite3_sys::SQLITE_FCNTL_POWERSAFE_OVERWRITE => {
            if let Some(p_arg) = (p_arg as *mut i32).as_mut() {
                if *p_arg < 0 {
                    // query current setting
                    *p_arg = state.powersafe_overwrite as i32;
                } else {
                    state.powersafe_overwrite = *p_arg == 1;
                }
            };

            libsqlite3_sys::SQLITE_OK
        }

        // Optionally intercept PRAGMA statements. Always fall back to normal pragma processing.
        libsqlite3_sys::SQLITE_FCNTL_PRAGMA => libsqlite3_sys::SQLITE_NOTFOUND,

        // May be invoked by SQLite on the database file handle shortly after it is opened in
        // order to provide a custom VFS with access to the connection's busy-handler callback.
        // Not implemented.
        libsqlite3_sys::SQLITE_FCNTL_BUSYHANDLER => libsqlite3_sys::SQLITE_NOTFOUND,

        // Generate a temporary filename. Not implemented.
        libsqlite3_sys::SQLITE_FCNTL_TEMPFILENAME => {
            if let Some(p_arg) = (p_arg as *mut *const c_char).as_mut() {
                let name = state.vfs.temporary_name().await;
                // unwrap() is fine as os strings are an arbitrary sequences of non-zero bytes
                let name = CString::new(name.as_bytes()).unwrap();
                let name = ManuallyDrop::new(name);
                *p_arg = name.as_ptr();
            };

            libsqlite3_sys::SQLITE_OK
        }

        // Query or set the maximum number of bytes that will be used for memory-mapped I/O.
        // Not implemented.
        libsqlite3_sys::SQLITE_FCNTL_MMAP_SIZE => libsqlite3_sys::SQLITE_NOTFOUND,

        // Advisory information to the VFS about what the higher layers of the SQLite stack are
        // doing.
        libsqlite3_sys::SQLITE_FCNTL_TRACE => {
            let trace = CStr::from_ptr(p_arg as *const c_char);
            log::trace!("{}", trace.to_string_lossy());
            libsqlite3_sys::SQLITE_OK
        }

        // Check whether or not the file has been renamed, moved, or deleted since it was first
        // opened.
        libsqlite3_sys::SQLITE_FCNTL_HAS_MOVED => match state.file.moved().await {
            Ok(moved) => {
                if let Some(p_arg) = (p_arg as *mut i32).as_mut() {
                    *p_arg = moved as i32;
                }
                libsqlite3_sys::SQLITE_OK
            }
            Err(err) => state.set_last_error(libsqlite3_sys::SQLITE_ERROR, err),
        },

        // Sent to the VFS immediately before the xSync method is invoked on a database file
        // descriptor. Silently ignored.
        libsqlite3_sys::SQLITE_FCNTL_SYNC => libsqlite3_sys::SQLITE_OK,

        // Sent to the VFS after a transaction has been committed immediately but before the
        // database is unlocked. Silently ignored.
        libsqlite3_sys::SQLITE_FCNTL_COMMIT_PHASETWO => libsqlite3_sys::SQLITE_OK,

        // Used for debugging. Swap the file handle with the one pointed to by the pArg
        // argument. This capability is used during testing and only needs to be supported when
        // SQLITE_TEST is defined. Not implemented.
        libsqlite3_sys::SQLITE_FCNTL_WIN32_SET_HANDLE => libsqlite3_sys::SQLITE_NOTFOUND,

        // Signal to the VFS layer that it might be advantageous to block on the next WAL lock
        // if the lock is not immediately available. The WAL subsystem issues this signal during
        // rare circumstances in order to fix a problem with priority inversion.
        // Not implemented.
        libsqlite3_sys::SQLITE_FCNTL_WAL_BLOCK => libsqlite3_sys::SQLITE_NOTFOUND,

        // Implemented by zipvfs only.
        libsqlite3_sys::SQLITE_FCNTL_ZIPVFS => libsqlite3_sys::SQLITE_NOTFOUND,

        // Implemented by the special VFS used by the RBU extension only.
        libsqlite3_sys::SQLITE_FCNTL_RBU => libsqlite3_sys::SQLITE_NOTFOUND,

        // Obtain the underlying native file handle associated with a file handle.
        // Not implemented.
        libsqlite3_sys::SQLITE_FCNTL_WIN32_GET_HANDLE => libsqlite3_sys::SQLITE_NOTFOUND,

        // Usage is not documented. Not implemented.
        libsqlite3_sys::SQLITE_FCNTL_PDB => libsqlite3_sys::SQLITE_NOTFOUND,

        // Used for "batch write mode". Not supported.
        libsqlite3_sys::SQLITE_FCNTL_BEGIN_ATOMIC_WRITE
        | libsqlite3_sys::SQLITE_FCNTL_COMMIT_ATOMIC_WRITE
        | libsqlite3_sys::SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE => libsqlite3_sys::SQLITE_NOTFOUND,

        // Configure a VFS to block for up to M milliseconds before failing when attempting to
        // obtain a file lock using the xLock or xShmLock methods of the VFS. Not implemented.
        libsqlite3_sys::SQLITE_FCNTL_LOCK_TIMEOUT => libsqlite3_sys::SQLITE_NOTFOUND,

        // Used by in-memory VFS.
        libsqlite3_sys::SQLITE_FCNTL_SIZE_LIMIT => libsqlite3_sys::SQLITE_NOTFOUND,

        // Invoked from within a checkpoint in wal mode after the client has finished copying
        // pages from the wal file to the database file, but before the *-shm file is updated to
        // record the fact that the pages have been checkpointed. Silently ignored.
        libsqlite3_sys::SQLITE_FCNTL_CKPT_DONE => libsqlite3_sys::SQLITE_OK,

        // Invoked from within a checkpoint in wal mode before the client starts to copy pages
        // from the wal file to the database file. Silently ignored.
        libsqlite3_sys::SQLITE_FCNTL_CKPT_START => libsqlite3_sys::SQLITE_OK,

        // Detect whether or not there is a database client in another process with a wal-mode
        // transaction open on the database or not. Not implemented because it is a
        // unix-specific feature.
        libsqlite3_sys::SQLITE_FCNTL_EXTERNAL_READER => libsqlite3_sys::SQLITE_NOTFOUND,

        // Unknown use-case. Ignored.
        libsqlite3_sys::SQLITE_FCNTL_CKSM_FILE => libsqlite3_sys::SQLITE_NOTFOUND,

        _ => libsqlite3_sys::SQLITE_NOTFOUND,
    }
}

/// Close a file.
pub unsafe extern "C" fn close<V: Vfs, F: DatabaseHandle>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
) -> c_int {
    close_inner::<V, F>(p_file)
}

/// Read data from a file.
pub unsafe extern "C" fn read<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    z_buf: *mut c_void,
    i_amt: c_int,
    i_ofst: libsqlite3_sys::sqlite3_int64,
) -> c_int {
    read_inner::<V, F>(p_file, z_buf, i_amt, i_ofst)
}

/// Write data to a file.
pub unsafe extern "C" fn write<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    z: *const c_void,
    i_amt: c_int,
    i_ofst: libsqlite3_sys::sqlite3_int64,
) -> c_int {
    write_inner::<V, F>(p_file, z, i_amt, i_ofst)
}

/// Truncate a file.
pub unsafe extern "C" fn truncate<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    size: libsqlite3_sys::sqlite3_int64,
) -> c_int {
    truncate_inner::<V, F>(p_file, size)
}

/// Persist changes to a file.
pub unsafe extern "C" fn sync<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    flags: c_int,
) -> c_int {
    sync_inner::<V, F>(p_file, flags)
}

/// Return the current file-size of a file.
pub unsafe extern "C" fn file_size<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    p_size: *mut libsqlite3_sys::sqlite3_int64,
) -> c_int {
    file_size_inner::<V, F>(p_file, p_size)
}

/// Lock a file.
pub unsafe extern "C" fn lock<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    e_lock: c_int,
) -> c_int {
    lock_inner::<V, F>(p_file, e_lock)
}

/// Unlock a file.
pub unsafe extern "C" fn unlock<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    e_lock: c_int,
) -> c_int {
    unlock_inner::<V, F>(p_file, e_lock)
}

/// Check if another file-handle holds a [LockKind::Reserved] lock on a file.
pub unsafe extern "C" fn check_reserved_lock<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    p_res_out: *mut c_int,
) -> c_int {
    check_reserved_lock_inner::<V, F>(p_file, p_res_out)
}

/// File control method. For custom operations on a mem-file.
pub unsafe extern "C" fn file_control<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    op: c_int,
    p_arg: *mut c_void,
) -> c_int {
    file_control_inner::<V, F>(p_file, op, p_arg)
}

/// Return the sector-size in bytes for a file.
pub unsafe extern "C" fn sector_size<F>(_p_file: *mut libsqlite3_sys::sqlite3_file) -> c_int {
    log::trace!("sector_size");

    1024
}

/// Return the device characteristic flags supported by a file.
pub unsafe extern "C" fn device_characteristics<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
) -> c_int {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_IOERR_SHMMAP,
    };

    log::trace!("[{}] device_characteristics", state.id,);

    // The following characteristics are needed to match the expected behavior of the tests.

    // after reboot following a crash or power loss, the only bytes in a file that were written
    // at the application level might have changed and that adjacent bytes, even bytes within
    // the same sector are guaranteed to be unchanged
    if state.powersafe_overwrite {
        libsqlite3_sys::SQLITE_IOCAP_POWERSAFE_OVERWRITE
    } else {
        0
    }
}

/// Create a shared memory file mapping.
#[tokio::main]
pub async unsafe fn shm_map_inner<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    region_ix: i32,
    region_size: i32,
    b_extend: i32,
    pp: *mut *mut c_void,
) -> i32 {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_IOERR_SHMMAP,
    };
    log::trace!(
        "[{}] shm_map pg={} sz={} extend={} ({})",
        state.id,
        region_ix,
        region_size,
        b_extend,
        state.db_name
    );

    if !F::WalIndex::enabled() {
        return libsqlite3_sys::SQLITE_IOERR_SHMLOCK;
    }

    if region_size != 32768 {
        return state.set_last_error(
            libsqlite3_sys::SQLITE_IOERR_SHMMAP,
            Error::InvalidRegionSize {
                size: region_size as isize,
            },
        );
    }

    let (wal_index, readonly) = match state.wal_index.as_mut() {
        Some((wal_index, readonly)) => (wal_index, *readonly),
        None => {
            let (wal_index, readonly) = state.wal_index.get_or_insert(
                match state
                    .file
                    .wal_index(false)
                    .await
                    .map(|wal_index| (wal_index, false))
                {
                    Ok((wal_index, readonly)) => (wal_index, readonly),
                    Err(Error::PermissionDenied) => {
                        // Try again as readonly
                        match state
                            .file
                            .wal_index(true)
                            .await
                            .map(|wal_index| (wal_index, true))
                        {
                            Ok(res) => res,
                            Err(err) => {
                                return state
                                    .set_last_error(libsqlite3_sys::SQLITE_IOERR_SHMMAP, err)
                            }
                        }
                    }
                    Err(err) => {
                        return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_SHMMAP, err);
                    }
                },
            );
            (wal_index, *readonly)
        }
    };

    let entry = state.wal_index_regions.entry(region_ix as u32);
    match entry {
        Entry::Occupied(mut entry) => {
            *pp = entry.get_mut().as_mut_ptr() as *mut c_void;
        }
        Entry::Vacant(entry) => {
            let mut m = match wal_index.map::<F>(region_ix as u32) {
                Ok(m) => Box::pin(m),
                Err(err) => {
                    return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_SHMMAP, err);
                }
            };
            *pp = m.as_mut_ptr() as *mut c_void;
            entry.insert(m);
        }
    }

    if readonly {
        libsqlite3_sys::SQLITE_READONLY
    } else {
        libsqlite3_sys::SQLITE_OK
    }
}

/// Create a shared memory file mapping.
pub unsafe extern "C" fn shm_map<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    region_ix: i32,
    region_size: i32,
    b_extend: i32,
    pp: *mut *mut c_void,
) -> i32 {
    shm_map_inner::<V, F>(p_file, region_ix, region_size, b_extend, pp)
}

/// Perform locking on a shared-memory segment.
pub unsafe extern "C" fn shm_lock<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    offset: i32,
    n: i32,
    flags: i32,
) -> i32 {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_IOERR_SHMMAP,
    };
    let locking = flags & libsqlite3_sys::SQLITE_SHM_LOCK > 0;
    let exclusive = flags & libsqlite3_sys::SQLITE_SHM_EXCLUSIVE > 0;
    log::trace!(
        "[{}] shm_lock offset={} n={} lock={} exclusive={} (flags={}) ({})",
        state.id,
        offset,
        n,
        locking,
        exclusive,
        flags,
        state.db_name
    );

    let range = offset as u8..(offset + n) as u8;
    let lock = match (locking, exclusive) {
        (true, true) => wip::WalIndexLock::Exclusive,
        (true, false) => wip::WalIndexLock::Shared,
        (false, _) => wip::WalIndexLock::None,
    };

    let (wal_index, readonly) = match state.wal_index.as_mut() {
        Some((wal_index, readonly)) => (wal_index, *readonly),
        None => {
            return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_SHMLOCK, Error::WalIndexLock)
        }
    };

    if locking {
        let has_exclusive = state
            .wal_index_locks
            .iter()
            .any(|(_, lock)| *lock == wip::WalIndexLock::Exclusive);

        if !has_exclusive {
            log::trace!(
                "[{}] does not have wal index write lock, pulling changes",
                state.id
            );
            for (region, data) in &mut state.wal_index_regions {
                if let Err(err) = wal_index.pull::<F>(*region as u32, data) {
                    return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_SHMLOCK, err);
                }
            }
        }
    } else {
        let releases_any_exclusive = state
            .wal_index_locks
            .iter()
            .any(|(region, lock)| *lock == wip::WalIndexLock::Exclusive && range.contains(region));

        // push index changes when moving from any exclusive lock to no exclusive locks
        if releases_any_exclusive && !readonly {
            log::trace!(
                "[{}] releasing an exclusive lock, pushing wal index changes",
                state.id,
            );
            for (region, data) in &mut state.wal_index_regions {
                if let Err(err) = wal_index.push::<F>(*region as u32, data) {
                    return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_SHMLOCK, err);
                }
            }
        }
    }

    match wal_index.lock::<F>(range.clone(), lock) {
        Ok(true) => {
            for region in range {
                state.wal_index_locks.insert(region, lock);
            }
            libsqlite3_sys::SQLITE_OK
        }
        Ok(false) => libsqlite3_sys::SQLITE_BUSY,
        Err(err) => state.set_last_error(libsqlite3_sys::SQLITE_IOERR_SHMLOCK, err),
    }
}

/// Memory barrier operation on shared memory.
pub unsafe extern "C" fn shm_barrier<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
) {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return,
    };
    log::trace!("[{}] shm_barrier ({})", state.id, state.db_name);

    let (wal_index, readonly) = if let Some((wal_index, readonly)) = state.wal_index.as_mut() {
        (wal_index, *readonly)
    } else {
        return;
    };

    if state.has_exclusive_lock && !readonly {
        log::trace!(
            "[{}] has exclusive db lock, pushing wal index changes",
            state.id,
        );
        for (region, data) in &mut state.wal_index_regions {
            if let Err(err) = wal_index.push::<F>(*region as u32, data) {
                log::error!("[{}] pushing wal index changes failed: {}", state.id, err)
            }
        }

        return;
    }

    let has_exclusive = state
        .wal_index_locks
        .iter()
        .any(|(_, lock)| *lock == wip::WalIndexLock::Exclusive);

    if !has_exclusive {
        log::trace!(
            "[{}] does not have wal index write lock, pulling changes",
            state.id
        );
        for (region, data) in &mut state.wal_index_regions {
            if let Err(err) = wal_index.pull::<F>(*region as u32, data) {
                log::error!("[{}] pulling wal index changes failed: {}", state.id, err)
            }
        }
    }
}

/// Unmap a shared memory segment.
pub unsafe extern "C" fn shm_unmap<V: Vfs, F: DatabaseHandle<Error = V::Error>>(
    p_file: *mut libsqlite3_sys::sqlite3_file,
    delete_flags: i32,
) -> i32 {
    let state = match file_state::<V, F>(p_file) {
        Ok(f) => f,
        Err(_) => return libsqlite3_sys::SQLITE_IOERR_SHMMAP,
    };
    log::trace!(
        "[{}] shm_unmap delete={} ({})",
        state.id,
        delete_flags == 1,
        state.db_name
    );

    state.wal_index_regions.clear();
    state.wal_index_locks.clear();

    if delete_flags == 1 {
        if let Some((wal_index, readonly)) = state.wal_index.take() {
            if !readonly {
                if let Err(err) = wal_index.delete::<F>() {
                    return state.set_last_error(libsqlite3_sys::SQLITE_ERROR, err);
                }
            }
        }
    }

    libsqlite3_sys::SQLITE_OK
}
