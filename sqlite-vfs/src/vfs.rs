// Example mem-fs implementation:
// https://github.com/sqlite/sqlite/blob/a959bf53110bfada67a3a52187acd57aa2f34e19/ext/misc/memvfs.c

use std::{
    ffi::{c_char, c_int, c_void, CStr, CString},
    io::ErrorKind,
    sync::Arc,
    time::Duration,
};

use tokio::runtime;

use crate::{
    error::Error,
    state::{null_ptr_error, vfs_state, FileExt, FileState},
    DatabaseHandle, OpenAccess, OpenKind, OpenOptions, Vfs, MAX_PATH_LENGTH,
};

/// Open a new file handler.
async unsafe fn open_inner<F: DatabaseHandle, V: Vfs<Handle = F>>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_name: *const c_char,
    p_file: *mut libsqlite3_sys::sqlite3_file,
    flags: c_int,
    p_out_flags: *mut c_int,
) -> c_int {
    let state = match vfs_state::<V>(p_vfs) {
        Ok(state) => state,
        Err(_) => return libsqlite3_sys::SQLITE_ERROR,
    };

    let name = if z_name.is_null() {
        None
    } else {
        let raw = CStr::from_ptr(z_name);
        match raw.to_str() {
            Ok(name) => Some(name),
            Err(_) => {
                return state.set_last_error(
                    libsqlite3_sys::SQLITE_CANTOPEN,
                    Error::InvalidDbName {
                        name: raw.to_owned(),
                    },
                )
            }
        }
    };
    log::trace!("open z_name={:?} flags={}", name, flags);

    let mut opts = match OpenOptions::from_flags(flags) {
        Some(opts) => opts,
        None => {
            return state.set_last_error(libsqlite3_sys::SQLITE_CANTOPEN, Error::InvalidOpenFlags);
        }
    };

    if z_name.is_null() && !opts.delete_on_close {
        return state.set_last_error(libsqlite3_sys::SQLITE_CANTOPEN, Error::InvalidOpenFlags);
    }

    let out_file = match (p_file as *mut FileState<V, F>).as_mut() {
        Some(f) => f,
        None => {
            return state.set_last_error(libsqlite3_sys::SQLITE_CANTOPEN, Error::InvalidFilePtr);
        }
    };

    let mut powersafe_overwrite = true;
    if flags & libsqlite3_sys::SQLITE_OPEN_URI > 0 && name.is_some() {
        let param = b"psow\0";
        if libsqlite3_sys::sqlite3_uri_boolean(z_name, param.as_ptr() as *const c_char, 1) == 0 {
            powersafe_overwrite = false;
        }
    }

    let name = match name {
        Some(s) => s.to_string(),
        None => state.vfs.temporary_name().await,
    };
    let result = state.vfs.open(&name, opts.clone()).await;
    let result = match result {
        Ok(f) => Ok(f),
        // handle creation failure due to readonly directory
        Err(Error::PermissionDenied)
            if matches!(
                opts.kind,
                OpenKind::SuperJournal | OpenKind::MainJournal | OpenKind::Wal
            ) && matches!(opts.access, OpenAccess::Create | OpenAccess::CreateNew)
                && !state.vfs.exists(&name).await.unwrap_or(false) =>
        {
            return state.set_last_error(
                libsqlite3_sys::SQLITE_READONLY_DIRECTORY,
                Error::PermissionDenied,
            );
        }

        // Try again as readonly
        Err(Error::PermissionDenied) if opts.access != OpenAccess::Read => {
            opts.access = OpenAccess::Read;
            state
                .vfs
                .open(&name, opts.clone())
                .await
                .map_err(|_| Error::PermissionDenied)
        }

        // // e.g. tried to open a directory
        // Err(err) if err.kind() == ErrorKind::Other && opts.access == OpenAccess::Read => {
        //     return state.set_last_error(libsqlite3_sys::SQLITE_IOERR, err);
        // }
        Err(err) => Err(err),
    };
    let file = match result {
        Ok(f) => f,
        Err(err) => {
            return state.set_last_error(libsqlite3_sys::SQLITE_CANTOPEN, err);
        }
    };

    if let Some(p_out_flags) = p_out_flags.as_mut() {
        *p_out_flags = opts.to_flags();
    }

    out_file.base.pMethods = &state.io_methods;
    out_file.ext.write(FileExt {
        vfs: state.vfs.clone(),
        vfs_name: state.name.clone(),
        db_name: name,
        file,
        delete_on_close: opts.delete_on_close,
        last_error: Arc::clone(&state.last_error),
        last_errno: 0,
        wal_index: None,
        wal_index_regions: Default::default(),
        wal_index_locks: Default::default(),
        has_exclusive_lock: false,
        id: state.next_id,
        chunk_size: None,
        persist_wal: false,
        powersafe_overwrite,
    });
    state.next_id = state.next_id.overflowing_add(1).0;

    // #[cfg(feature = "sqlite_test")]
    // libsqlite3_sys::sqlite3_inc_open_file_count();

    libsqlite3_sys::SQLITE_OK
}

/// Delete the file located at `z_path`. If the `sync_dir` argument is true, ensure the
/// file-system modifications are synced to disk before returning.
async unsafe fn delete_inner<V: Vfs>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_path: *const c_char,
    _sync_dir: c_int,
) -> c_int {
    let state = match vfs_state::<V>(p_vfs) {
        Ok(state) => state,
        Err(_) => return libsqlite3_sys::SQLITE_DELETE,
    };

    let raw = CStr::from_ptr(z_path);
    let path = match raw.to_str() {
        Ok(name) => name,
        Err(_) => {
            return state.set_last_error(
                libsqlite3_sys::SQLITE_ERROR,
                crate::error::Error::InvalidDbName {
                    name: raw.to_owned(),
                },
            )
        }
    };
    log::trace!("delete name={}", path);

    match state.vfs.delete(path).await {
        Ok(_) => libsqlite3_sys::SQLITE_OK,
        Err(err) => {
            if let Error::DbNotFound { name } = &err {
                return libsqlite3_sys::SQLITE_IOERR_DELETE_NOENT;
            }
            state.set_last_error(libsqlite3_sys::SQLITE_DELETE, err)
        }
    }
}

/// Test for access permissions. Return true if the requested permission is available, or false
/// otherwise.
async unsafe fn access_inner<V: Vfs>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_path: *const c_char,
    flags: c_int,
    p_res_out: *mut c_int,
) -> c_int {
    // #[cfg(feature = "sqlite_test")]
    // if simulate_io_error() {
    //     return libsqlite3_sys::SQLITE_IOERR_ACCESS;
    // }

    let state = match vfs_state::<V>(p_vfs) {
        Ok(state) => state,
        Err(_) => return libsqlite3_sys::SQLITE_ERROR,
    };

    let path = match CStr::from_ptr(z_path).to_str() {
        Ok(name) => name,
        Err(_) => {
            log::warn!(
                "access failed: database must be valid utf8 (received: {:?})",
                CStr::from_ptr(z_path)
            );

            if let Ok(p_res_out) = p_res_out.as_mut().ok_or_else(null_ptr_error::<V::Error>) {
                *p_res_out = false as i32;
            }

            return libsqlite3_sys::SQLITE_OK;
        }
    };
    log::trace!("access z_name={} flags={}", path, flags);

    let result = match flags {
        libsqlite3_sys::SQLITE_ACCESS_EXISTS => state.vfs.exists(path).await,
        libsqlite3_sys::SQLITE_ACCESS_READ => state.vfs.access(path, false).await,
        libsqlite3_sys::SQLITE_ACCESS_READWRITE => state.vfs.access(path, true).await,
        _ => return libsqlite3_sys::SQLITE_IOERR_ACCESS,
    };

    if let Err(err) = result.and_then(|ok| {
        let p_res_out: &mut c_int = p_res_out.as_mut().ok_or_else(null_ptr_error)?;
        *p_res_out = ok as i32;
        Ok(())
    }) {
        return state.set_last_error(libsqlite3_sys::SQLITE_IOERR_ACCESS, err);
    }

    libsqlite3_sys::SQLITE_OK
}

/// Populate buffer `z_out` with the full canonical pathname corresponding to the pathname in
/// `z_path`. `z_out` is guaranteed to point to a buffer of at least (INST_MAX_PATHNAME+1)
/// bytes.
async unsafe fn full_pathname_inner<V: Vfs>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_path: *const c_char,
    n_out: c_int,
    z_out: *mut c_char,
) -> c_int {
    // #[cfg(feature = "sqlite_test")]
    // if simulate_io_error() {
    //     return libsqlite3_sys::SQLITE_ERROR;
    // }

    let state = match vfs_state::<V>(p_vfs) {
        Ok(state) => state,
        Err(_) => return libsqlite3_sys::SQLITE_ERROR,
    };

    let raw = CStr::from_ptr(z_path);
    let path = match raw.to_str() {
        Ok(name) => name,
        Err(_) => {
            return state.set_last_error(
                libsqlite3_sys::SQLITE_ERROR,
                Error::InvalidDbName {
                    name: raw.to_owned(),
                },
            )
        }
    };
    log::trace!("full_pathname name={}", path);

    let name = match state
        .vfs
        .full_pathname(path)
        .await
        .map(|name| CString::new(name.to_string()).expect("str should never contain null byte"))
    {
        Ok(name) => name,
        Err(err) => return state.set_last_error(libsqlite3_sys::SQLITE_ERROR, err),
    };

    let name = name.to_bytes_with_nul();
    if name.len() > n_out as usize || name.len() > MAX_PATH_LENGTH {
        return state.set_last_error(libsqlite3_sys::SQLITE_CANTOPEN, Error::PathTooLong);
    }
    let out = std::slice::from_raw_parts_mut(z_out as *mut u8, name.len());
    out.copy_from_slice(name);

    libsqlite3_sys::SQLITE_OK
}

/// Open a new file handler.
pub unsafe extern "C" fn open<F: DatabaseHandle, V: Vfs<Handle = F>>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_name: *const c_char,
    p_file: *mut libsqlite3_sys::sqlite3_file,
    flags: c_int,
    p_out_flags: *mut c_int,
) -> c_int {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(open_inner::<F, V>(
            p_vfs,
            z_name,
            p_file,
            flags,
            p_out_flags,
        ))
}

/// Delete the file located at `z_path`. If the `sync_dir` argument is true, ensure the
/// file-system modifications are synced to disk before returning.
pub unsafe extern "C" fn delete<V: Vfs>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_path: *const c_char,
    sync_dir: c_int,
) -> c_int {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(delete_inner::<V>(p_vfs, z_path, sync_dir))
}

/// Test for access permissions. Return true if the requested permission is available, or false
/// otherwise.
pub unsafe extern "C" fn access<V: Vfs>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_path: *const c_char,
    flags: c_int,
    p_res_out: *mut c_int,
) -> c_int {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(access_inner::<V>(p_vfs, z_path, flags, p_res_out))
}

/// Populate buffer `z_out` with the full canonical pathname corresponding to the pathname in
/// `z_path`. `z_out` is guaranteed to point to a buffer of at least (INST_MAX_PATHNAME+1)
/// bytes.
pub unsafe extern "C" fn full_pathname<V: Vfs>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_path: *const c_char,
    n_out: c_int,
    z_out: *mut c_char,
) -> c_int {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(full_pathname_inner::<V>(p_vfs, z_path, n_out, z_out))
}

/// Open the dynamic library located at `z_path` and return a handle.
#[allow(unused_variables)]
pub unsafe extern "C" fn dlopen<V>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_path: *const c_char,
) -> *mut c_void {
    log::trace!("dlopen");

    #[cfg(feature = "loadext")]
    {
        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return null_mut(),
        };

        if let Some(dlopen) = state.parent_vfs.as_ref().and_then(|v| v.xDlOpen) {
            return dlopen(state.parent_vfs, z_path);
        }
    }

    std::ptr::null_mut()
}

/// Populate the buffer `z_err_msg` (size `n_byte` bytes) with a human readable utf-8 string
/// describing the most recent error encountered associated with dynamic libraries.
#[allow(unused_variables)]
pub unsafe extern "C" fn dlerror<V>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    n_byte: c_int,
    z_err_msg: *mut c_char,
) {
    log::trace!("dlerror");

    #[cfg(feature = "loadext")]
    {
        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return,
        };

        if let Some(dlerror) = state.parent_vfs.as_ref().and_then(|v| v.xDlError) {
            return dlerror(state.parent_vfs, n_byte, z_err_msg);
        }

        return;
    }

    #[cfg(not(feature = "loadext"))]
    {
        let msg = concat!("Loadable extensions are not supported", "\0");
        libsqlite3_sys::sqlite3_snprintf(n_byte, z_err_msg, msg.as_ptr() as _);
    }
}

/// Return a pointer to the symbol `z_sym` in the dynamic library pHandle.
#[allow(unused_variables)]
pub unsafe extern "C" fn dlsym<V>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    p: *mut c_void,
    z_sym: *const c_char,
) -> Option<unsafe extern "C" fn(*mut libsqlite3_sys::sqlite3_vfs, *mut c_void, *const c_char)> {
    log::trace!("dlsym");

    #[cfg(feature = "loadext")]
    {
        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return None,
        };

        if let Some(dlsym) = state.parent_vfs.as_ref().and_then(|v| v.xDlSym) {
            return dlsym(state.parent_vfs, p, z_sym);
        }
    }

    None
}

/// Close the dynamic library handle `p_handle`.
#[allow(unused_variables)]
pub unsafe extern "C" fn dlclose<V>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    p_handle: *mut c_void,
) {
    log::trace!("dlclose");

    #[cfg(feature = "loadext")]
    {
        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return,
        };

        if let Some(dlclose) = state.parent_vfs.as_ref().and_then(|v| v.xDlClose) {
            return dlclose(state.parent_vfs, p_handle);
        }
    }
}

async unsafe fn randomness_inner<V: Vfs>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    n_byte: c_int,
    z_buf_out: *mut c_char,
) -> c_int {
    log::trace!("randomness");

    let bytes = std::slice::from_raw_parts_mut(z_buf_out as *mut i8, n_byte as usize);
    if cfg!(feature = "sqlite_test") {
        // During testing, the buffer is simply initialized to all zeroes for repeatability
        bytes.fill(0);
    } else {
        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return 0,
        };

        state.vfs.random(bytes).await;
    }
    bytes.len() as c_int
}

/// Populate the buffer pointed to by `z_buf_out` with `n_byte` bytes of random data.
pub unsafe extern "C" fn randomness<V: Vfs>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    n_byte: c_int,
    z_buf_out: *mut c_char,
) -> c_int {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(randomness_inner::<V>(p_vfs, n_byte, z_buf_out))
}

/// Sleep for `n_micro` microseconds. Return the number of microseconds actually slept.
pub unsafe extern "C" fn sleep<V: Vfs>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    n_micro: c_int,
) -> c_int {
    log::trace!("sleep");

    let state = match vfs_state::<V>(p_vfs) {
        Ok(state) => state,
        Err(_) => return libsqlite3_sys::SQLITE_ERROR,
    };
    state
        .vfs
        .sleep(Duration::from_micros(n_micro as u64))
        .as_micros() as c_int
}

/// Return the current time as a Julian Day number in `p_time_out`.
pub unsafe extern "C" fn current_time<V>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    p_time_out: *mut f64,
) -> c_int {
    log::trace!("current_time");

    let mut i = 0i64;
    current_time_int64::<V>(p_vfs, &mut i);

    *p_time_out = i as f64 / 86400000.0;
    libsqlite3_sys::SQLITE_OK
}

pub unsafe extern "C" fn current_time_int64<V>(
    _p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    p: *mut i64,
) -> i32 {
    log::trace!("current_time_int64");

    const UNIX_EPOCH: i64 = 24405875 * 8640000;
    let now = time::OffsetDateTime::now_utc().unix_timestamp() + UNIX_EPOCH;
    // #[cfg(feature = "sqlite_test")]
    // let now = if libsqlite3_sys::sqlite3_get_current_time() > 0 {
    //     libsqlite3_sys::sqlite3_get_current_time() as i64 * 1000 + UNIX_EPOCH
    // } else {
    //     now
    // };

    *p = now;
    libsqlite3_sys::SQLITE_OK
}

#[cfg(feature = "syscall")]
pub unsafe extern "C" fn set_system_call<V>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_name: *const ::std::os::raw::c_char,
    p_new_func: libsqlite3_sys::libsqlite3_syscall_ptr,
) -> ::std::os::raw::c_int {
    let state = match vfs_state::<V>(p_vfs) {
        Ok(state) => state,
        Err(_) => return libsqlite3_sys::SQLITE_ERROR,
    };

    if let Some(set_system_call) = state.parent_vfs.as_ref().and_then(|v| v.xSetSystemCall) {
        return set_system_call(state.parent_vfs, z_name, p_new_func);
    }

    libsqlite3_sys::SQLITE_ERROR
}

#[cfg(feature = "syscall")]
pub unsafe extern "C" fn get_system_call<V>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_name: *const ::std::os::raw::c_char,
) -> libsqlite3_sys::libsqlite3_syscall_ptr {
    let state = match vfs_state::<V>(p_vfs) {
        Ok(state) => state,
        Err(_) => return None,
    };

    if let Some(get_system_call) = state.parent_vfs.as_ref().and_then(|v| v.xGetSystemCall) {
        return get_system_call(state.parent_vfs, z_name);
    }

    None
}

#[cfg(feature = "syscall")]
pub unsafe extern "C" fn next_system_call<V>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    z_name: *const ::std::os::raw::c_char,
) -> *const ::std::os::raw::c_char {
    let state = match vfs_state::<V>(p_vfs) {
        Ok(state) => state,
        Err(_) => return std::ptr::null(),
    };

    if let Some(next_system_call) = state.parent_vfs.as_ref().and_then(|v| v.xNextSystemCall) {
        return next_system_call(state.parent_vfs, z_name);
    }

    std::ptr::null()
}

pub unsafe extern "C" fn get_last_error<V: Vfs>(
    p_vfs: *mut libsqlite3_sys::sqlite3_vfs,
    n_byte: c_int,
    z_err_msg: *mut c_char,
) -> c_int {
    let state = match vfs_state::<V>(p_vfs) {
        Ok(state) => state,
        Err(_) => return libsqlite3_sys::SQLITE_ERROR,
    };
    if let Some((eno, err)) = state.last_error.lock().unwrap().as_ref() {
        let msg = match CString::new(err.to_string()) {
            Ok(msg) => msg,
            Err(_) => return libsqlite3_sys::SQLITE_ERROR,
        };

        let msg = msg.to_bytes_with_nul();
        if msg.len() > n_byte as usize {
            return libsqlite3_sys::SQLITE_ERROR;
        }
        let out = std::slice::from_raw_parts_mut(z_err_msg as *mut u8, msg.len());
        out.copy_from_slice(msg);

        return *eno;
    }
    libsqlite3_sys::SQLITE_OK
}
