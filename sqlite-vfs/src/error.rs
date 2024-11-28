use std::ffi::CString;

use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error<External = Box<dyn std::error::Error>> {
    UnexpectedEof,

    #[snafu(display("database must be valid utf8 (received {name:?})"))]
    InvalidDbName {
        name: CString,
    },

    #[snafu(display("database {name} not found"))]
    DbNotFound {
        name: String,
    },

    #[snafu(display("path too long"))]
    PathTooLong,

    #[snafu(display("invalid open flags"))]
    InvalidOpenFlags,

    #[snafu(display("invalid file pointer"))]
    InvalidFilePtr,

    #[snafu(display("permission denied"))]
    PermissionDenied,

    #[snafu(display("received null pointer"))]
    NullPtr,

    #[snafu(display("write zero (???)"))]
    WriteZero,

    #[snafu(display("expected {name} arg"))]
    ExpectedArg {
        name: &'static str,
    },

    #[snafu(display("encountered region size other than 32kB (got {size})"))]
    InvalidRegionSize {
        size: isize,
    },

    #[snafu(display("wal is disabled"))]
    WalDisabled,

    #[snafu(display("trying to lock wal index, which isn't created yet"))]
    WalIndexLock,

    External {
        cause: External,
    },
}

impl<T> From<T> for Error<T> {
    fn from(value: T) -> Self {
        Self::External { cause: value }
    }
}
