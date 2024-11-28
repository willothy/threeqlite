#![allow(async_fn_in_trait)]

use rusqlite::{Connection, OpenFlags};
use vfs::ThreeQLite;

pub mod error;
pub mod handle;
pub mod vfs;
pub mod wal;

fn main() -> Result<(), crate::error::Error> {
    dotenvy::dotenv().unwrap();

    tracing_subscriber::fmt::init();

    let rt = tokio::runtime::Runtime::new().unwrap();

    let tq = rt.block_on(ThreeQLite::new());
    sqlite_vfs::register("bruhfs", tq, true).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        "main.db3",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        "bruhfs",
    )
    .unwrap();

    conn.execute("PRAGMA page_size = 4096;", []).unwrap();
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode=MEMORY", [], |row| row.get(0))
        .unwrap();
    assert_eq!(journal_mode, "memory");

    let n: i64 = conn.query_row("SELECT 42", [], |row| row.get(0)).unwrap();
    assert_eq!(n, 42);

    Ok(())
}
