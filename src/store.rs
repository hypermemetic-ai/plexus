//! One SQLite database per activation, through rusqlite (feature `store`).
//!
//! An activation keeps a [`Store`] as a field, made in its `Default`, and
//! uses it as a [`rusqlite::Connection`]. The database is opened at its
//! first use, not when it is made: the activation is made before
//! [`main`](crate::main) has read where its bytes go.
//!
//! Natively the bytes go to `<DIR>/<name>.db`, DIR being `--data DIR` or
//! the environment's `PLEXUS_DATA`; with neither, to memory, said once on
//! stderr. In a component they go to the host's files when it keeps them
//! (`store/wasm.rs`), else to memory.

use std::cell::OnceCell;
use std::ops::Deref;

use rusqlite::Connection;

#[cfg(target_arch = "wasm32")]
mod wasm;

/// An activation's database: a [`Connection`], opened at first use, with
/// its schema run on it then.
pub struct Store {
    name: &'static str,
    schema: &'static str,
    db: OnceCell<Connection>,
}

impl Store {
    /// The database named `name`. `schema` is run whenever it is opened, so
    /// it says `create table if not exists`.
    pub fn open(name: &'static str, schema: &'static str) -> Store {
        Store { name, schema, db: OnceCell::new() }
    }
}

impl Deref for Store {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.db.get_or_init(|| {
            let db = connect(self.name).unwrap_or_else(|e| panic!("the store {:?} does not open: {e}", self.name));
            db.execute_batch(self.schema).unwrap_or_else(|e| panic!("the schema of the store {:?}: {e}", self.name));
            db
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
static DATA: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

/// Where every store of this process keeps its file: `--data DIR`, read by
/// [`main`](crate::main) before the first store opens.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn set_data(dir: std::path::PathBuf) {
    let _ = DATA.set(dir);
}

#[cfg(not(target_arch = "wasm32"))]
fn connect(name: &str) -> rusqlite::Result<Connection> {
    let dir = DATA.get().cloned().or_else(|| std::env::var_os("PLEXUS_DATA").filter(|d| !d.is_empty()).map(Into::into));
    match dir {
        Some(dir) => {
            std::fs::create_dir_all(&dir).map_err(|e| rusqlite::Error::InvalidPath(format!("{}: {e}", dir.display()).into()))?;
            Connection::open(dir.join(format!("{name}.db")))
        }
        None => {
            eprintln!("plexus: the store {name:?} is in memory and goes when the process ends (no --data DIR, no PLEXUS_DATA)");
            Connection::open_in_memory()
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn connect(name: &str) -> rusqlite::Result<Connection> {
    wasm::install();
    Connection::open(format!("{name}.db"))
}
