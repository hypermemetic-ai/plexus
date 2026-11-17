//! The one SQLite of a page: the export `plexus:store/sql`, rusqlite over
//! the VFS of `vfs.rs`. Every activation's `plexus::Store` in a component
//! reaches it through the import of that name, so SQLite is fetched once,
//! not compiled into each activation.

use std::cell::RefCell;

use rusqlite::types::{self, ToSqlOutput};
use rusqlite::{Connection, ToSql, params_from_iter};

mod vfs;
#[path = "../../src/store/packed.rs"]
mod packed;

use packed::ValueRef;

wit_bindgen::generate!({ path: "../wit", world: "engine" });

use exports::plexus::store::sql::Guest;

thread_local!(static DBS: RefCell<Vec<(String, Connection)>> = const { RefCell::new(Vec::new()) });

fn with<T>(db: u32, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, String> {
    DBS.with_borrow(|dbs| match dbs.get(db as usize) {
        Some((_, c)) => f(c).map_err(|e| e.to_string()),
        None => Err(format!("no database {db}")),
    })
}

impl ToSql for ValueRef<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match *self {
            ValueRef::Null => types::ValueRef::Null,
            ValueRef::Integer(i) => types::ValueRef::Integer(i),
            ValueRef::Real(r) => types::ValueRef::Real(r),
            ValueRef::Text(t) => types::ValueRef::Text(t),
            ValueRef::Blob(b) => types::ValueRef::Blob(b),
        }))
    }
}

fn unpack(mut block: &[u8]) -> Result<Vec<ValueRef<'_>>, String> {
    let mut params = Vec::new();
    while !block.is_empty() {
        params.push(ValueRef::take(&mut block).map_err(|e| format!("the parameters: {e}"))?);
    }
    Ok(params)
}

struct Engine;

impl Guest for Engine {
    fn open(name: String, schema: String) -> Result<u32, String> {
        vfs::install();
        DBS.with_borrow_mut(|dbs| {
            if let Some(at) = dbs.iter().position(|(n, _)| *n == name) {
                return Ok(at as u32);
            }
            let db = Connection::open(format!("{name}.db")).map_err(|e| e.to_string())?;
            db.execute_batch(&schema).map_err(|e| format!("the schema of {name:?}: {e}"))?;
            dbs.push((name, db));
            Ok(dbs.len() as u32 - 1)
        })
    }

    // Statements are kept prepared by their text, so an author who runs the
    // same SQL again does not prepare it again.
    fn execute(db: u32, sql: String, params: Vec<u8>) -> Result<u64, String> {
        let params = unpack(&params)?;
        with(db, |c| c.prepare_cached(&sql)?.execute(params_from_iter(&params)).map(|n| n as u64))
    }

    fn query(db: u32, sql: String, params: Vec<u8>) -> Result<Vec<u8>, String> {
        let params = unpack(&params)?;
        with(db, |c| {
            let mut st = c.prepare_cached(&sql)?;
            let columns = st.column_count();
            let mut rows = st.query(params_from_iter(&params))?;
            let mut block = (columns as u32).to_le_bytes().to_vec();
            while let Some(row) = rows.next()? {
                for i in 0..columns {
                    match row.get_ref(i)? {
                        types::ValueRef::Null => ValueRef::Null,
                        types::ValueRef::Integer(i) => ValueRef::Integer(i),
                        types::ValueRef::Real(r) => ValueRef::Real(r),
                        types::ValueRef::Text(t) => ValueRef::Text(t),
                        types::ValueRef::Blob(b) => ValueRef::Blob(b),
                    }
                    .put(&mut block);
                }
            }
            Ok(block)
        })
    }
}

export!(Engine);
