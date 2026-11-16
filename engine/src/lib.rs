//! The one SQLite of a page: the export `plexus:store/sql`, rusqlite over
//! the VFS of `vfs.rs`. Every activation's `plexus::Store` in a component
//! reaches it through the import of that name, so SQLite is fetched once,
//! not compiled into each activation.

use std::cell::RefCell;

use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{Connection, ToSql, params_from_iter};

mod vfs;

wit_bindgen::generate!({ path: "../wit", world: "engine" });

use exports::plexus::store::sql::{Guest, Rows, Value};

thread_local!(static DBS: RefCell<Vec<(String, Connection)>> = const { RefCell::new(Vec::new()) });

fn with<T>(db: u32, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T, String> {
    DBS.with_borrow(|dbs| match dbs.get(db as usize) {
        Some((_, c)) => f(c).map_err(|e| e.to_string()),
        None => Err(format!("no database {db}")),
    })
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Value::Null => ValueRef::Null,
            Value::Integer(i) => ValueRef::Integer(*i),
            Value::Real(r) => ValueRef::Real(*r),
            Value::Text(t) => ValueRef::Text(t.as_bytes()),
            Value::Blob(b) => ValueRef::Blob(b),
        }))
    }
}

fn value(v: ValueRef) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Integer(i),
        ValueRef::Real(r) => Value::Real(r),
        ValueRef::Text(t) => Value::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Value::Blob(b.to_vec()),
    }
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
    fn execute(db: u32, sql: String, params: Vec<Value>) -> Result<u64, String> {
        with(db, |c| c.prepare_cached(&sql)?.execute(params_from_iter(&params)).map(|n| n as u64))
    }

    fn query(db: u32, sql: String, params: Vec<Value>) -> Result<Rows, String> {
        with(db, |c| {
            let mut st = c.prepare_cached(&sql)?;
            let columns = st.column_count();
            let mut rows = st.query(params_from_iter(&params))?;
            let mut values = Vec::new();
            while let Some(row) = rows.next()? {
                for i in 0..columns {
                    values.push(value(row.get_ref(i)?));
                }
            }
            Ok(Rows { columns: columns as u32, values })
        })
    }
}

export!(Engine);
