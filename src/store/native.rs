//! A store in a process: rusqlite, its file under `--data DIR`, in the
//! directory of the place its activation is served at.

use rusqlite::types::{self, ToSqlOutput};
use rusqlite::{Connection, ToSql, params_from_iter};

use super::{Error, Result, Row, Value, ValueRef};

static DATA: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

/// Where every store of this process keeps its file: `--data DIR`, read by
/// [`main`](crate::main) before the first store opens.
pub(crate) fn set_data(dir: std::path::PathBuf) {
    let _ = DATA.set(dir);
}

fn err(e: rusqlite::Error) -> Error {
    Error(e.to_string())
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self.as_ref() {
            ValueRef::Null => types::ValueRef::Null,
            ValueRef::Integer(i) => types::ValueRef::Integer(i),
            ValueRef::Real(r) => types::ValueRef::Real(r),
            ValueRef::Text(t) => types::ValueRef::Text(t),
            ValueRef::Blob(b) => types::ValueRef::Blob(b),
        }))
    }
}

fn value(v: types::ValueRef) -> ValueRef {
    match v {
        types::ValueRef::Null => ValueRef::Null,
        types::ValueRef::Integer(i) => ValueRef::Integer(i),
        types::ValueRef::Real(r) => ValueRef::Real(r),
        types::ValueRef::Text(t) => ValueRef::Text(t),
        types::ValueRef::Blob(b) => ValueRef::Blob(b),
    }
}

pub(super) struct Db(Connection);

impl Db {
    pub(super) fn open(place: Option<&str>, name: &str, schema: &str) -> Result<Db> {
        let dir = DATA.get().cloned().or_else(|| std::env::var_os("PLEXUS_DATA").filter(|d| !d.is_empty()).map(std::path::PathBuf::from));
        let db = match dir {
            Some(dir) => {
                let dir = place.map_or(dir.clone(), |p| dir.join(p));
                std::fs::create_dir_all(&dir).map_err(|e| Error(format!("{}: {e}", dir.display())))?;
                Connection::open(dir.join(format!("{name}.db")))
            }
            None => {
                let of = place.map(|p| format!(" of {p}")).unwrap_or_default();
                eprintln!("plexus: the store {name:?}{of} is in memory and goes when the process ends (no --data DIR, no PLEXUS_DATA)");
                Connection::open_in_memory()
            }
        }
        .map_err(err)?;
        db.execute_batch(schema).map_err(|e| Error(format!("the schema: {e}")))?;
        Ok(Db(db))
    }

    pub(super) fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        self.0.prepare_cached(sql).and_then(|mut s| s.execute(params_from_iter(params))).map(|n| n as u64).map_err(err)
    }

    pub(super) fn query(&self, sql: &str, params: &[Value], f: &mut dyn FnMut(&Row) -> Result<()>) -> Result<()> {
        let mut st = self.0.prepare_cached(sql).map_err(err)?;
        let columns = st.column_count();
        let mut rows = st.query(params_from_iter(params)).map_err(err)?;
        while let Some(row) = rows.next().map_err(err)? {
            let values = (0..columns).map(|i| row.get_ref(i).map(value)).collect::<rusqlite::Result<Vec<_>>>().map_err(err)?;
            f(&Row(&values))?;
        }
        Ok(())
    }
}
