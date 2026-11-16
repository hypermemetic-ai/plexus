//! A store in a component: the import `plexus:store/sql`, which the host
//! answers with its one SQLite.

use super::{Error, Result, Row, Value};

wit_bindgen::generate!({
    path: "wit/store.wit",
    world: "store",
    with: { "plexus:store/sql@0.0.1/value": crate::store::Value },
});

use self::plexus::store::sql;

pub(super) struct Db(u32);

impl Db {
    pub(super) fn open(name: &str, schema: &str) -> Result<Db> {
        sql::open(name, schema).map(Db).map_err(Error)
    }

    pub(super) fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        sql::execute(self.0, sql, params).map_err(Error)
    }

    pub(super) fn query(&self, sql: &str, params: &[Value], f: &mut dyn FnMut(&Row) -> Result<()>) -> Result<()> {
        let rows = sql::query(self.0, sql, params).map_err(Error)?;
        rows.values.chunks(rows.columns.max(1) as usize).try_for_each(|row| f(&Row(row)))
    }
}
