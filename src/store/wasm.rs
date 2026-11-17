//! A store in a component: the import `plexus:store/sql`, which the host
//! answers with its one SQLite. Parameters go and rows come back as one
//! block of bytes each (`packed.rs`), so a read crosses as one copy, not a
//! value at a time; a row's columns are borrowed from the block.

use super::{Error, Result, Row, Value, ValueRef};

wit_bindgen::generate!({ path: "wit/store.wit", world: "store" });

use self::plexus::store::sql;

fn pack(params: &[Value]) -> Vec<u8> {
    let mut block = Vec::new();
    for p in params {
        p.as_ref().put(&mut block);
    }
    block
}

pub(super) struct Db(u32);

impl Db {
    pub(super) fn open(name: &str, schema: &str) -> Result<Db> {
        sql::open(name, schema).map(Db).map_err(Error)
    }

    pub(super) fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        sql::execute(self.0, sql, &pack(params)).map_err(Error)
    }

    pub(super) fn query(&self, sql: &str, params: &[Value], f: &mut dyn FnMut(&Row) -> Result<()>) -> Result<()> {
        let block = sql::query(self.0, sql, &pack(params)).map_err(Error)?;
        let bad = |e: &str| Error(format!("the engine's rows: {e}"));
        let (columns, mut rest) = block.split_first_chunk::<4>().ok_or_else(|| bad("no column count"))?;
        let columns = u32::from_le_bytes(*columns) as usize;
        let mut row = Vec::with_capacity(columns);
        while !rest.is_empty() {
            row.clear();
            for _ in 0..columns {
                row.push(ValueRef::take(&mut rest).map_err(bad)?);
            }
            f(&Row(&row))?;
        }
        Ok(())
    }
}
