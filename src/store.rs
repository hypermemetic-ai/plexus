//! One SQLite database per activation (feature `store`).
//!
//! An activation keeps a [`Store`] as a field, made in its `Default`, and
//! runs SQL on it: [`execute`](Store::execute), [`query`](Store::query),
//! [`query_one`](Store::query_one), [`transaction`](Store::transaction).
//! The same source is rusqlite in a process and, in a component, the import
//! `plexus:store/sql` (`wit/store.wit`): SQLite is the host's, once for
//! every activation it runs, and none of it is in the activation's wasm.
//! The database is opened at its first use, not when it is made: the
//! activation is made before [`main`](crate::main) has read where its bytes
//! go.
//!
//! Natively the bytes go to `<DIR>/<name>.db`, DIR being `--data DIR` or
//! the environment's `PLEXUS_DATA`; with neither, to memory, said once on
//! stderr. In a component they go where the host keeps them.

use std::cell::OnceCell;
use std::fmt;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
use native as db;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native::set_data;

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
use wasm as db;

/// A value SQLite keeps: a parameter, or a column of a row.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// What SQLite refused, or a column that is not the type asked for.
#[derive(Debug, Clone, PartialEq)]
pub struct Error(pub String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// One row of a query, its columns by position.
pub struct Row<'a>(&'a [Value]);

impl Row<'_> {
    pub fn get<T: FromValue>(&self, column: usize) -> Result<T> {
        let v = self.0.get(column).ok_or_else(|| Error(format!("no column {column}")))?;
        T::from_value(v).map_err(|e| Error(format!("column {column}: {e}")))
    }
}

/// A Rust value read out of a column.
pub trait FromValue: Sized {
    fn from_value(v: &Value) -> Result<Self>;
}

// Says the value's kind, not the value: formatting a float would put
// Rust's float printing into every component that stores.
fn wrong(v: &Value, want: &str) -> Error {
    let kind = match v {
        Value::Null => "null",
        Value::Integer(_) => "an integer",
        Value::Real(_) => "a real",
        Value::Text(_) => "text",
        Value::Blob(_) => "a blob",
    };
    Error(format!("{kind} is not {want}"))
}

macro_rules! integers {
    ($($t:ty),*) => {$(
        impl From<$t> for Value {
            fn from(n: $t) -> Value {
                Value::Integer(n as i64)
            }
        }
        impl FromValue for $t {
            fn from_value(v: &Value) -> Result<$t> {
                match v {
                    Value::Integer(n) => <$t>::try_from(*n).map_err(|_| wrong(v, stringify!($t))),
                    _ => Err(wrong(v, stringify!($t))),
                }
            }
        }
    )*};
}
integers!(i8, i16, i32, i64, u8, u16, u32);

impl From<bool> for Value {
    fn from(b: bool) -> Value {
        Value::Integer(b as i64)
    }
}
impl FromValue for bool {
    fn from_value(v: &Value) -> Result<bool> {
        i64::from_value(v).map(|n| n != 0)
    }
}
impl From<f64> for Value {
    fn from(r: f64) -> Value {
        Value::Real(r)
    }
}
impl From<f32> for Value {
    fn from(r: f32) -> Value {
        Value::Real(r as f64)
    }
}
impl FromValue for f64 {
    fn from_value(v: &Value) -> Result<f64> {
        match v {
            Value::Real(r) => Ok(*r),
            Value::Integer(n) => Ok(*n as f64),
            _ => Err(wrong(v, "a number")),
        }
    }
}
impl FromValue for f32 {
    fn from_value(v: &Value) -> Result<f32> {
        f64::from_value(v).map(|r| r as f32)
    }
}
impl From<String> for Value {
    fn from(t: String) -> Value {
        Value::Text(t)
    }
}
impl From<&str> for Value {
    fn from(t: &str) -> Value {
        Value::Text(t.into())
    }
}
impl FromValue for String {
    fn from_value(v: &Value) -> Result<String> {
        match v {
            Value::Text(t) => Ok(t.clone()),
            _ => Err(wrong(v, "text")),
        }
    }
}
impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Value {
        Value::Blob(b)
    }
}
impl FromValue for Vec<u8> {
    fn from_value(v: &Value) -> Result<Vec<u8>> {
        match v {
            Value::Blob(b) => Ok(b.clone()),
            _ => Err(wrong(v, "a blob")),
        }
    }
}
impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(o: Option<T>) -> Value {
        o.map_or(Value::Null, Into::into)
    }
}
impl<T: FromValue> FromValue for Option<T> {
    fn from_value(v: &Value) -> Result<Option<T>> {
        match v {
            Value::Null => Ok(None),
            v => T::from_value(v).map(Some),
        }
    }
}

/// An activation's database, opened at first use with its schema run on it.
pub struct Store {
    name: &'static str,
    schema: &'static str,
    db: OnceCell<db::Db>,
}

impl Store {
    /// The database named `name`. `schema` is run whenever it is opened, so
    /// it says `create table if not exists`.
    pub fn open(name: &'static str, schema: &'static str) -> Store {
        Store { name, schema, db: OnceCell::new() }
    }

    fn db(&self) -> &db::Db {
        self.db.get_or_init(|| db::Db::open(self.name, self.schema).unwrap_or_else(|e| panic!("the store {:?} does not open: {e}", self.name)))
    }

    /// Run one statement; the number of rows it changed.
    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        self.db().execute(sql, params)
    }

    /// Run one statement; each row it answers, through `f`.
    pub fn query<T>(&self, sql: &str, params: &[Value], mut f: impl FnMut(&Row) -> Result<T>) -> Result<Vec<T>> {
        let mut all = Vec::new();
        self.db().query(sql, params, &mut |row| Ok(all.push(f(row)?)))?;
        Ok(all)
    }

    /// Run one statement; its first row through `f`, or none.
    pub fn query_one<T>(&self, sql: &str, params: &[Value], f: impl FnOnce(&Row) -> Result<T>) -> Result<Option<T>> {
        let mut f = Some(f);
        let mut first = None;
        self.db().query(sql, params, &mut |row| {
            if let Some(f) = f.take() {
                first = Some(f(row)?);
            }
            Ok(())
        })?;
        Ok(first)
    }

    /// Run `f` in a transaction: committed if it answers `Ok`, rolled back
    /// if not. `f` is not async, so no other call runs inside it.
    pub fn transaction<T>(&self, f: impl FnOnce(&Store) -> Result<T>) -> Result<T> {
        self.execute("begin", &[])?;
        match f(self) {
            Ok(t) => self.execute("commit", &[]).map(|_| t),
            Err(e) => {
                let _ = self.execute("rollback", &[]);
                Err(e)
            }
        }
    }
}
