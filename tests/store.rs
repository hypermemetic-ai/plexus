//! store › `Store`, natively: what an activation's SQL does on rusqlite.
//! The same source runs in a component over plexus:store/sql (plexus-rt's
//! page tests drive the kiln and the glaze that way).

use plexus::Store;
use plexus::store::{Error, Row, Value};

const SCHEMA: &str = "create table if not exists firing (id integer primary key, cone integer not null, hours real, loaded_by text, photo blob)";

fn firing(r: &Row) -> plexus::store::Result<(u32, u8, Option<f32>, String)> {
    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
}

#[test]
fn execute_query_and_query_one() {
    let s = Store::open("store-a", SCHEMA);
    assert_eq!(s.execute("insert into firing (cone, hours, loaded_by) values (?1, ?2, ?3)", &[6u8.into(), 2.5f32.into(), "ada".into()]).unwrap(), 1);
    let id: u32 = s.query_one("insert into firing (cone, loaded_by) values (?1, ?2) returning id", &[9u8.into(), String::from("bo").into()], |r| r.get(0)).unwrap().unwrap();
    assert_eq!(id, 2);
    assert_eq!(s.query("select id, cone, hours, loaded_by from firing order by id", &[], firing).unwrap(), vec![(1, 6, Some(2.5), "ada".into()), (2, 9, None, "bo".into())]);
    assert_eq!(s.query_one("select id, cone, hours, loaded_by from firing where id = ?1", &[7u32.into()], firing).unwrap(), None);
    let photo: Vec<u8> = s.query_one("select ?1", &[vec![1u8, 2, 3].into()], |r| r.get(0)).unwrap().unwrap();
    assert_eq!(photo, [1, 2, 3]);
}

#[test]
fn errors_are_values() {
    let s = Store::open("store-b", SCHEMA);
    let Error(e) = s.execute("insert into nowhere values (1)", &[]).unwrap_err();
    assert!(e.contains("no such table: nowhere"), "{e}");
    let Error(e) = s.query_one("select 'text'", &[], |r| r.get::<u32>(0)).unwrap_err();
    assert_eq!(e, "column 0: text is not u32");
    let Error(e) = s.query_one("select 300", &[], |r| r.get::<u8>(0)).unwrap_err();
    assert_eq!(e, "column 0: an integer is not u8");
}

#[test]
fn a_transaction_commits_or_rolls_back() {
    let s = Store::open("store-c", SCHEMA);
    s.transaction(|s| s.execute("insert into firing (cone) values (1)", &[])).unwrap();
    let refused = s.transaction(|s| {
        s.execute("insert into firing (cone) values (2)", &[])?;
        Err::<(), _>(Error("changed my mind".into()))
    });
    assert_eq!(refused, Err(Error("changed my mind".into())));
    assert_eq!(s.query("select cone from firing", &[], |r| r.get::<i64>(0)).unwrap(), [1]);
    assert_eq!(s.query_one("select ?1", &[Value::Null], |r| r.get::<Option<i64>>(0)).unwrap(), Some(None));
}
