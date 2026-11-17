//! names › One activation cannot open another's database: a yard and the
//! shelf it holds in this process each open a store named `kiln`, and each
//! gets its own, in the directory of the place it is served at.
//!
//! The data directory is read from the environment, once a process; so the
//! test runs itself again with `PLEXUS_DATA` set, rather than set it here.

use std::sync::Arc;

use plexus::{Connection, Server, pair};

/// Keeps what it is given.
#[plexus::activation]
pub mod shelf {
    use plexus::Store;

    pub struct Shelf {
        store: Store,
    }

    impl Default for Shelf {
        fn default() -> Shelf {
            Shelf { store: Store::open("kiln", "create table if not exists kept (what text not null)") }
        }
    }

    impl Shelf {
        /// Keep `what`; how many it keeps now.
        pub fn put(&self, what: String) -> u32 {
            self.store.execute("insert into kept (what) values (?1)", &[what.into()]).expect("a write");
            self.store.query_one("select count(*) from kept", &[], |r| r.get(0)).expect("a read").expect("a count")
        }
    }
}

/// Holds a shelf, and opens a store of the same name.
#[plexus::activation]
pub mod yard {
    use super::shelf::Shelf;
    use plexus::Store;

    pub struct Yard {
        store: Store,
        shelf: Shelf,
    }

    impl Default for Yard {
        fn default() -> Yard {
            Yard { store: Store::open("kiln", "create table if not exists kept (what text not null)"), shelf: Shelf::default() }
        }
    }

    impl Yard {
        /// Keep `what`; how many it keeps now.
        pub fn put(&self, what: String) -> u32 {
            self.store.execute("insert into kept (what) values (?1)", &[what.into()]).expect("a write");
            self.store.query_one("select count(*) from kept", &[], |r| r.get(0)).expect("a read").expect("a count")
        }

        /// What the shelf keeps, asked through Rust, not through the server.
        pub fn on_the_shelf(&self) -> u32 {
            self.shelf.put("by the yard".into())
        }

        #[child]
        pub fn shelf(&self) -> &Shelf {
            &self.shelf
        }
    }
}

#[test]
fn another_activations_name_is_not_its_database() {
    let Some(dir) = std::env::var_os("PLEXUS_DATA") else {
        let dir = std::env::temp_dir().join(format!("plexus-store-names-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ran = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "another_activations_name_is_not_its_database", "--nocapture"])
            .env("PLEXUS_DATA", &dir)
            .status()
            .unwrap();
        let files = [dir.join("yard/kiln.db"), dir.join("yard.shelf/kiln.db"), dir.join("kiln.db")].map(|f| f.is_file());
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(ran.success(), "the run with PLEXUS_DATA failed");
        assert_eq!(files, [true, true, false], "each activation's file in its place's directory, none at the top");
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut server = Server::new();
        yard::serve(yard::Yard::default(), &mut server, "yard");
        let (near, far) = pair();
        tokio::spawn(Arc::new(server).serve(far));
        let (conn, driving) = Connection::new(near, None);
        tokio::spawn(driving);
        let put = async |at: &str, what: &str| -> u32 { serde_json::from_str(conn.call(at, serde_json::json!({ "what": what })).await.unwrap().get()).unwrap() };

        // The parent first reaches the shelf's store through Rust: still the shelf's.
        let on: u32 = serde_json::from_str(conn.call("yard.on-the-shelf", ()).await.unwrap().get()).unwrap();
        assert_eq!(on, 1);
        assert_eq!(put("yard.put", "a pot").await, 1, "the yard's kiln holds the yard's alone");
        assert_eq!(put("yard.shelf.put", "a bowl").await, 2, "the shelf's kiln holds the shelf's alone");
        assert_eq!(put("yard.put", "a cup").await, 2);
    });
    assert!(dir.join("yard.shelf/kiln.db").is_file());
}
