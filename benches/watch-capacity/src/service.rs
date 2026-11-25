//! Native transport and query baseline, independent of the proposed watch IR.
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Result, ensure};
use plexus::tls::{Identity, Listen};
use rusqlite::Connection;
use tokio::sync::watch;

#[derive(Clone, Copy, Default)]
struct Revision {
    seq: u64,
    committed_us: u64,
}

pub struct Store {
    db: Mutex<Connection>,
    members: HashMap<String, usize>,
    queries: AtomicU64,
    encoded: AtomicU64,
    revisions: Vec<watch::Sender<Revision>>,
}

impl Store {
    fn new(keys: &[String], orgs: usize, rows: usize) -> Result<Self> {
        let mut db = Connection::open_in_memory()?;
        db.execute_batch(
            "CREATE TABLE ticket(org INTEGER, id INTEGER, title TEXT, status TEXT,
                          PRIMARY KEY(org, id)) WITHOUT ROWID;",
        )?;
        let tx = db.transaction()?;
        {
            let mut insert = tx.prepare("INSERT INTO ticket VALUES (?1, ?2, ?3, 'Ready')")?;
            for org in 0..orgs {
                for n in 0..rows {
                    insert.execute(rusqlite::params![
                        org as i64,
                        n as i64,
                        format!("Organization {org} ticket {n}")
                    ])?;
                }
            }
        }
        tx.commit()?;
        let hot = std::env::var("WATCH_PROFILE").as_deref() == Ok("hot");
        let members = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.clone(), crate::user_org(i, orgs, hot)))
            .collect();
        Ok(Self {
            db: Mutex::new(db),
            members,
            queries: AtomicU64::new(0),
            encoded: AtomicU64::new(0),
            revisions: (0..orgs)
                .map(|_| watch::Sender::new(Revision::default()))
                .collect(),
        })
    }

    fn read(&self, org: usize) -> tickets::Page {
        self.queries.fetch_add(1, Ordering::Relaxed);
        let db = self.db.lock().unwrap();
        let revision = *self.revisions[org].borrow();
        let mut q = db
            .prepare_cached("SELECT id,title,status FROM ticket WHERE org=?1 ORDER BY id LIMIT 50")
            .unwrap();
        let rows = q
            .query_map([org as i64], |r| {
                Ok(tickets::Row {
                    id: r.get(0)?,
                    title: r.get(1)?,
                    status: r.get(2)?,
                })
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        tickets::Page {
            org: org as u32,
            sequence: revision.seq,
            committed_us: revision.committed_us,
            rows,
        }
    }

    fn write(&self, org: usize) -> u64 {
        // Keep commit, revision update and read under the same lock. Signaling
        // after sqlite's autocommit gives every observing reader a timestamp.
        let db = self.db.lock().unwrap();
        let source = &self.revisions[org];
        let seq = source.borrow().seq + 1;
        db.execute(
            "UPDATE ticket SET status=?1 WHERE org=?2 AND id=0",
            rusqlite::params![format!("Revision {seq}"), org as i64],
        )
        .unwrap();
        source.send_replace(Revision {
            seq,
            committed_us: crate::clock_us(),
        });
        seq
    }
}

#[plexus::activation]
pub mod tickets {
    use super::Store;
    use futures::{Stream, stream};
    use plexus::Caller;
    use std::sync::Arc;
    use xxhash_rust::xxh3::xxh3_128;

    pub struct Row {
        pub id: u32,
        pub title: String,
        pub status: String,
    }
    pub struct Page {
        pub org: u32,
        pub sequence: u64,
        pub committed_us: u64,
        pub rows: Vec<Row>,
    }
    pub struct Tickets(pub Arc<Store>);

    impl Tickets {
        pub fn list(&self, caller: &Caller) -> Page {
            self.0.read(self.0.members[caller.principal()])
        }
        pub fn advance(&self, caller: &Caller) -> u64 {
            self.0.write(self.0.members[caller.principal()])
        }
        pub fn watch(&self, caller: &Caller) -> impl Stream<Item = Page> {
            let store = self.0.clone();
            let org = store.members[caller.principal()];
            let rx = store.revisions[org].subscribe();
            stream::unfold(
                (store, rx, None),
                move |(store, mut rx, previous)| async move {
                    loop {
                        if previous.is_some() {
                            rx.changed().await.ok()?;
                        }
                        rx.borrow_and_update();
                        let page = store.read(org);
                        let encoded = serde_json::to_vec(&page.rows).unwrap();
                        store
                            .encoded
                            .fetch_add(encoded.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        let digest = xxh3_128(&encoded);
                        if previous != Some(digest) {
                            return Some((page, (store, rx, Some(digest))));
                        }
                    }
                },
            )
        }
    }
}

pub async fn run(orgs: usize, rows: usize) -> Result<()> {
    let keys = io::stdin().lock().lines().collect::<io::Result<Vec<_>>>()?;
    ensure!(
        orgs > 0 && orgs <= keys.len() && rows >= 50,
        "invalid fixture dimensions"
    );
    let store = Arc::new(Store::new(&keys, orgs, rows)?);
    let identity = Identity::generate()?;
    let mut server = plexus::Server::new();
    plexus::serve(tickets::Tickets(store.clone()), &mut server);
    let observer = keys[0].clone();
    server.method_with_caller("bench.metrics", move |_, context| {
        let authorized = context.caller().is_some_and(|c| c.principal() == observer);
        let store = store.clone();
        async move {
            if !authorized {
                return Err(plexus::fail("denied", "benchmark observer required", ()));
            }
            plexus::raw(&serde_json::json!({
                "page_queries":store.queries.load(Ordering::Relaxed),
                "watch_bytes_hashed":store.encoded.load(Ordering::Relaxed)
            }))
        }
    });
    // Multiple loopback destination ports give a colocated driver enough TCP
    // four-tuples. Every listener feeds the same process, store and acceptor.
    let listener = Arc::new(Listen::new(&identity, keys.iter().map(String::as_str))?);
    let server = Arc::new(server);
    let mut urls = Vec::new();
    for _ in 0..keys.len().div_ceil(8000) {
        let socket = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        urls.push(format!("wss://{}/", socket.local_addr()?));
        let (server, listener) = (server.clone(), listener.clone());
        tokio::spawn(async move {
            while let Ok((tcp, _)) = socket.accept().await {
                let (server, listener) = (server.clone(), listener.clone());
                tokio::spawn(async move {
                    if let Ok((channel, principal)) = listener.accept(tcp).await {
                        let _ = server
                            .serve_as(channel, plexus::Caller::new(principal))
                            .await;
                    }
                });
            }
        });
    }
    println!("{} {}", urls.join(","), identity.principal());
    io::stdout().flush()?;
    std::future::pending().await
}
