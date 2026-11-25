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

#[derive(Debug)]
struct Evaluated {
    page: tickets::Page,
    digest: u128,
    row_bytes: usize,
}

// The fixture has immutable membership and one fixed projection, sort and
// limit. An org is therefore its complete visibility and query key.
struct Group {
    subscribers: Mutex<usize>,
    current: watch::Sender<Option<Arc<Evaluated>>>,
}

struct SharedWatch {
    store: Arc<Store>,
    org: usize,
    current: watch::Receiver<Option<Arc<Evaluated>>>,
}

impl Drop for SharedWatch {
    fn drop(&mut self) {
        let group = &self.store.groups.as_ref().unwrap()[self.org];
        let mut count = group.subscribers.lock().unwrap();
        *count -= 1;
        if *count == 0 {
            group.current.send_replace(None);
        }
    }
}

pub struct Store {
    db: Mutex<Connection>,
    members: HashMap<String, usize>,
    queries: AtomicU64,
    encoded: AtomicU64,
    revisions: Vec<watch::Sender<Revision>>,
    groups: Option<Vec<Group>>,
    read_hits: AtomicU64,
    initial_hits: AtomicU64,
}

impl Store {
    fn new(keys: &[String], orgs: usize, rows: usize) -> Result<Self> {
        let hot = std::env::var("WATCH_PROFILE").as_deref() == Ok("hot");
        let mode = std::env::var("WATCH_EVALUATION").unwrap_or_else(|_| "per-watch".into());
        ensure!(
            matches!(mode.as_str(), "per-watch" | "shared"),
            "unknown evaluation mode"
        );
        Self::configured(keys, orgs, rows, hot, mode == "shared")
    }

    fn configured(
        keys: &[String],
        orgs: usize,
        rows: usize,
        hot: bool,
        shared: bool,
    ) -> Result<Self> {
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
        let members = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.clone(), crate::user_org(i, orgs, hot)))
            .collect();
        Ok(Self {
            groups: shared.then(|| {
                (0..orgs)
                    .map(|_| Group {
                        subscribers: Mutex::new(0),
                        current: watch::Sender::new(None),
                    })
                    .collect()
            }),
            read_hits: AtomicU64::new(0),
            initial_hits: AtomicU64::new(0),
            db: Mutex::new(db),
            members,
            queries: AtomicU64::new(0),
            encoded: AtomicU64::new(0),
            revisions: (0..orgs)
                .map(|_| watch::Sender::new(Revision::default()))
                .collect(),
        })
    }

    fn query(&self, org: usize) -> tickets::Page {
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

    fn evaluate(&self, org: usize) -> Evaluated {
        let page = self.query(org);
        let encoded = serde_json::to_vec(&page.rows).unwrap();
        self.encoded
            .fetch_add(encoded.len() as u64, Ordering::Relaxed);
        Evaluated {
            page,
            digest: xxhash_rust::xxh3::xxh3_128(&encoded),
            row_bytes: encoded.len(),
        }
    }

    fn read(&self, org: usize) -> tickets::Page {
        if let Some(groups) = &self.groups {
            let group = &groups[org];
            // Use the same publication boundary as writes and last-watch removal.
            let snapshot = {
                let _count = group.subscribers.lock().unwrap();
                group.current.borrow().clone()
            };
            if let Some(snapshot) = snapshot {
                self.read_hits.fetch_add(1, Ordering::Relaxed);
                return snapshot.page.clone();
            }
        }
        self.query(org)
    }

    fn subscribe(self: &Arc<Self>, org: usize) -> Option<SharedWatch> {
        let group = &self.groups.as_ref()?[org];
        let mut count = group.subscribers.lock().unwrap();
        if *count == 0 {
            group
                .current
                .send_replace(Some(Arc::new(self.evaluate(org))));
        } else {
            self.initial_hits.fetch_add(1, Ordering::Relaxed);
        }
        *count += 1;
        Some(SharedWatch {
            store: self.clone(),
            org,
            current: group.current.subscribe(),
        })
    }

    fn write(&self, org: usize) -> u64 {
        // Serialize commit + publication for this view. Readers never see
        // revision metadata from a different transaction than their rows.
        let group = self.groups.as_ref().map(|groups| &groups[org]);
        let subscribers = group.map(|g| g.subscribers.lock().unwrap());
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
        drop(db);
        if let (Some(group), Some(count)) = (group, subscribers.as_deref()) {
            if *count > 0 {
                group
                    .current
                    .send_replace(Some(Arc::new(self.evaluate(org))));
            }
        }
        seq
    }
}

#[plexus::activation]
pub mod tickets {
    use super::Store;
    use futures::future::Either;
    use futures::{Stream, stream};
    use plexus::Caller;
    use std::sync::Arc;

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
            if let Some(subscription) = store.subscribe(org) {
                return Either::Left(stream::unfold(
                    (subscription, None),
                    |(mut subscription, mut last)| async move {
                        loop {
                            if last.is_some() {
                                subscription.current.changed().await.ok()?;
                            }
                            let current = subscription.current.borrow_and_update().clone()?;
                            let version = (current.page.sequence, current.digest);
                            if let Some((old_seq, old_digest)) = last {
                                if version.0 <= old_seq || version.1 == old_digest {
                                    last = Some(version);
                                    continue;
                                }
                            }
                            return Some((current.page.clone(), (subscription, Some(version))));
                        }
                    },
                ));
            }
            let rx = store.revisions[org].subscribe();
            Either::Right(stream::unfold(
                (store, rx, None),
                move |(store, mut rx, previous)| async move {
                    loop {
                        if previous.is_some() {
                            rx.changed().await.ok()?;
                        }
                        rx.borrow_and_update();
                        let current = store.evaluate(org);
                        if previous != Some(current.digest) {
                            return Some((current.page, (store, rx, Some(current.digest))));
                        }
                    }
                },
            ))
        }
    }
}

pub async fn run(orgs: usize, rows: usize, access: super::distributed::Access) -> Result<()> {
    let keys = io::stdin().lock().lines().collect::<io::Result<Vec<_>>>()?;
    ensure!(
        orgs > 0 && orgs <= keys.len() && rows >= 50,
        "invalid fixture dimensions"
    );
    let store = Arc::new(Store::new(&keys, orgs, rows)?);
    let identity = Identity::generate()?;
    let mut server = plexus::Server::new();
    plexus::serve(tickets::Tickets(store.clone()), &mut server);
    server.method_with_caller("bench.clock", |_, _| async {
        plexus::raw(&crate::clock_us())
    });
    let observer = keys[0].clone();
    server.method_with_caller("bench.metrics", move |_, context| {
        let authorized = context.caller().is_some_and(|c| c.principal() == observer);
        let store = store.clone();
        async move {
            if !authorized {
                return Err(plexus::fail("denied", "benchmark observer required", ()));
            }
            let mut cached_views = 0;
            let mut cached_row_bytes = 0;
            let mut shared_subscriptions = 0;
            if let Some(groups) = &store.groups {
                for group in groups {
                    let count = group.subscribers.lock().unwrap();
                    shared_subscriptions += *count;
                    if let Some(snapshot) = group.current.borrow().as_ref() {
                        cached_views += 1;
                        cached_row_bytes += snapshot.row_bytes;
                    }
                }
            }
            plexus::raw(&serde_json::json!({
                "list_cache_hits": store.read_hits.load(Ordering::Relaxed),
                "initial_cache_hits": store.initial_hits.load(Ordering::Relaxed),
                "cached_views": cached_views,
                "cached_row_json_bytes": cached_row_bytes,
                "shared_subscriptions": shared_subscriptions,
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
    let access = Arc::new(access);
    for _ in 0..keys.len().div_ceil(8000) {
        let socket = tokio::net::TcpListener::bind((access.bind, 0)).await?;
        urls.push(format!("wss://{}/", socket.local_addr()?));
        let (server, listener) = (server.clone(), listener.clone());
        let access = access.clone();
        tokio::spawn(async move {
            while let Ok((tcp, peer)) = socket.accept().await {
                if !access.sources.contains(&peer.ip()) {
                    continue;
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt, StreamExt};

    fn store(shared: bool) -> Arc<Store> {
        Arc::new(
            Store::configured(&["a".into(), "b".into(), "c".into()], 2, 51, false, shared).unwrap(),
        )
    }

    #[tokio::test]
    async fn shared_views_follow_membership_and_release_the_last_subscription() {
        let store = store(true);
        let tickets = tickets::Tickets(store.clone());
        let a = plexus::Caller::new("a");
        let b = plexus::Caller::new("b");
        let c = plexus::Caller::new("c");
        let mut first = Box::pin(tickets.watch(&a));
        let mut other = Box::pin(tickets.watch(&b));
        let mut same = Box::pin(tickets.watch(&c));
        assert_eq!(first.next().await.unwrap().org, 0);
        assert_eq!(same.next().await.unwrap().org, 0);
        assert_eq!(other.next().await.unwrap().org, 1);
        assert_eq!(tickets.list(&a).rows.len(), 50);
        assert_eq!(store.queries.load(Ordering::Relaxed), 2);
        assert_eq!(store.initial_hits.load(Ordering::Relaxed), 1);
        assert_eq!(store.read_hits.load(Ordering::Relaxed), 1);
        assert_eq!(tickets.advance(&c), 1);
        assert_eq!(first.next().await.unwrap().sequence, 1);
        assert!(other.next().now_or_never().is_none());
        assert_eq!(tickets.advance(&a), 2);
        let latest = same.next().await.unwrap();
        assert_eq!(latest.sequence, 2);
        assert_eq!(latest.rows[0].status, "Revision 2");
        assert_eq!(tickets.list(&b).sequence, 0);
        assert_eq!(store.queries.load(Ordering::Relaxed), 4);
        drop(first);
        drop(same);
        let groups = store.groups.as_ref().unwrap();
        assert!(groups[0].current.borrow().is_none());
        assert!(groups[1].current.borrow().is_some());
        tickets.advance(&a);
        assert_eq!(store.queries.load(Ordering::Relaxed), 4);
        assert_eq!(tickets.list(&a).sequence, 3);
        assert_eq!(store.queries.load(Ordering::Relaxed), 5);
        assert!(groups[0].current.borrow().is_none());
    }

    #[test]
    fn concurrent_commits_publish_a_consistent_latest_snapshot() {
        let store = store(true);
        let subscription = store.subscribe(0).unwrap();
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    for _ in 0..12 {
                        store.write(0);
                    }
                });
            }
        });
        let current = subscription.current.borrow();
        let snapshot = current.as_ref().unwrap();
        assert_eq!(snapshot.page.sequence, 48);
        assert_eq!(snapshot.page.rows[0].status, "Revision 48");
        assert_eq!(store.queries.load(Ordering::Relaxed), 49);
    }

    #[tokio::test]
    async fn same_rows_do_not_wake_consumers_with_a_duplicate_page() {
        let store = store(true);
        let tickets = tickets::Tickets(store.clone());
        let a = plexus::Caller::new("a");
        let mut subscription = Box::pin(tickets.watch(&a));
        assert_eq!(subscription.next().await.unwrap().sequence, 0);
        let group = &store.groups.as_ref().unwrap()[0];
        {
            let _count = group.subscribers.lock().unwrap();
            let mut same = store.evaluate(0);
            same.page.sequence = 1;
            store.revisions[0].send_replace(Revision {
                seq: 1,
                committed_us: 0,
            });
            group.current.send_replace(Some(Arc::new(same)));
        }
        assert!(subscription.next().now_or_never().is_none());
        assert_eq!(tickets.advance(&a), 2);
        assert_eq!(subscription.next().await.unwrap().sequence, 2);
    }

    #[tokio::test]
    async fn reference_mode_queries_for_each_read_and_subscription() {
        let store = store(false);
        let tickets = tickets::Tickets(store.clone());
        let a = plexus::Caller::new("a");
        let c = plexus::Caller::new("c");
        let mut first = Box::pin(tickets.watch(&a));
        let mut second = Box::pin(tickets.watch(&c));
        assert_eq!(first.next().await.unwrap().sequence, 0);
        assert_eq!(second.next().await.unwrap().sequence, 0);
        tickets.list(&a);
        tickets.advance(&a);
        assert_eq!(first.next().await.unwrap().sequence, 1);
        assert_eq!(second.next().await.unwrap().sequence, 1);
        assert_eq!(store.queries.load(Ordering::Relaxed), 5);
        assert_eq!(store.read_hits.load(Ordering::Relaxed), 0);
    }
}
