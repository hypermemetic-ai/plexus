//! Native transport and query baseline, independent of the proposed watch IR.
mod timing;

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
    page: StoredPage,
    sequence: u64,
    digest: u128,
    row_bytes: usize,
}

#[derive(Debug)]
enum StoredPage {
    Shared(plexus::Encoded<tickets::Page>),
    PerCall(tickets::Page),
}

impl StoredPage {
    fn answer(&self, store: &Store) -> plexus::Encoded<tickets::Page> {
        match self {
            Self::Shared(page) => page.clone(),
            Self::PerCall(page) => store.encode(page),
        }
    }
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
    timing: Option<Arc<timing::Timing>>,
    db: Mutex<Connection>,
    members: HashMap<String, usize>,
    queries: AtomicU64,
    encoded: AtomicU64,
    revisions: Vec<watch::Sender<Revision>>,
    groups: Option<Vec<Group>>,
    share_encoding: bool,
    page_encodes: AtomicU64,
    read_hits: AtomicU64,
    initial_hits: AtomicU64,
}

impl Store {
    pub(crate) fn new(keys: &[String], orgs: usize, rows: usize) -> Result<Self> {
        let hot = std::env::var("WATCH_PROFILE").as_deref() == Ok("hot");
        let mode = std::env::var("WATCH_EVALUATION").unwrap_or_else(|_| "per-watch".into());
        ensure!(
            matches!(mode.as_str(), "per-watch" | "shared"),
            "unknown evaluation mode"
        );
        let encoding = std::env::var("WATCH_ENCODING").unwrap_or_else(|_| "shared".into());
        ensure!(matches!(encoding.as_str(), "shared" | "per-call"), "unknown encoding mode");
        let mut store = Self::configured(keys, orgs, rows, hot, mode == "shared")?;
        store.share_encoding = encoding == "shared";
        store.timing = timing::Timing::configured(orgs)?;
        Ok(store)
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
            timing: None,
            groups: shared.then(|| {
                (0..orgs)
                    .map(|_| Group {
                        subscribers: Mutex::new(0),
                        current: watch::Sender::new(None),
                    })
                    .collect()
            }),
            share_encoding: true,
            page_encodes: AtomicU64::new(0),
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

    fn encode(&self, page: &tickets::Page) -> plexus::Encoded<tickets::Page> {
        self.page_encodes.fetch_add(1, Ordering::Relaxed);
        plexus::Encoded::new(page).expect("ticket page is serializable")
    }

    fn evaluate(&self, org: usize) -> Evaluated {
        let page = self.query(org);
        let encoded = serde_json::to_vec(&page.rows).unwrap();
        self.encoded
            .fetch_add(encoded.len() as u64, Ordering::Relaxed);
        Evaluated {
            sequence: page.sequence,
            page: if self.share_encoding && self.groups.is_some() {
                StoredPage::Shared(self.encode(&page))
            } else {
                StoredPage::PerCall(page)
            },
            digest: xxhash_rust::xxh3::xxh3_128(&encoded),
            row_bytes: encoded.len(),
        }
    }

    fn read(&self, org: usize) -> plexus::Encoded<tickets::Page> {
        if let Some(groups) = &self.groups {
            let group = &groups[org];
            // Use the same publication boundary as writes and last-watch removal.
            let snapshot = {
                let _count = group.subscribers.lock().unwrap();
                group.current.borrow().clone()
            };
            if let Some(snapshot) = snapshot {
                self.read_hits.fetch_add(1, Ordering::Relaxed);
                return snapshot.page.answer(self);
            }
        }
        self.encode(&self.query(org))
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
        if let Some(meter) = &self.timing {
            meter.committed(org, seq);
        }
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
    use futures::{Stream, StreamExt, stream};
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
        pub fn list(&self, caller: &Caller) -> plexus::Encoded<Page> {
            self.0.read(self.0.members[caller.principal()])
        }
        pub fn advance(&self, caller: &Caller) -> u64 {
            self.0.write(self.0.members[caller.principal()])
        }
        pub fn watch(&self, caller: &Caller) -> impl Stream<Item = plexus::Encoded<Page>> {
            let store = self.0.clone();
            let org = store.members[caller.principal()];
            let observed = store.timing.as_ref().map(|m| m.watch(org));
            let items = if let Some(subscription) = store.subscribe(org) {
                Either::Left(stream::unfold(
                    (subscription, None),
                    |(mut subscription, mut last)| async move {
                        loop {
                            if last.is_some() {
                                subscription.current.changed().await.ok()?;
                            }
                            let current = subscription.current.borrow_and_update().clone()?;
                            let version = (current.sequence, current.digest);
                            if let Some((old_seq, old_digest)) = last {
                                if version.0 <= old_seq || version.1 == old_digest {
                                    last = Some(version);
                                    continue;
                                }
                            }
                            return Some((current.page.answer(&subscription.store), (subscription, Some(version))));
                        }
                    },
                ))
            } else {
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
                                return Some((current.page.answer(&store), (store, rx, Some(current.digest))));
                            }
                        }
                    },
                ))
            };
            items.map(move |item| {
                let _ = &observed;
                item
            })
        }
    }
}

// Tenant dispatch pins a connection to a replica. Concurrent dispatch serves
// a single activation on a worker pool. Both share the same store and authority.
fn serving(store: Arc<Store>, observer: String, lanes: std::num::NonZeroUsize, concurrent: bool) -> Arc<plexus::Server> {
    let mut server = plexus::Server::new();
    if concurrent {
        plexus::serve_concurrent(tickets::Tickets(store.clone()), lanes, &mut server);
    } else {
        plexus::serve(tickets::Tickets(store.clone()), &mut server);
    }
    server.method_with_caller("bench.clock", |_, _| async {
        plexus::raw(&crate::clock_us())
    });
    server.method_with_caller("bench.metrics", move |_, context| {
        let authorized = context.caller().is_some_and(|c| c.principal() == observer);
        let store = store.clone();
        async move {
            if !authorized {
                return Err(plexus::fail("denied", "benchmark observer required", ()));
            }
            let mut cached_views = 0;
            let mut cached_row_bytes = 0;
            let mut cached_page_bytes = 0;
            let mut shared_subscriptions = 0;
            if let Some(groups) = &store.groups {
                for group in groups {
                    let count = group.subscribers.lock().unwrap();
                    shared_subscriptions += *count;
                    if let Some(snapshot) = group.current.borrow().as_ref() {
                        cached_views += 1;
                        cached_row_bytes += snapshot.row_bytes;
                        if let StoredPage::Shared(page) = &snapshot.page {
                            cached_page_bytes += page.as_raw().get().len();
                        }
                    }
                }
            }
            plexus::raw(&serde_json::json!({
                "server_timing": store.timing.as_ref().map(|m| m.snapshot()),
                "activation_lanes": lanes.get(),
                "dispatch": if concurrent { "concurrent" } else { "tenant" },
                "encoding": if store.share_encoding { "shared" } else { "per-call" },
                "page_encodes": store.page_encodes.load(Ordering::Relaxed),
                "list_cache_hits": store.read_hits.load(Ordering::Relaxed),
                "initial_cache_hits": store.initial_hits.load(Ordering::Relaxed),
                "cached_views": cached_views,
                "cached_row_json_bytes": cached_row_bytes,
                "cached_page_json_bytes": cached_page_bytes,
                "shared_subscriptions": shared_subscriptions,
                "page_queries":store.queries.load(Ordering::Relaxed),
                "watch_bytes_hashed":store.encoded.load(Ordering::Relaxed)
            }))
        }
    });
    Arc::new(server)
}

pub async fn run(orgs: usize, rows: usize, access: super::distributed::Access) -> Result<()> {
    let keys = io::stdin().lock().lines().collect::<io::Result<Vec<_>>>()?;
    ensure!(
        orgs > 0 && orgs <= keys.len() && rows >= 50,
        "invalid fixture dimensions"
    );
    let store = Arc::new(Store::new(&keys, orgs, rows)?);
    let identity = Identity::generate()?;
    let lanes: usize = std::env::var("WATCH_ACTIVATION_LANES")
        .unwrap_or_else(|_| "1".into()).parse()?;
    ensure!((1..=64).contains(&lanes), "expected 1..64 activation lanes");
    let lanes = std::num::NonZeroUsize::new(lanes).unwrap();
    let dispatch = std::env::var("WATCH_DISPATCH").unwrap_or_else(|_| "tenant".into());
    ensure!(matches!(dispatch.as_str(), "tenant" | "concurrent"), "unknown activation dispatch");
    let concurrent = dispatch == "concurrent";
    let replicas = if concurrent { 1 } else { lanes.get() };
    let servers: Arc<Vec<_>> = Arc::new((0..replicas).map(|_| serving(store.clone(), keys[0].clone(), lanes, concurrent)).collect());
    // Multiple loopback destination ports give a colocated driver enough TCP
    // four-tuples. Every listener feeds the same process, store and acceptor.
    let listener = Arc::new(Listen::new(&identity, keys.iter().map(String::as_str))?);
    let mut urls = Vec::new();
    let access = Arc::new(access);
    for _ in 0..keys.len().div_ceil(8000) {
        let socket = tokio::net::TcpListener::bind((access.bind, 0)).await?;
        urls.push(format!("wss://{}/", socket.local_addr()?));
        let (servers, store, listener) = (servers.clone(), store.clone(), listener.clone());
        let access = access.clone();
        tokio::spawn(async move {
            while let Ok((tcp, peer)) = socket.accept().await {
                if !access.sources.contains(&peer.ip()) {
                    continue;
                }
                let (servers, store, listener) = (servers.clone(), store.clone(), listener.clone());
                tokio::spawn(async move {
                    if let Ok((channel, principal)) = listener.accept(tcp).await {
                        let org = store.members[&principal];
                        let channel = match &store.timing {
                            Some(meter) => meter.wrap(channel, org),
                            None => channel,
                        };
                        let _ = servers[org % servers.len()]
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

    fn page(value: plexus::Encoded<tickets::Page>) -> tickets::Page {
        serde_json::from_str(value.as_raw().get()).unwrap()
    }

    fn store(shared: bool) -> Arc<Store> {
        Arc::new(
            Store::configured(&["a".into(), "b".into(), "c".into()], 2, 51, false, shared).unwrap(),
        )
    }

    #[tokio::test]
    async fn generated_read_and_watch_keep_the_previous_client_contract() {
        // The original generated fixture's IR hash, before encoded responses.
        assert_eq!(tickets::HASH, "70e40dfb4526da26");
        let store = store(true);
        let server = serving(store.clone(), "a".into(), 2.try_into().unwrap(), true);
        let (client, transport) = plexus::pair();
        tokio::spawn(server.serve_as(transport, plexus::Caller::new("a")));
        let (connection, driver) = plexus::Connection::new(client, Some(tickets::HASH));
        tokio::spawn(driver);
        connection.ready().await.unwrap();
        let mut watch = connection.stream("tickets.watch", ()).unwrap();
        let item = watch.next().await.unwrap().unwrap();
        let answer = connection.call("tickets.list", ()).await.unwrap();
        let expected = serde_json::to_string(&store.query(0)).unwrap();
        assert_eq!(item.get(), expected);
        assert_eq!(answer.get(), expected);
        let page: tickets::Page = serde_json::from_str(answer.get()).unwrap();
        assert_eq!((page.org, page.rows.len()), (0, 50));
        assert_eq!(store.page_encodes.load(Ordering::Relaxed), 1);
        let a = store.read(0);
        let b = store.read(0);
        assert_eq!(a.as_raw().get().as_ptr(), b.as_raw().get().as_ptr());
    }

    #[test]
    #[ignore = "release microbenchmark, run with --ignored --nocapture"]
    fn repeated_50_row_page_encoding_cost() {
        use std::hint::black_box;
        use std::time::Instant;
        let store = store(true);
        let page = store.query(0);
        let shared = store.encode(&page);
        for _ in 0..3 {
            for mode in ["clone-and-encode", "encode", "shared"] {
                let start = Instant::now();
                for _ in 0..10_000 {
                    let body = match mode {
                        "clone-and-encode" => plexus::raw(&black_box(&page).clone()).unwrap(),
                        "encode" => store.encode(black_box(&page)).into_raw(),
                        _ => black_box(&shared).clone().into_raw(),
                    };
                    black_box(body);
                }
                eprintln!("{mode}: {:.0} ns/page", start.elapsed().as_nanos() as f64 / 10_000.0);
            }
        }
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
        assert_eq!(page(first.next().await.unwrap()).org, 0);
        assert_eq!(page(same.next().await.unwrap()).org, 0);
        assert_eq!(page(other.next().await.unwrap()).org, 1);
        assert_eq!(page(tickets.list(&a)).rows.len(), 50);
        assert_eq!(store.queries.load(Ordering::Relaxed), 2);
        assert_eq!(store.initial_hits.load(Ordering::Relaxed), 1);
        assert_eq!(store.read_hits.load(Ordering::Relaxed), 1);
        assert_eq!(tickets.advance(&c), 1);
        assert_eq!(page(first.next().await.unwrap()).sequence, 1);
        assert!(other.next().now_or_never().is_none());
        assert_eq!(tickets.advance(&a), 2);
        let latest = page(same.next().await.unwrap());
        assert_eq!(latest.sequence, 2);
        assert_eq!(latest.rows[0].status, "Revision 2");
        assert_eq!(page(tickets.list(&b)).sequence, 0);
        assert_eq!(store.queries.load(Ordering::Relaxed), 4);
        drop(first);
        drop(same);
        let groups = store.groups.as_ref().unwrap();
        assert!(groups[0].current.borrow().is_none());
        assert!(groups[1].current.borrow().is_some());
        tickets.advance(&a);
        assert_eq!(store.queries.load(Ordering::Relaxed), 4);
        assert_eq!(page(tickets.list(&a)).sequence, 3);
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
        assert_eq!(snapshot.sequence, 48);
        assert_eq!(page(snapshot.page.answer(&store)).rows[0].status, "Revision 48");
        assert_eq!(store.queries.load(Ordering::Relaxed), 49);
    }

    #[tokio::test]
    async fn same_rows_do_not_wake_consumers_with_a_duplicate_page() {
        let store = store(true);
        let tickets = tickets::Tickets(store.clone());
        let a = plexus::Caller::new("a");
        let mut subscription = Box::pin(tickets.watch(&a));
        assert_eq!(page(subscription.next().await.unwrap()).sequence, 0);
        let group = &store.groups.as_ref().unwrap()[0];
        {
            let _count = group.subscribers.lock().unwrap();
            store.revisions[0].send_replace(Revision {
                seq: 1,
                committed_us: 0,
            });
            group.current.send_replace(Some(Arc::new(store.evaluate(0))));
        }
        assert!(subscription.next().now_or_never().is_none());
        assert_eq!(tickets.advance(&a), 2);
        assert_eq!(page(subscription.next().await.unwrap()).sequence, 2);
    }

    #[tokio::test]
    async fn reference_mode_queries_for_each_read_and_subscription() {
        let store = store(false);
        let tickets = tickets::Tickets(store.clone());
        let a = plexus::Caller::new("a");
        let c = plexus::Caller::new("c");
        let mut first = Box::pin(tickets.watch(&a));
        let mut second = Box::pin(tickets.watch(&c));
        assert_eq!(page(first.next().await.unwrap()).sequence, 0);
        assert_eq!(page(second.next().await.unwrap()).sequence, 0);
        tickets.list(&a);
        tickets.advance(&a);
        assert_eq!(page(first.next().await.unwrap()).sequence, 1);
        assert_eq!(page(second.next().await.unwrap()).sequence, 1);
        assert_eq!(store.queries.load(Ordering::Relaxed), 5);
        assert_eq!(store.read_hits.load(Ordering::Relaxed), 0);
    }
}
