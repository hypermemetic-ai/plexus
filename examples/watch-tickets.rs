//! A caller-scoped watchable ticket board, also served to the Plexus UI browser test.
use futures::{Stream, stream};
use plexus::{
    Caller,
    tls::{Identity, Listen},
};
use std::{
    collections::HashMap,
    error::Error,
    io::{self, BufRead, Write},
    sync::Arc,
};
use tokio::sync::watch;

struct Board {
    org: u32,
    sequence: u64,
    rows: Vec<tickets::Row>,
}

struct Store {
    members: HashMap<String, usize>,
    boards: Vec<watch::Sender<Board>>,
}

impl Store {
    fn new(keys: &[String], orgs: u32, rows: u32) -> io::Result<Self> {
        if orgs == 0 || orgs as usize > keys.len() || rows == 0 {
            return Err(io::Error::other(
                "expected enrolled organizations and nonempty boards",
            ));
        }
        let members: HashMap<_, _> = keys
            .iter()
            .enumerate()
            .map(|(n, key)| (key.clone(), n % orgs as usize))
            .collect();
        if members.len() != keys.len() {
            return Err(io::Error::other("duplicate enrollment principal"));
        }
        let boards = (0..orgs)
            .map(|org| {
                let rows = (0..rows)
                    .map(|id| tickets::Row {
                        id,
                        title: format!("Organization {org} ticket {id}"),
                        status: "Ready".into(),
                    })
                    .collect();
                watch::Sender::new(Board {
                    org,
                    sequence: 0,
                    rows,
                })
            })
            .collect();
        Ok(Self { members, boards })
    }

    // Enrollment uses precisely the principals accepted by the TLS listener.
    fn board(&self, caller: &Caller) -> &watch::Sender<Board> {
        &self.boards[self.members[caller.principal()]]
    }

    fn ticket_change(&self, caller: &Caller) -> impl Stream<Item = ()> + Send + use<> {
        stream::unfold(self.board(caller).subscribe(), |mut changes| async move {
            changes.changed().await.ok()?;
            Some(((), changes))
        })
    }
}

#[plexus::activation]
mod tickets {
    use super::{Caller, Store};
    pub struct Row {
        pub id: u32,
        pub title: String,
        pub status: String,
    }
    pub struct Page {
        pub org: u32,
        pub sequence: u64,
        pub rows: Vec<Row>,
    }
    pub struct Tickets {
        pub(super) store: Store,
    }

    impl Tickets {
        #[watch(on = [store.ticket_change(caller)])]
        pub fn list(&self, caller: &Caller, status: Option<String>) -> plexus::Encoded<Page> {
            let board = self.store.board(caller).borrow();
            let rows: Vec<_> = board
                .rows
                .iter()
                .filter(|row| status.as_ref().is_none_or(|status| row.status == *status))
                .take(50)
                .cloned()
                .collect();
            // A change outside this page can advance the sequence alone.
            let digest = plexus::watch_digest(&plexus::raw(&rows).unwrap());
            plexus::Encoded::new(&Page {
                org: board.org,
                sequence: board.sequence,
                rows,
            })
            .unwrap()
            .with_watch_digest(digest)
        }

        #[watch(on = [store.ticket_change(caller)])]
        pub fn get(&self, caller: &Caller, id: u32) -> Option<Row> {
            self.store
                .board(caller)
                .borrow()
                .rows
                .get(id as usize)
                .cloned()
        }

        pub fn advance(&self, caller: &Caller) -> u64 {
            let board = self.store.board(caller);
            board.send_modify(|page| {
                page.sequence += 1;
                page.rows[0].status = format!("Revision {}", page.sequence);
            });
            board.borrow().sequence
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("client-ts") if args.len() == 1 => print!("{}", tickets::CLIENT_TS),
        Some("interface") if args.len() == 1 => println!("{}", tickets::IR),
        Some("serve-external")
            if args.len() == 5 && args[3] == "127.0.0.1" && args[4] == "127.0.0.1" =>
        {
            let keys = io::stdin().lock().lines().collect::<io::Result<Vec<_>>>()?;
            let store = Store::new(&keys, args[1].parse()?, args[2].parse()?)?;
            let identity = Identity::generate()?;
            let listener = Listen::new(&identity, keys.iter().map(String::as_str))?;
            let mut server = plexus::Server::new();
            tickets::serve(tickets::Tickets { store }, &mut server, "tickets");
            let addr = listener
                .spawn("127.0.0.1:0".parse()?, Arc::new(server))
                .await?;
            println!("wss://{addr}/ {}", identity.principal());
            io::stdout().flush()?;
            futures::future::pending::<()>().await;
        }
        _ => {
            return Err(
                "expected interface, client-ts, or serve-external ORGS ROWS 127.0.0.1 127.0.0.1"
                    .into(),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use serde_json::json;

    #[tokio::test]
    async fn filtered_watch_refills_the_page_and_suppresses_excluded_row_edits() {
        let store = Store::new(&["alice".into(), "bob".into()], 2, 51).unwrap();
        let mut server = plexus::Server::new();
        tickets::serve(tickets::Tickets { store }, &mut server, "tickets");
        let (client, peer) = plexus::pair();
        tokio::spawn(Arc::new(server).serve_as(peer, Caller::new("alice")));
        let (conn, driver) = plexus::Connection::new(client, Some(tickets::HASH));
        tokio::spawn(driver);
        let filter = json!({ "status": "Ready" });
        let mut watch = conn.stream("tickets.list-watch", &filter).unwrap();
        let first = watch.next().await.unwrap().unwrap();
        assert_eq!(
            first.get(),
            conn.call("tickets.list", &filter).await.unwrap().get()
        );
        conn.call("tickets.advance", ()).await.unwrap();
        let changed = watch.next().await.unwrap().unwrap();
        let page: tickets::Page = serde_json::from_str(changed.get()).unwrap();
        assert_eq!(page.org, 0);
        assert!(page.rows.iter().map(|r| r.id).eq(1..=50));
        conn.call("tickets.advance", ()).await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), watch.next())
                .await
                .is_err()
        );
    }
}
