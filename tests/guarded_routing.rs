//! Address keys are consumed at their own segment before a call crosses a mount.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::{StreamExt, channel::mpsc, future::BoxFuture};
use plexus::{
    Activation, Caller, Connection, Failure, Passed, Place, Raw, Remote, Server, fail, pair, raw,
};

fn connect(server: Server, caller: &str) -> Connection {
    let (near, far) = pair();
    tokio::spawn(Arc::new(server).serve_as(far, Caller::new(caller)));
    let (conn, driver) = Connection::new(near, None);
    tokio::spawn(driver);
    conn
}

#[tokio::test]
async fn a_mount_preserves_its_own_keys_and_groups_below_it() {
    let mut far = Server::new();
    far.describe("oven.kiln", r#"{"hash":"kiln"}"#).unwrap();
    far.method_with_context(
        "oven.kiln.get",
        "oven.kiln",
        &["room"],
        |_, call| async move {
            raw(&(
                call.require_caller()?.principal(),
                call.keys().parse::<u32>("room")?,
            ))
        },
    );
    far.method("oven.kiln.ambient", |_| async { raw(&20) });
    far.method_with_context(
        "oven.kiln.shelf.get",
        "oven.kiln.shelf",
        &["label"],
        |_, call| async move {
            raw(&(
                call.require_caller()?.principal(),
                call.keys().parse::<String>("label")?,
            ))
        },
    );
    let far = connect(far, "service");
    let mut near = Server::new();
    near.guard_descendants("shop", &["tenant"], |call| async move {
        if call.require_caller()?.principal() == "visitor"
            && call.keys().parse::<String>("tenant")? == "a.cme"
        {
            Ok(())
        } else {
            Err(fail("denied", "no access to this shop", "shop"))
        }
    });
    let remote = Remote::new(far, "oven.kiln");
    near.mount("shop.kiln", Box::new(remote.clone()));
    near.mount("shop.backup", Box::new(remote.clone()));
    near.mount("shop.opaque", Box::new(Opaque(remote)));
    let near = connect(near, "visitor");

    for mount in ["kiln", "backup"] {
        let route = format!("shop[tenant=a.cme].{mount}");
        assert_eq!(
            near.call(&format!("{route}[room=7].get"), ())
                .await
                .unwrap()
                .get(),
            r#"["service",7]"#,
        );
        assert_eq!(
            near.call(&format!("{route}.get"), ())
                .await
                .unwrap_err()
                .code(),
            Some("invalid-context")
        );
        assert_eq!(
            near.call(&format!("shop[tenant=other].{mount}[room=7].get"), ())
                .await
                .unwrap_err()
                .code(),
            Some("denied")
        );
        assert_eq!(
            near.call(&format!("{route}.ambient"), ())
                .await
                .unwrap()
                .get(),
            "20"
        );
        assert_eq!(
            near.call(&format!("{route}.shelf[label=b.c].get"), ())
                .await
                .unwrap()
                .get(),
            r#"["service","b.c"]"#,
        );
        assert_eq!(
            near.call(&format!("{route}._hash"), ())
                .await
                .unwrap()
                .get(),
            r#""kiln""#
        );
        assert_eq!(
            near.call(&format!("shop.{mount}._hash"), ())
                .await
                .unwrap()
                .get(),
            r#""kiln""#
        );
        assert_eq!(
            near.call(&format!("shop.{mount}.shelf[label=b.c].get"), ())
                .await
                .unwrap_err()
                .code(),
            Some("invalid-context")
        );
        assert_eq!(
            near.call(&format!("shop[tenant=other].{mount}.unknown"), ())
                .await
                .unwrap_err()
                .code(),
            Some("denied")
        );
        assert_eq!(
            near.call(&format!("{route}.unknown"), ())
                .await
                .unwrap_err()
                .code(),
            Some("not-found")
        );
        assert_eq!(
            near.call(&format!("shop[tenant=other].{mount}._hash"), ())
                .await
                .unwrap_err()
                .code(),
            Some("denied")
        );
    }
    assert_eq!(
        near.call("shop[tenant=a.cme].opaque.ambient", ())
            .await
            .unwrap()
            .get(),
        "20"
    );
    assert_eq!(
        near.call("shop[tenant=a.cme].opaque[room=7].get", ())
            .await
            .unwrap_err()
            .code(),
        Some("invalid-context"),
    );
    for address in [
        "shop.kiln[tenant=a.cme].shelf.get",
        "shop[tenant=a.cme].kiln[label=b.c].shelf.get",
        "shop[tenant=a.cme].kiln.shelf[label=b.c][other=d].get",
        "shop[tenant=a.cme].kiln.shelf..get",
        "shop.kiln.._ir",
        "shop[tenant=a.cme]kiln.shelf.get",
    ] {
        assert_eq!(
            near.call(address, ()).await.unwrap_err().code(),
            Some("invalid-context"),
            "{address}"
        );
    }
    assert_eq!(
        near.call("shops.kiln.shelf.get", ())
            .await
            .unwrap_err()
            .code(),
        Some("not-found"),
    );
}

// A preexisting implementor can continue passing unkeyed calls unchanged.
struct Opaque(Remote);

impl Activation for Opaque {
    fn pass(&self, method: &str, params: Option<Raw>) -> Passed {
        self.0.pass(method, params)
    }

    fn tree(&self, at: &str) -> BoxFuture<'static, Result<Vec<Place>, Failure>> {
        self.0.tree(at)
    }
}

struct SignalOnDrop(mpsc::UnboundedSender<()>);

impl Drop for SignalOnDrop {
    fn drop(&mut self) {
        let _ = self.0.unbounded_send(());
    }
}

#[tokio::test]
async fn a_cancel_during_validation_leaves_the_remote_untouched() {
    let (entered, mut entering) = mpsc::unbounded();
    let (dropped, mut dropping) = mpsc::unbounded();
    let dispatches = Arc::new(AtomicUsize::new(0));
    let mut far = Server::new();
    far.method("kiln.get", {
        let dispatches = dispatches.clone();
        move |_| {
            dispatches.fetch_add(1, Ordering::Relaxed);
            async { raw(&"fired") }
        }
    });
    let mut near = Server::new();
    near.guard_descendants("shop", &[], {
        move |_| {
            let (entered, dropped) = (entered.clone(), dropped.clone());
            async move {
                let _drop = SignalOnDrop(dropped);
                let _ = entered.unbounded_send(());
                std::future::pending().await
            }
        }
    });
    near.mount(
        "shop.kiln",
        Box::new(Remote::new(connect(far, "service"), "kiln")),
    );
    let near = connect(near, "visitor");
    let call = tokio::spawn(near.call("shop.kiln.get", ()));
    let timeout = Duration::from_secs(5);
    assert_eq!(
        tokio::time::timeout(timeout, entering.next())
            .await
            .unwrap(),
        Some(())
    );
    call.abort();
    assert_eq!(
        tokio::time::timeout(timeout, dropping.next())
            .await
            .unwrap(),
        Some(())
    );
    assert_eq!(dispatches.load(Ordering::Relaxed), 0);
}
