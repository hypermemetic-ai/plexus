//! A mountable permission service. Applications validate their own target
//! schema and consult this same book at each protected resource opening.
use plexus::{Caller, Grant, Json, Store};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

pub type Entry = Grant<String, String, Value>;
type Key = (String, String, String);
type Validate = dyn Fn(&str, &Value) -> Result<(), String> + Send + Sync;

#[derive(Clone)]
pub struct Book(Arc<Policy>);
struct Policy {
    roots: HashSet<String>,
    baseline: HashSet<Key>,
    validate: Box<Validate>,
    db: Mutex<Store>,
    // Always take db before leases. Removing a sender permanently ends every
    // live lease on that exact grant, even if the grant is immediately restored.
    leases: Mutex<HashMap<Key, tokio::sync::watch::Sender<()>>>,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS requests (
 id INTEGER PRIMARY KEY AUTOINCREMENT, principal TEXT NOT NULL, method TEXT NOT NULL, target TEXT NOT NULL,
 status TEXT NOT NULL CHECK (status IN ('pending','approved','denied')), reviewer TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS pending_request ON requests(principal,method,target) WHERE status='pending';
CREATE TABLE IF NOT EXISTS overrides (
 principal TEXT NOT NULL, method TEXT NOT NULL, target TEXT NOT NULL, allowed INTEGER NOT NULL,
 PRIMARY KEY(principal,method,target));";

fn identity(id: &str) -> bool {
    !id.is_empty() && !id.contains('*') && !id.chars().any(|c| c.is_whitespace() || c.is_control())
}

impl Book {
    pub fn new(
        roots: Vec<String>,
        grants: Vec<Entry>,
        validate: impl Fn(&str, &Value) -> Result<(), String> + Send + Sync + 'static,
    ) -> Result<Self, String> {
        if roots.iter().any(|r| !identity(r)) {
            return Err("invalid Plexus admin identity".into());
        }
        let mut baseline = HashSet::new();
        for (i, grant) in grants.into_iter().enumerate() {
            let key = normalized(&validate, &grant.principal, &grant.method, grant.target)
                .map_err(|e| format!("grant {}: {e}", i + 1))?;
            baseline.insert(key);
        }
        Ok(Self(Arc::new(Policy {
            roots: roots.into_iter().collect(),
            baseline,
            validate: Box::new(validate),
            leases: Mutex::new(HashMap::new()),
            db: Mutex::new(Store::open("permissions", SCHEMA)),
        })))
    }

    fn admin(&self, caller: &Caller) -> bool {
        caller.peer() == caller.principal() && self.0.roots.contains(caller.peer())
    }

    fn key(&self, principal: &str, method: &str, target: Value) -> Result<Key, AccessError> {
        normalized(&*self.0.validate, principal, method, target).map_err(AccessError::Invalid)
    }

    fn db(&self) -> Result<std::sync::MutexGuard<'_, Store>, AccessError> {
        self.0.db.lock().map_err(|_| AccessError::Storage)
    }

    pub fn allows(
        &self,
        principal: &str,
        method: &str,
        target: Value,
    ) -> Result<bool, AccessError> {
        let k = self.key(principal, method, target)?;
        let db = self.db()?;
        self.effective(&db, &k)
    }

    pub fn lease(
        &self,
        principal: &str,
        method: &str,
        target: Value,
    ) -> Result<Lease, AccessError> {
        let k = self.key(principal, method, target)?;
        let db = self.db()?;
        if !self.effective(&db, &k)? {
            return Err(AccessError::Forbidden);
        }
        let mut active = self.0.leases.lock().map_err(|_| AccessError::Storage)?;
        active.retain(|_, sender| sender.receiver_count() > 0);
        Ok(Lease(
            active
                .entry(k)
                .or_insert_with(|| tokio::sync::watch::channel(()).0)
                .subscribe(),
        ))
    }

    fn effective(&self, db: &Store, k: &Key) -> Result<bool, AccessError> {
        let value = db.query_one(
            "SELECT allowed FROM overrides WHERE principal=? AND method=? AND target=?",
            &bind(k),
            |r| r.get::<bool>(0),
        )?;
        Ok(value.unwrap_or_else(|| self.0.baseline.contains(k)))
    }

    fn request(&self, k: Key) -> Result<PermissionRequest, AccessError> {
        let db = self.db()?;
        if self.effective(&db, &k)? {
            return Err(AccessError::AlreadyGranted);
        }
        db.transaction(|tx| {
            tx.execute("INSERT INTO requests(principal,method,target,status) VALUES (?,?,?,'pending') ON CONFLICT DO NOTHING", &bind(&k))?;
            tx.query_one("SELECT id,principal,method,target,status,reviewer FROM requests WHERE principal=? AND method=? AND target=? AND status='pending'", &bind(&k), request_row)?
                .ok_or_else(|| plexus::store::Error("pending request missing".into()))
        }).map_err(Into::into)
    }

    fn decide(
        &self,
        caller: &Caller,
        id: u32,
        approve: bool,
    ) -> Result<PermissionRequest, AccessError> {
        if !self.admin(caller) {
            return Err(AccessError::Forbidden);
        }
        let db = self.db()?;
        // Check persisted requests against the currently installed target validator.
        let request = db
            .query_one(
                "SELECT id,principal,method,target,status,reviewer FROM requests WHERE id=?",
                &[id.into()],
                request_row,
            )?
            .ok_or(AccessError::UnknownRequest)?;
        let status = if approve { "approved" } else { "denied" };
        if request.status == status {
            return Ok(request);
        }
        if request.status != "pending" {
            return Err(AccessError::AlreadyDecided);
        }
        let k = if approve {
            Some(self.key(&request.principal, &request.method, decode(request.target)?)?)
        } else {
            None
        };
        db.transaction(|tx| {
            let changed = tx.execute(
                "UPDATE requests SET status=?,reviewer=? WHERE id=? AND status='pending'",
                &[status.into(), caller.peer().into(), id.into()],
            )?;
            if changed == 1
                && let Some(k) = &k
            {
                write_grant(tx, k, true)?;
            }
            tx.query_one(
                "SELECT id,principal,method,target,status,reviewer FROM requests WHERE id=?",
                &[id.into()],
                request_row,
            )?
            .ok_or_else(|| plexus::store::Error("request disappeared".into()))
        })
        .map_err(Into::into)
        .and_then(|result| {
            if result.status == status {
                Ok(result)
            } else {
                Err(AccessError::AlreadyDecided)
            }
        })
    }
}

/// A previously admitted stream stays revoked across an immediate regrant.
#[derive(Clone)]
pub struct Lease(tokio::sync::watch::Receiver<()>);
impl Lease {
    pub async fn revoked(mut self) {
        let _ = self.0.changed().await;
    }
}

fn normalized(
    validate: &Validate,
    principal: &str,
    method: &str,
    mut target: Value,
) -> Result<Key, String> {
    if !identity(principal) {
        return Err("expected an exact principal ID".into());
    }
    validate(method, &target)?;
    target.sort_all_objects();
    Ok((principal.into(), method.into(), target.to_string()))
}
fn decode(json: Json) -> Result<Value, AccessError> {
    serde_json::from_str(&json.0).map_err(|_| AccessError::Invalid("invalid JSON target".into()))
}
fn bind(k: &Key) -> [plexus::store::Value; 3] {
    [
        k.0.as_str().into(),
        k.1.as_str().into(),
        k.2.as_str().into(),
    ]
}
fn write_grant(db: &Store, k: &Key, allowed: bool) -> plexus::store::Result<()> {
    let [p, m, t] = bind(k);
    db.execute("INSERT INTO overrides(principal,method,target,allowed) VALUES (?,?,?,?) ON CONFLICT(principal,method,target) DO UPDATE SET allowed=excluded.allowed", &[p,m,t,allowed.into()])?;
    Ok(())
}
fn request_row(row: &plexus::store::Row) -> plexus::store::Result<PermissionRequest> {
    Ok(PermissionRequest {
        id: row.get(0)?,
        principal: row.get(1)?,
        method: row.get(2)?,
        target: Json(row.get(3)?),
        status: row.get(4)?,
        reviewer: row.get(5)?,
    })
}

#[plexus::activation(context = PermissionCx)]
pub mod permissions {
    use super::*;
    #[derive(plexus::Context)]
    pub struct PermissionCx {
        #[from(caller)]
        caller: Caller,
    }
    pub struct PermissionRequest {
        pub id: u32,
        pub principal: String,
        pub method: String,
        pub target: Json,
        pub status: String,
        pub reviewer: Option<String>,
    }
    pub enum AccessError {
        Forbidden,
        Invalid(String),
        AlreadyGranted,
        AlreadyDecided,
        UnknownRequest,
        Storage,
    }
    pub struct Permissions {
        pub(super) book: Book,
    }
    impl Permissions {
        /// Ask for an exact grant for the authenticated caller.
        pub fn request(
            &self,
            cx: &PermissionCx,
            method: String,
            target: Json,
        ) -> Result<PermissionRequest, AccessError> {
            self.book.request(
                self.book
                    .key(cx.caller.principal(), &method, decode(target)?)?,
            )
        }
        /// Show your requests. A directly authenticated Plexus admin sees every request.
        pub fn requests(
            &self,
            cx: &PermissionCx,
            pending: Option<bool>,
        ) -> Result<Vec<PermissionRequest>, AccessError> {
            self.book.db()?.query("SELECT id,principal,method,target,status,reviewer FROM requests WHERE (? OR principal=?) AND (NOT ? OR status='pending') ORDER BY id DESC",
                &[self.book.admin(&cx.caller).into(), cx.caller.principal().into(), pending.unwrap_or(false).into()], request_row).map_err(Into::into)
        }
        pub fn approve(
            &self,
            cx: &PermissionCx,
            id: u32,
        ) -> Result<PermissionRequest, AccessError> {
            self.book.decide(&cx.caller, id, true)
        }
        pub fn deny(&self, cx: &PermissionCx, id: u32) -> Result<PermissionRequest, AccessError> {
            self.book.decide(&cx.caller, id, false)
        }
        /// Add or restore an exact grant immediately.
        pub fn grant(
            &self,
            cx: &PermissionCx,
            principal: String,
            method: String,
            target: Json,
        ) -> Result<(), AccessError> {
            self.change(&cx.caller, principal, method, target, true)
        }
        /// Revoke an exact grant, including a grant loaded at startup.
        pub fn revoke(
            &self,
            cx: &PermissionCx,
            principal: String,
            method: String,
            target: Json,
        ) -> Result<(), AccessError> {
            self.change(&cx.caller, principal, method, target, false)
        }
    }
}
pub use permissions::{AccessError, PermissionRequest, Permissions};
impl Permissions {
    pub fn new(book: Book) -> Self {
        Self { book }
    }
    fn change(
        &self,
        caller: &Caller,
        principal: String,
        method: String,
        target: Json,
        allowed: bool,
    ) -> Result<(), AccessError> {
        if !self.book.admin(caller) {
            return Err(AccessError::Forbidden);
        }
        let k = self.book.key(&principal, &method, decode(target)?)?;
        let db = self.book.db()?;
        db.transaction(|tx| {
            write_grant(tx, &k, allowed)?;
            let [p,m,t] = bind(&k);
            tx.execute("UPDATE requests SET status=?,reviewer=? WHERE principal=? AND method=? AND target=? AND status='pending'",
                &[if allowed { "approved" } else { "denied" }.into(), caller.peer().into(), p,m,t])?;
            Ok(())
        })?;
        if !allowed {
            self.book
                .0
                .leases
                .lock()
                .map_err(|_| AccessError::Storage)?
                .remove(&k);
        }
        Ok(())
    }
}
impl From<plexus::store::Error> for AccessError {
    fn from(_: plexus::store::Error) -> Self {
        Self::Storage
    }
}
