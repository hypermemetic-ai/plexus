//! Local method admission and one authenticated service hop.
use std::collections::{HashMap, HashSet};

use plexus_proto::Open;
use serde::{Deserialize, Serialize};

use crate::context::keyed_path;
use crate::server::failure;
use crate::{Caller, Failure, Raw, require_caller};

// A separate open address makes a receiver without forwarding support fail
// closed. The call's normal credit, input and cancel frames keep their id.
pub(crate) const FORWARD: &str = "_as";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Forwarded {
    pub method: String,
    pub principal: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub params: Option<Raw>,
}

// Preserve an explicit JSON null as a parameter through the hop.
fn present<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Raw>, D::Error> {
    Raw::deserialize(d).map(Some)
}

pub(crate) type MethodGrant = (&'static str, &'static [&'static str]);

type Authorize = dyn Fn(&Caller, &str, &[&str]) -> bool + Send + Sync;

#[derive(Default)]
pub(crate) struct Grants {
    forwarders: HashMap<String, HashSet<String>>,
    policy: Option<Box<Authorize>>,
}

impl Grants {
    pub fn authorize(
        &mut self,
        policy: impl Fn(&Caller, &str, &[&str]) -> bool + Send + Sync + 'static,
    ) {
        self.policy = Some(Box::new(policy));
    }

    pub fn trust(&mut self, peer: &str, address: &str) {
        self.forwarders
            .entry(peer.into())
            .or_default()
            .insert(address.into());
    }

    pub fn check(
        &self,
        (grant, roles): MethodGrant,
        caller: Option<&Caller>,
    ) -> Result<(), Failure> {
        let caller = require_caller(caller)?;
        if self
            .policy
            .as_ref()
            .is_some_and(|check| check(caller, grant, roles))
        {
            Ok(())
        } else {
            Err(failure("forbidden", &format!("missing grant for {grant}")))
        }
    }

    pub fn resolve(
        &self,
        open: Open,
        peer: Option<Caller>,
        admit: impl FnOnce(&str, bool, bool) -> Result<(), Failure>,
    ) -> Result<(Open, Option<Caller>), Failure> {
        if open.method != FORWARD {
            return Ok((open, peer));
        }
        let peer = require_caller(peer.as_ref())?;
        let denied = || {
            failure(
                "forbidden",
                "this peer may not forward the caller on this method",
            )
        };
        let allowed = self.forwarders.get(peer.principal()).ok_or_else(denied)?;
        let invalid = || failure("invalid-params", "invalid forwarded call");
        let params = open.params.ok_or_else(invalid)?;
        let call: Forwarded = serde_json::from_str(params.get()).map_err(|_| invalid())?;
        let path = keyed_path(&call.method)?;
        if call.principal.is_empty() {
            return Err(invalid());
        }
        let public_metadata =
            path.groups.is_empty() && matches!(path.segments.last(), Some(&"_ir" | &"_hash"));
        admit(&path.name, allowed.contains(&path.name), public_metadata)?;
        let caller = Caller {
            principal: call.principal.into(),
            via: Some(peer.principal.clone()),
        };
        Ok((
            Open {
                method: call.method,
                params: call.params,
                input: open.input,
            },
            Some(caller),
        ))
    }
}
