//! Typed peer clients and activation-local binding resolution.
use std::{
    collections::BTreeMap,
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::{Connection, Failure, fail, lock};

/// Implemented by the activation macro. The client contains an authenticated connection.
pub trait PeerInterface: 'static {
    const NAMESPACE: &'static str;
    const HASH: &'static str;
    type Client;
    fn client(connection: Connection) -> Self::Client;
}

/// A live, interface-checked dependency injected when a call opens.
pub struct Peer<T: PeerInterface>(T::Client);

impl<T: PeerInterface> Deref for Peer<T> {
    type Target = T::Client;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Default)]
pub(crate) struct Bindings {
    pub(crate) root: String,
    pub(crate) slots: BTreeMap<String, Slot>,
    pub(crate) required: Vec<String>,
}

pub(crate) struct Slot {
    pub(crate) namespace: &'static str,
    pub(crate) hash: &'static str,
    pub(crate) live: Arc<Mutex<Option<Connection>>>,
}

impl Bindings {
    pub(crate) fn check(&self) -> Result<(), Failure> {
        for name in &self.required {
            if lock(&self.slots[name].live).is_none() {
                return Err(unavailable(name));
            }
        }
        Ok(())
    }

    pub(crate) fn resolve<T: PeerInterface>(&self, name: Option<&str>) -> Result<Peer<T>, Failure> {
        let mut matches = self.slots.iter().filter(|(slot, binding)| {
            name.is_none_or(|n| n == slot.as_str())
                && binding.namespace == T::NAMESPACE
                && binding.hash == T::HASH
        });
        let (name, binding) = matches
            .next()
            .ok_or_else(|| unavailable(name.unwrap_or(T::NAMESPACE)))?;
        if matches.next().is_some() {
            return Err(fail(
                "ambiguous-peer",
                "select a named peer binding",
                T::NAMESPACE,
            ));
        }
        let connection = lock(&binding.live)
            .clone()
            .ok_or_else(|| unavailable(name))?;
        Ok(Peer(T::client(connection)))
    }
}

fn unavailable(slot: &str) -> Failure {
    fail("unavailable", "peer binding is unavailable", slot)
}

#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub fn decode<T: serde::de::DeserializeOwned>(raw: crate::Raw) -> Result<T, crate::Error> {
    serde_json::from_str(raw.get())
        .map_err(|e| crate::Error::Failed(fail("invalid-result", &e.to_string(), ())))
}
