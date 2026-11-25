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

/// The interface an activation requests from its application.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerDependency {
    pub namespace: &'static str,
    pub hash: &'static str,
}

impl PeerDependency {
    pub const fn of<T: PeerInterface>() -> Self {
        Self {
            namespace: T::NAMESPACE,
            hash: T::HASH,
        }
    }
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
    pub(crate) interface: PeerDependency,
    pub(crate) provider: bool,
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

    /// Selection depends only on application configuration, including while a peer is offline.
    pub(crate) fn provider(
        &self,
        interface: PeerDependency,
    ) -> Result<Option<(&str, &Slot)>, Failure> {
        if let Some(slot) = self.slots.get(interface.namespace)
            && slot.provider
            && slot.interface == interface
        {
            return Ok(Some((interface.namespace, slot)));
        }
        let mut matching = self
            .slots
            .iter()
            .filter(|(_, slot)| slot.interface == interface)
            .map(|(name, slot)| (name.as_str(), slot));
        let single = matching.next();
        if matching.next().is_some() {
            return Err(fail(
                "ambiguous-peer",
                "select a provider with PeerHost::inject_peer",
                interface.namespace,
            ));
        }
        Ok(single)
    }

    pub(crate) fn resolve<T: PeerInterface>(&self, name: Option<&str>) -> Result<Peer<T>, Failure> {
        let interface = PeerDependency::of::<T>();
        let (name, binding) = match name {
            Some(name) => (
                name,
                self.slots
                    .get(name)
                    .filter(|slot| slot.interface == interface)
                    .ok_or_else(|| unavailable(name))?,
            ),
            None => self
                .provider(interface)?
                .ok_or_else(|| unavailable(interface.namespace))?,
        };
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
