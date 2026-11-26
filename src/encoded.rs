//! Immutable, typed JSON replies for generated native activation methods.
use std::marker::PhantomData;

use crate::{Failure, Raw};

/// A snapshot of one native method result. A generated method returning
/// `Encoded<T>`, or streaming its items, has the same interface as `T`.
/// Create the snapshot from the authorized value and clone it for callers
/// entitled to that exact value. Each clone shares its immutable JSON bytes.
///
/// Direct byte results use the Layers base64 codec and are served as bytes.
#[derive(Debug)]
pub struct Encoded<T> {
    body: Raw,
    digest: Option<u128>,
    marker: PhantomData<fn(T) -> T>,
}

impl<T: serde::Serialize> Encoded<T> {
    /// Serialize the value once using the same JSON encoder as a native reply.
    pub fn new(value: &T) -> Result<Self, Failure> {
        Ok(Self {
            body: crate::raw(value)?.into_shared(),
            digest: None,
            marker: PhantomData,
        })
    }
}

impl<T> Clone for Encoded<T> {
    fn clone(&self) -> Self {
        Self {
            body: self.body.clone(),
            digest: self.digest,
            marker: PhantomData,
        }
    }
}

impl<T> Encoded<T> {
    /// Use the digest of the authorized visible value for watch deduplication.
    /// Supply the same digest only for semantically interchangeable results.
    pub fn with_watch_digest(mut self, digest: u128) -> Self {
        self.digest = Some(digest);
        self
    }

    #[doc(hidden)]
    pub fn into_watch_value(self) -> (Raw, u128) {
        let digest = self.digest.unwrap_or_else(|| crate::watch_digest(&self.body));
        (self.body, digest)
    }

    /// Borrow the immutable JSON for inspection.
    pub fn as_raw(&self) -> &Raw {
        &self.body
    }

    /// Hand the snapshot to the native transport.
    pub fn into_raw(self) -> Raw {
        self.body
    }
}
