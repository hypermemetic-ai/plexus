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
    marker: PhantomData<fn(T) -> T>,
}

impl<T: serde::Serialize> Encoded<T> {
    /// Serialize the value once using the same JSON encoder as a native reply.
    pub fn new(value: &T) -> Result<Self, Failure> {
        Ok(Self {
            body: crate::raw(value)?.into_shared(),
            marker: PhantomData,
        })
    }
}

impl<T> Clone for Encoded<T> {
    fn clone(&self) -> Self {
        Self {
            body: self.body.clone(),
            marker: PhantomData,
        }
    }
}

impl<T> Encoded<T> {
    /// Borrow the immutable JSON for inspection.
    pub fn as_raw(&self) -> &Raw {
        &self.body
    }

    /// Hand the snapshot to the native transport.
    pub fn into_raw(self) -> Raw {
        self.body
    }
}
