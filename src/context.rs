//! Caller identities and the path keys used to open an activation.

use std::collections::HashMap;
use std::str::FromStr;

use crate::server::failure;
use crate::{Caller, Failure};

/// Resolve a transport-proved caller against the activation's own state.
/// An implementation can reject identities the activation does not admit.
pub trait FromCaller<A>: Sized {
    fn from_caller(activation: &A, caller: &Caller) -> Result<Self, Failure>;
}

impl<A> FromCaller<A> for Caller {
    fn from_caller(_: &A, caller: &Caller) -> Result<Self, Failure> {
        Ok(caller.clone())
    }
}

/// An activation's context, filled on its own thread from the connection's
/// caller and the named keys on the activation's own path segment.
pub trait Context<A>: Sized {
    const KEYS: &'static [&'static str];
    fn from_context(activation: &A, caller: Option<&Caller>, keys: &ContextKeys) -> Result<Self, Failure>;
}

/// The named keys supplied on an activation's path segment.
#[derive(Default)]
pub struct ContextKeys(HashMap<String, String>);

impl ContextKeys {
    /// Read a declared key using its field type's `FromStr` implementation.
    pub fn parse<T: FromStr>(&self, key: &str) -> Result<T, Failure> {
        self.0.get(key).ok_or_else(|| missing(key))?.parse().map_err(|_| invalid(&format!("invalid context key `{key}`")))
    }

    pub(crate) fn check(&self, expected: &[&str]) -> Result<(), Failure> {
        if let Some(key) = self.0.keys().find(|key| !expected.contains(&key.as_str())) {
            return Err(invalid(&format!("unexpected context key `{key}`")));
        }
        if let Some(key) = expected.iter().find(|key| !self.0.contains_key(**key)) {
            return Err(missing(key));
        }
        Ok(())
    }
}

/// Connection identity and validated path keys for one local invocation.
#[doc(hidden)]
pub struct CallContext {
    pub(crate) input: Option<crate::RawInput>,
    caller: Option<Caller>,
    keys: ContextKeys,
}

impl CallContext {
    pub(crate) fn new(caller: Option<Caller>, keys: ContextKeys) -> Self {
        Self { caller, keys, input: None }
    }

    /// Take the streamed input of this call. Polling it grants the first
    /// credit, so a refused call never starts pulling its caller's source.
    pub fn take_input<T: 'static>(&mut self, decode: impl Fn(crate::Raw) -> Result<T, Failure> + Send + 'static) -> Result<crate::Input<T>, Failure> {
        self.input.take().map(|input| crate::Input::receive(input, decode))
            .ok_or_else(|| failure("invalid-params", "this method requires an input stream"))
    }

    pub fn caller(&self) -> Option<&Caller> {
        self.caller.as_ref()
    }

    pub fn require_caller(&self) -> Result<&Caller, Failure> {
        require_caller(self.caller())
    }

    pub fn keys(&self) -> &ContextKeys {
        &self.keys
    }
}

/// Require a transport-proved caller before running a caller-aware method.
pub fn require_caller(caller: Option<&Caller>) -> Result<&Caller, Failure> {
    caller.ok_or_else(|| failure("unauthenticated", "this method requires a signed-in caller"))
}

fn missing(key: &str) -> Failure {
    invalid(&format!("missing context key `{key}`"))
}

fn invalid(message: &str) -> Failure {
    failure("invalid-context", message)
}

pub(crate) struct KeyedPath<'a> {
    pub(crate) name: String,
    pub(crate) groups: HashMap<usize, ContextKeys>,
    pub(crate) segments: Vec<&'a str>,
}

/// Split the segments and their key groups. Periods inside brackets belong
/// to the values. Keep the original segments for forwarding to a child.
pub(crate) fn keyed_path(mut rest: &str) -> Result<KeyedPath<'_>, Failure> {
    let mut path = KeyedPath { name: String::new(), groups: HashMap::new(), segments: Vec::new() };
    loop {
        let original = rest;
        let index = path.segments.len();
        let end = rest.find(['.', '[']).unwrap_or(rest.len());
        let segment = &rest[..end];
        if !valid_name(segment) {
            return Err(invalid("invalid name in keyed method path"));
        }
        if index > 0 {
            path.name.push('.');
        }
        path.name.push_str(segment);
        rest = &rest[end..];
        if let Some(keys) = rest.strip_prefix('[') {
            let (keys, after) = keys.split_once(']').ok_or_else(|| invalid("missing `]` in keyed method path"))?;
            let mut values = HashMap::new();
            for key in keys.split(',') {
                let (key, value) = key.split_once('=').ok_or_else(|| invalid("context keys must be written as `name=value`"))?;
                if !valid_name(key) || value.is_empty() || value.bytes().any(|b| b.is_ascii_control() || b"[],=".contains(&b)) {
                    return Err(invalid("invalid name or value in context key"));
                }
                if values.insert(key.to_owned(), value.to_owned()).is_some() {
                    return Err(invalid(&format!("duplicate context key `{key}`")));
                }
            }
            path.groups.insert(index, ContextKeys(values));
            rest = after;
        }
        path.segments.push(&original[..original.len() - rest.len()]);
        if rest.is_empty() {
            return Ok(path);
        }
        rest = rest.strip_prefix('.').ok_or_else(|| invalid("expected `.` after context keys"))?;
    }
}

pub(crate) fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}
