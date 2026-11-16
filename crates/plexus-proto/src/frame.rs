//! The frame: one message of one call.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// JSON kept as the text it arrived as. A frame's payload is never parsed
/// by the layers that only carry it.
pub type Raw = Box<RawValue>;

/// Which call of a connection a frame belongs to. The caller picks it at
/// `open`; both ends use it until the call ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CallId(pub u32);

impl CallId {
    /// Frames about the connection itself (`hello`) travel on call 0.
    pub const CONNECTION: CallId = CallId(0);
}

/// One message of one call.
#[derive(Debug, Clone)]
pub struct Frame {
    pub call: CallId,
    /// Where this frame sits in its call, per direction, from 1. Always
    /// known in memory. Written on the wire only where the channel can
    /// lose, repeat or reorder frames ([`Numbering::Numbered`]); on an
    /// ordered channel both ends count instead ([`Counter`]) and it is 0
    /// until counted.
    ///
    /// [`Numbering::Numbered`]: crate::Numbering::Numbered
    pub seq: u64,
    pub body: Body,
}

/// What a frame says.
#[derive(Debug, Clone)]
pub enum Body {
    /// Once a connection, on call 0: who is speaking. Sent again if the
    /// interface changes while the connection is open.
    Hello(Hello),
    /// The caller starts a call.
    Open(Open),
    /// One element of a call's stream, in either direction.
    Item(Raw),
    /// Something to show while waiting; never part of the answer.
    Progress(Raw),
    /// This side has no more to say. `Err` is why the call failed.
    End(Result<(), Failure>),
    /// The caller no longer wants the answer.
    Cancel,
    /// The receiver will take this many more items.
    Credit(u32),
    /// After a reconnect: "I have every frame up to this `seq`".
    Resume(u64),
}

/// Who is speaking, said once a connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// The hash of the interface this side serves. A caller that has seen
    /// it before already knows every method and type behind it.
    pub interface: String,
    /// True when this side writes `seq` on every frame.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub numbered: bool,
}

/// The start of a call: a method by its dotted path, and its parameters.
#[derive(Debug, Clone)]
pub struct Open {
    pub method: String,
    /// `None` is "no parameters", and is not written.
    pub params: Option<Raw>,
}

/// Why a call failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failure {
    /// A stable word a program matches on: `forbidden`, `not_found`.
    pub code: String,
    /// A sentence for a person.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// Whatever else the failure carries, typed by the interface.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Raw>,
}

impl Frame {
    /// An item whose payload is already JSON text. Fails if it is not JSON.
    pub fn item(call: CallId, json: &str) -> Result<Frame, serde_json::Error> {
        Ok(Frame { call, seq: 0, body: Body::Item(RawValue::from_string(json.to_owned())?) })
    }

    /// An item from any serializable value.
    pub fn item_of<T: Serialize>(call: CallId, value: &T) -> Result<Frame, serde_json::Error> {
        Ok(Frame { call, seq: 0, body: Body::Item(serde_json::value::to_raw_value(value)?) })
    }

    /// The end of a call that went well.
    pub fn end(call: CallId) -> Frame {
        Frame { call, seq: 0, body: Body::End(Ok(())) }
    }

    /// The end of a call that failed.
    pub fn failed(call: CallId, code: &str, message: &str) -> Frame {
        let failure = Failure { code: code.to_owned(), message: message.to_owned(), data: None };
        Frame { call, seq: 0, body: Body::End(Err(failure)) }
    }

    /// The start of a call.
    pub fn open(call: CallId, method: &str, params: Option<Raw>) -> Frame {
        Frame { call, seq: 0, body: Body::Open(Open { method: method.to_owned(), params }) }
    }

    /// The first frame of a connection.
    pub fn hello(interface: &str, numbered: bool) -> Frame {
        let hello = Hello { interface: interface.to_owned(), numbered };
        Frame { call: CallId::CONNECTION, seq: 0, body: Body::Hello(hello) }
    }
}

/// Numbers the frames of an ordered channel, one direction of it.
///
/// Both ends run one over what they send and one over what they receive,
/// and arrive at the same `seq` for every frame without a byte being spent
/// on it.
#[derive(Debug, Default)]
pub struct Counter {
    next: HashMap<CallId, u64>,
}

impl Counter {
    pub fn new() -> Counter {
        Counter::default()
    }

    /// Give `frame` its place in its call. An `open` is 0; what follows
    /// counts from 1. The call is forgotten at its `end`.
    pub fn stamp(&mut self, frame: &mut Frame) {
        match frame.body {
            Body::Hello(_) => frame.seq = 0,
            Body::Open(_) => {
                frame.seq = 0;
                self.next.insert(frame.call, 1);
            }
            Body::End(_) => {
                frame.seq = self.next.remove(&frame.call).unwrap_or(1);
            }
            _ => {
                let next = self.next.entry(frame.call).or_insert(1);
                frame.seq = *next;
                *next += 1;
            }
        }
    }

    /// How many calls are still open in this direction.
    pub fn open_calls(&self) -> usize {
        self.next.len()
    }
}
