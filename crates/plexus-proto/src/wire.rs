//! The frame's bytes.
//!
//! A frame is a JSON array, read by its length:
//!
//! | bytes | meaning |
//! | -- | -- |
//! | `[7]` | call 7 ended well |
//! | `[7,BODY]` | an item of call 7 |
//! | `[7,KIND,BODY]` | any other frame; `KIND` is one digit |
//! | `[7,KIND,BODY,SEQ]` | the same on a numbered channel |
//!
//! Items are nearly all the traffic, so the item is the short form.

use std::fmt;

use serde::Deserialize;
use serde::de::{SeqAccess, Visitor};
use serde_json::value::RawValue;

use crate::frame::{Body, CallId, Failure, Frame, Hello, Open};

/// The digit that names a frame's kind on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Hello = 0,
    Open = 1,
    Item = 2,
    Progress = 3,
    End = 4,
    Cancel = 5,
    Credit = 6,
    Resume = 7,
}

impl Kind {
    fn from_digit(d: u64) -> Option<Kind> {
        Some(match d {
            0 => Kind::Hello,
            1 => Kind::Open,
            2 => Kind::Item,
            3 => Kind::Progress,
            4 => Kind::End,
            5 => Kind::Cancel,
            6 => Kind::Credit,
            7 => Kind::Resume,
            _ => return None,
        })
    }
}

impl Body {
    /// The kind this body is written as.
    pub fn kind(&self) -> Kind {
        match self {
            Body::Hello(_) => Kind::Hello,
            Body::Open(_) => Kind::Open,
            Body::Item(_) => Kind::Item,
            Body::Progress(_) => Kind::Progress,
            Body::End(_) => Kind::End,
            Body::Cancel => Kind::Cancel,
            Body::Credit(_) => Kind::Credit,
            Body::Resume(_) => Kind::Resume,
        }
    }
}

/// Whether a channel writes each frame's `seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Numbering {
    /// The channel is ordered and loses nothing (a WebSocket, a pipe, a
    /// function call). `seq` is counted by both ends and never written.
    Counted,
    /// The channel may lose, repeat or reorder (a queue, datagrams, a relay
    /// that merges). Every frame writes its `seq`.
    Numbered,
}

/// Why bytes were not a frame.
#[derive(Debug)]
pub enum DecodeError {
    /// Not JSON, or not an array of one to four elements.
    Shape(serde_json::Error),
    /// The first element is not a call number.
    Call,
    /// The kind is not a digit this version knows.
    Kind(u64),
    /// The body does not fit its kind.
    Body(Kind, serde_json::Error),
    /// The fourth element is not a number.
    Seq,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Shape(e) => write!(f, "not a frame: {e}"),
            DecodeError::Call => write!(f, "a frame's first element is its call number"),
            DecodeError::Kind(k) => write!(f, "unknown kind {k}"),
            DecodeError::Body(k, e) => write!(f, "the body of a {k:?} frame: {e}"),
            DecodeError::Seq => write!(f, "a frame's fourth element is its seq"),
        }
    }
}

impl std::error::Error for DecodeError {}

fn push_num(out: &mut Vec<u8>, mut n: u64) {
    let mut buf = [0u8; 20];
    let mut at = buf.len();
    loop {
        at -= 1;
        buf[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[at..]);
}

impl Frame {
    /// This frame's bytes.
    pub fn encode(&self, numbering: Numbering) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len_hint());
        self.encode_into(numbering, &mut out);
        out
    }

    fn encoded_len_hint(&self) -> usize {
        24 + match &self.body {
            Body::Item(raw) | Body::Progress(raw) => raw.get().len(),
            _ => 40,
        }
    }

    /// This frame's bytes, appended to `out`: a writer that sends many
    /// frames keeps one buffer.
    pub fn encode_into(&self, numbering: Numbering, out: &mut Vec<u8>) {
        out.push(b'[');
        push_num(out, self.call.0 as u64);
        let numbered = numbering == Numbering::Numbered;
        match &self.body {
            // The two short forms. A numbered channel never uses them.
            Body::End(Ok(())) if !numbered => {}
            Body::Item(raw) if !numbered => {
                out.push(b',');
                out.extend_from_slice(raw.get().as_bytes());
            }
            body => {
                out.push(b',');
                out.push(b'0' + body.kind() as u8);
                out.push(b',');
                match body {
                    Body::Hello(hello) => serde_json::to_writer(&mut *out, hello).expect("a Hello is JSON"),
                    Body::Open(open) => {
                        out.push(b'[');
                        serde_json::to_writer(&mut *out, &open.method).expect("a string is JSON");
                        if let Some(params) = &open.params {
                            out.push(b',');
                            out.extend_from_slice(params.get().as_bytes());
                        }
                        out.push(b']');
                    }
                    Body::Item(raw) | Body::Progress(raw) => out.extend_from_slice(raw.get().as_bytes()),
                    Body::End(Ok(())) | Body::Cancel => out.extend_from_slice(b"null"),
                    Body::End(Err(failure)) => serde_json::to_writer(&mut *out, failure).expect("a Failure is JSON"),
                    Body::Credit(n) => push_num(out, *n as u64),
                    Body::Resume(seq) => push_num(out, *seq),
                }
                if numbered {
                    out.push(b',');
                    push_num(out, self.seq);
                }
            }
        }
        out.push(b']');
    }

    /// Read one frame. On a counted channel its `seq` is 0 until a
    /// [`Counter`](crate::Counter) stamps it.
    pub fn decode(bytes: &[u8]) -> Result<Frame, DecodeError> {
        let parts: Parts<'_> = serde_json::from_slice(bytes).map_err(DecodeError::Shape)?;
        let call = parts.at[0].and_then(|r| r.get().parse::<u32>().ok()).map(CallId).ok_or(DecodeError::Call)?;
        let (kind, body) = match parts.len {
            1 => return Ok(Frame { call, seq: 0, body: Body::End(Ok(())) }),
            2 => (Kind::Item, parts.at[1].expect("len 2")),
            _ => {
                let digit = parts.at[1].and_then(|r| r.get().parse::<u64>().ok()).ok_or(DecodeError::Kind(u64::MAX))?;
                (Kind::from_digit(digit).ok_or(DecodeError::Kind(digit))?, parts.at[2].expect("len 3"))
            }
        };
        let seq = match parts.at[3] {
            Some(raw) => raw.get().parse::<u64>().map_err(|_| DecodeError::Seq)?,
            None => 0,
        };
        let bad = |e| DecodeError::Body(kind, e);
        let text = body.get();
        let body = match kind {
            Kind::Hello => Body::Hello(serde_json::from_str::<Hello>(text).map_err(bad)?),
            Kind::Open => {
                let open: Parts<'_> = serde_json::from_str(text).map_err(bad)?;
                let method = serde_json::from_str::<String>(open.at[0].expect("len 1").get()).map_err(bad)?;
                if open.len > 2 {
                    return Err(bad(serde::de::Error::custom("an open is [method] or [method, params]")));
                }
                Body::Open(Open { method, params: open.at[1].map(|p| p.to_owned()) })
            }
            Kind::Item => Body::Item(body.to_owned()),
            Kind::Progress => Body::Progress(body.to_owned()),
            Kind::End if text == "null" => Body::End(Ok(())),
            Kind::End => Body::End(Err(serde_json::from_str::<Failure>(text).map_err(bad)?)),
            Kind::Cancel => Body::Cancel,
            Kind::Credit => Body::Credit(serde_json::from_str(text).map_err(bad)?),
            Kind::Resume => Body::Resume(serde_json::from_str(text).map_err(bad)?),
        };
        Ok(Frame { call, seq, body })
    }
}

/// A frame's one to four elements, each still the text it arrived as. No
/// allocation: the slices point into the bytes being read.
struct Parts<'a> {
    len: usize,
    at: [Option<&'a RawValue>; 4],
}

impl<'de> Deserialize<'de> for Parts<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Parts<'de>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an array of one to four elements")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Parts<'de>, A::Error> {
                let mut parts = Parts { len: 0, at: [None; 4] };
                while let Some(raw) = seq.next_element::<&'de RawValue>()? {
                    if parts.len == 4 {
                        return Err(serde::de::Error::invalid_length(5, &self));
                    }
                    parts.at[parts.len] = Some(raw);
                    parts.len += 1;
                }
                if parts.len == 0 {
                    return Err(serde::de::Error::invalid_length(0, &self));
                }
                Ok(parts)
            }
        }
        d.deserialize_seq(V)
    }
}
