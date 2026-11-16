//! What travels between Plexus services, and nothing that runs.
//!
//! A [`Frame`] is one message of one call. Its bytes are as few as the
//! channel allows: what never changes during a call is not sent with every
//! item.
//!
//! ```
//! use plexus_proto::{Body, CallId, Frame, Numbering};
//!
//! let item = Frame::item(CallId(7), r#"{"n":1}"#).unwrap();
//! assert_eq!(item.encode(Numbering::Counted), br#"[7,{"n":1}]"#);
//!
//! let back = Frame::decode(br#"[7,{"n":1}]"#).unwrap();
//! assert!(matches!(back.body, Body::Item(_)));
//! ```

mod frame;
mod wire;

pub use frame::{Body, CallId, Counter, Failure, Frame, Hello, Open, Raw};
pub use wire::{DecodeError, Kind, Numbering};
