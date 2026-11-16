//! plexus-proto behind four plain exports, so a page can run it with
//! `WebAssembly.instantiate` and nothing else.
//!
//! The page writes a frame's bytes at `input(len)`, calls `read(len, side,
//! numbered)`, and reads `output_len()` bytes of JSON at `output()`: what
//! the frame is, its `seq` as this end counts it, and its bytes written
//! back both ways.

use std::cell::RefCell;

use plexus_proto::{Body, Counter, Frame, Numbering};
use serde_json::json;

#[derive(Default)]
struct State {
    input: Vec<u8>,
    output: Vec<u8>,
    /// One counter a direction: 0 is caller to service, 1 the way back.
    counters: [Counter; 2],
}

thread_local! {
    static STATE: RefCell<State> = RefCell::new(State::default());
}

/// Room for `len` bytes of input; the address to write them at.
#[unsafe(no_mangle)]
pub extern "C" fn input(len: usize) -> *mut u8 {
    STATE.with_borrow_mut(|s| {
        s.input.resize(len, 0);
        s.input.as_mut_ptr()
    })
}

/// Where the last answer is.
#[unsafe(no_mangle)]
pub extern "C" fn output() -> *const u8 {
    STATE.with_borrow(|s| s.output.as_ptr())
}

/// How long the last answer is.
#[unsafe(no_mangle)]
pub extern "C" fn output_len() -> usize {
    STATE.with_borrow(|s| s.output.len())
}

/// Forget every open call: a new connection.
#[unsafe(no_mangle)]
pub extern "C" fn reset() {
    STATE.with_borrow_mut(|s| s.counters = Default::default());
}

/// Read the frame at `input`, travelling in direction `side` (0 or 1).
#[unsafe(no_mangle)]
pub extern "C" fn read(side: usize, numbered: usize) {
    STATE.with_borrow_mut(|s| {
        let answer = match Frame::decode(&s.input) {
            Err(e) => json!({ "ok": false, "error": e.to_string() }),
            Ok(mut frame) => {
                // The page plays the sender too: a frame that arrives without
                // its seq is given one here, as the sending end would.
                let _ = numbered;
                if frame.seq == 0 {
                    s.counters[side & 1].stamp(&mut frame);
                }
                let says = match &frame.body {
                    Body::Hello(h) => format!("interface {}{}", h.interface, if h.numbered { ", numbered" } else { "" }),
                    Body::Open(o) => o.method.clone(),
                    Body::Item(raw) | Body::Progress(raw) => raw.get().to_owned(),
                    Body::End(Ok(())) => "ok".to_owned(),
                    Body::End(Err(f)) => format!("{}: {}", f.code, f.message),
                    Body::Cancel => String::new(),
                    Body::Credit(n) => format!("{n} more"),
                    Body::Resume(seq) => format!("have up to {seq}"),
                };
                json!({
                    "ok": true,
                    "call": frame.call.0,
                    "seq": frame.seq,
                    "kind": format!("{:?}", frame.body.kind()).to_lowercase(),
                    "says": says,
                    "counted": String::from_utf8_lossy(&frame.encode(Numbering::Counted)),
                    "numbered": String::from_utf8_lossy(&frame.encode(Numbering::Numbered)),
                    "open_calls": s.counters[side & 1].open_calls(),
                })
            }
        };
        s.output = serde_json::to_vec(&answer).expect("JSON");
    })
}
