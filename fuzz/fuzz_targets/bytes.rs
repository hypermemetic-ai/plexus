//! Any bytes, in any chunks, to `ws.rs`'s reader as a server and as a
//! caller: it ends or hands on frames, never panics, never holds more than
//! its limits.
#![no_main]

#[path = "wire.rs"]
mod wire;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let [flags, chunks, rest @ ..] = data else { return };
    let (chunks, input) = rest.split_at((*chunks as usize % 16).min(rest.len()));
    wire::read(wire::Wire::new(input.to_vec(), chunks.to_vec()), flags & 1 == 1, drop);
});
