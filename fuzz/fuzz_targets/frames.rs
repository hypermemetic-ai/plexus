//! Frames well formed and not, in any chunks, to `ws.rs`'s reader, which
//! must read exactly what RFC 6455 says they hold: every message whole and
//! in order, every ping answered, and the close or refusal it ends with —
//! no byte of one frame read into another. A model here says what that is.
#![no_main]

#[path = "wire.rs"]
mod wire;

use arbitrary::Arbitrary;

const MOST_FRAME: usize = 16 << 20;
const MOST_MESSAGE: usize = 64 << 20;

#[derive(Arbitrary, Debug)]
struct Case {
    server: bool,
    chunks: Vec<u8>,
    frames: Vec<Frame>,
}

#[derive(Arbitrary, Debug)]
struct Frame {
    fin: bool,
    kind: u8,
    rsv: u8,
    mask: [u8; 4],
    wrong_mask: u8,
    long_form: u8,
    payload: Payload,
    repeat: u16,
}

#[derive(Arbitrary, Debug)]
enum Payload {
    Text(String),
    Bytes(Vec<u8>),
    Close(u16, String),
}

impl Frame {
    fn opcode(&self) -> u8 {
        match self.kind % 20 {
            0..=3 => 0,
            4..=8 => 1,
            9..=11 => 2,
            12 => 8,
            13 | 14 => 9,
            15 => 10,
            _ => self.kind >> 4,
        }
    }

    fn rsv(&self) -> u8 {
        if self.rsv > 240 { self.rsv & 7 } else { 0 }
    }

    fn payload(&self) -> Vec<u8> {
        let one = match &self.payload {
            Payload::Text(s) => s.as_bytes().to_vec(),
            Payload::Bytes(b) => b.clone(),
            Payload::Close(code, reason) => [&code.to_be_bytes()[..], reason.as_bytes()].concat(),
        };
        // Now and then a long one, up to just past a frame's limit.
        let mut payload = if self.repeat >= 0xff00 { one.repeat(((self.repeat & 0xff) as usize + 1) << 16) } else { one };
        payload.truncate(MOST_FRAME + 16);
        payload
    }

    fn masked(&self, server: bool) -> bool {
        server != (self.wrong_mask > 250)
    }

    fn encode(&self, payload: &[u8], server: bool, out: &mut Vec<u8>) {
        out.push(u8::from(self.fin) << 7 | self.rsv() << 4 | self.opcode());
        let m = if self.masked(server) { 0x80 } else { 0 };
        let len = payload.len();
        if len < 126 && self.long_form < 250 {
            out.push(m | len as u8);
        } else if len < 0x10000 && self.long_form < 253 {
            out.push(m | 126);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            out.push(m | 127);
            out.extend_from_slice(&(len as u64).to_be_bytes());
        }
        if self.masked(server) {
            out.extend_from_slice(&self.mask);
            out.extend(payload.iter().zip(self.mask.iter().cycle()).map(|(b, m)| b ^ m));
        } else {
            out.extend_from_slice(payload);
        }
    }
}

#[derive(Debug, PartialEq)]
enum End {
    /// A close, answered with these bytes.
    Closed(Vec<u8>),
    Refused(u16),
    /// The input ran out first.
    Cut,
}

/// UTF-8 so far, or a message that may still become it.
fn utf8(text: &[u8], last: bool) -> bool {
    match std::str::from_utf8(text) {
        Ok(_) => true,
        Err(e) => e.error_len().is_none() && !last,
    }
}

/// What the reader must hand on and send, from the frames it is sent.
fn model(frames: &[(&Frame, Vec<u8>)], server: bool) -> (Vec<Vec<u8>>, Vec<Vec<u8>>, End) {
    let (mut messages, mut pongs, mut fragments) = (Vec::new(), Vec::new(), None::<(bool, Vec<u8>)>);
    let end = 'end: {
        for (f, payload) in frames {
            let (op, fin, len) = (f.opcode(), f.fin, payload.len());
            if matches!(op, 3..=7 | 11..=15) || f.rsv() != 0 || f.masked(server) != server || (op >= 8 && (!fin || len > 125)) {
                break 'end End::Refused(1002);
            }
            if len > MOST_FRAME {
                break 'end End::Refused(1009);
            }
            match (op, &mut fragments) {
                (8, _) => {
                    if len == 1 {
                        break 'end End::Refused(1002);
                    }
                    if len >= 2 {
                        if !matches!(u16::from_be_bytes([payload[0], payload[1]]), 1000..=1003 | 1007..=1014 | 3000..=4999) {
                            break 'end End::Refused(1002);
                        }
                        if std::str::from_utf8(&payload[2..]).is_err() {
                            break 'end End::Refused(1007);
                        }
                    }
                    break 'end End::Closed(payload[..len.min(2)].to_vec());
                }
                (9, _) => pongs.push(payload.clone()),
                (10, _) => {}
                (1 | 2, None) => {
                    if op == 1 && !utf8(payload, fin) {
                        break 'end End::Refused(1007);
                    }
                    if fin {
                        messages.push(payload.clone());
                    } else {
                        fragments = Some((op == 1, payload.clone()));
                    }
                }
                (0, Some((text, so_far))) => {
                    if so_far.len() + len > MOST_MESSAGE {
                        break 'end End::Refused(1009);
                    }
                    so_far.extend_from_slice(payload);
                    if *text && !utf8(so_far, fin) {
                        break 'end End::Refused(1007);
                    }
                    if fin {
                        messages.push(fragments.take().unwrap().1);
                    }
                }
                _ => break 'end End::Refused(1002),
            }
        }
        End::Cut
    };
    (messages, pongs, end)
}

/// The frames one end wrote: opcode and payload, each unmasked.
fn written(mut bytes: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut frames = Vec::new();
    while !bytes.is_empty() {
        let (first, second) = (bytes[0], bytes[1]);
        assert_eq!(first & 0xf0, 0x80, "final, no reserved bit");
        let (len, mut at) = match second & 0x7f {
            126 => (u16::from_be_bytes([bytes[2], bytes[3]]) as usize, 4),
            127 => (u64::from_be_bytes(bytes[2..10].try_into().unwrap()) as usize, 10),
            n => (n as usize, 2),
        };
        let mask = if second & 0x80 != 0 {
            at += 4;
            bytes[at - 4..at].to_vec()
        } else {
            vec![0]
        };
        let payload = bytes[at..at + len].iter().zip(mask.iter().cycle()).map(|(b, m)| b ^ m).collect();
        frames.push((first & 0x0f, payload));
        bytes = &bytes[at + len..];
    }
    frames
}

libfuzzer_sys::fuzz_target!(|case: Case| {
    let (mut input, mut frames) = (Vec::new(), Vec::new());
    for f in &case.frames {
        let payload = f.payload();
        if input.len() + payload.len() > 80 << 20 {
            break;
        }
        f.encode(&payload, case.server, &mut input);
        frames.push((f, payload));
    }
    let (messages, pongs, end) = model(&frames, case.server);
    drop(frames);
    let mut expected = messages.into_iter();
    let (failed, out) = wire::read(wire::Wire::new(input, case.chunks), case.server, |message| {
        assert!(expected.next().is_some_and(|m| m == message[..]), "a message not as sent");
    });
    assert!(expected.next().is_none(), "a message not handed on");
    let kind = failed.map(|e| e.kind());
    let mut sent = written(&out).into_iter();
    for pong in pongs {
        assert_eq!(sent.next(), Some((10, pong)), "a ping not answered");
    }
    match end {
        End::Closed(reply) => {
            assert_eq!(kind, None);
            assert_eq!(sent.next(), Some((8, reply)));
        }
        End::Refused(code) => {
            assert_eq!(kind, Some(std::io::ErrorKind::InvalidData));
            assert_eq!(sent.next(), Some((8, code.to_be_bytes().to_vec())));
        }
        End::Cut => assert_eq!(kind, Some(std::io::ErrorKind::BrokenPipe)),
    }
    assert_eq!(sent.next(), None, "nothing after the close");
});
