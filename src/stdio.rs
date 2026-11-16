//! Frames as lines: a process's stdin and stdout, or a child's.
//!
//! A frame is one line of JSON. JSON keeps a newline only as whitespace
//! between tokens, never inside a string, so a newline in a frame is
//! written as a space and the line stays one frame.

use std::io;
use std::process::Stdio;

use futures::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::process::{Child, Command};
use tokio_util::bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use crate::Channel;

/// A channel over anything read and written as lines.
pub fn lines(read: impl AsyncRead + Send + 'static, write: impl AsyncWrite + Send + 'static) -> Channel {
    let receive = FramedRead::new(Box::pin(read), Lines::default()).map(|line| line.map(|l: BytesMut| l.to_vec()));
    Channel::new(FramedWrite::new(Box::pin(write), Lines::default()), receive)
}

/// This process's stdin and stdout.
pub fn stdio() -> Channel {
    lines(tokio::io::stdin(), tokio::io::stdout())
}

/// Start `command` with its stdin and stdout as a channel to it.
pub fn child(command: &mut Command) -> io::Result<(Child, Channel)> {
    let mut child = command.stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;
    let (stdin, stdout) = (child.stdin.take(), child.stdout.take());
    Ok((child, lines(stdout.expect("piped"), stdin.expect("piped"))))
}

/// One frame a line. `scanned` is how far the bytes waiting were already
/// searched for a newline, so a long line arriving in pieces is read once.
#[derive(Default)]
struct Lines {
    scanned: usize,
}

impl Decoder for Lines {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<BytesMut>> {
        loop {
            let Some(at) = src[self.scanned..].iter().position(|&b| b == b'\n').map(|i| i + self.scanned) else {
                self.scanned = src.len();
                return Ok(None);
            };
            self.scanned = 0;
            let mut line = src.split_to(at + 1);
            line.truncate(at);
            if line.last() == Some(&b'\r') {
                line.truncate(at - 1);
            }
            if !line.is_empty() {
                return Ok(Some(line));
            }
        }
    }

    /// A last line with no newline after it is a frame too.
    fn decode_eof(&mut self, src: &mut BytesMut) -> io::Result<Option<BytesMut>> {
        match self.decode(src)? {
            None if !src.is_empty() => {
                self.scanned = 0;
                Ok(Some(src.split()))
            }
            line => Ok(line),
        }
    }
}

impl Encoder<Vec<u8>> for Lines {
    type Error = io::Error;

    fn encode(&mut self, frame: Vec<u8>, dst: &mut BytesMut) -> io::Result<()> {
        dst.reserve(frame.len() + 1);
        let start = dst.len();
        dst.put_slice(&frame);
        for b in &mut dst[start..] {
            if *b == b'\n' || *b == b'\r' {
                *b = b' ';
            }
        }
        dst.put_u8(b'\n');
        Ok(())
    }
}
