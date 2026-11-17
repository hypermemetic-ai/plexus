//! A value as SQLite gives it, borrowed, and the one block of bytes in which
//! a statement's parameters, or a query's rows, cross `plexus:store/sql`.
//! The engine includes this file too, so both sides read and write it alike.
//!
//! A value is a tag byte and what follows it: 0 null; 1 an integer, 8 bytes;
//! 2 a real, 8 bytes; 3 text and 4 a blob, a 4-byte length and the bytes.
//! Numbers are little-endian. Parameters are values one after another; rows
//! are a 4-byte column count, then each row's values in order.

/// A value borrowed from where it lies: a row natively, the block in a
/// component.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueRef<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a [u8]),
    Blob(&'a [u8]),
}

#[allow(dead_code)] // each side uses some of it
impl<'a> ValueRef<'a> {
    pub(crate) fn put(self, out: &mut Vec<u8>) {
        match self {
            ValueRef::Null => out.push(0),
            ValueRef::Integer(i) => {
                out.push(1);
                out.extend_from_slice(&i.to_le_bytes());
            }
            ValueRef::Real(r) => {
                out.push(2);
                out.extend_from_slice(&r.to_le_bytes());
            }
            ValueRef::Text(b) | ValueRef::Blob(b) => {
                out.push(if matches!(self, ValueRef::Text(_)) { 3 } else { 4 });
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
        }
    }

    /// The value at the front of `block`, which moves past it.
    pub(crate) fn take(block: &mut &'a [u8]) -> Result<ValueRef<'a>, &'static str> {
        let (&tag, rest) = block.split_first().ok_or("a value cut short")?;
        let (v, rest) = match tag {
            0 => (ValueRef::Null, rest),
            1 | 2 => {
                let (n, rest) = rest.split_first_chunk::<8>().ok_or("a number cut short")?;
                (if tag == 1 { ValueRef::Integer(i64::from_le_bytes(*n)) } else { ValueRef::Real(f64::from_le_bytes(*n)) }, rest)
            }
            3 | 4 => {
                let (n, rest) = rest.split_first_chunk::<4>().ok_or("a length cut short")?;
                let n = u32::from_le_bytes(*n) as usize;
                if rest.len() < n {
                    return Err("bytes cut short");
                }
                let (b, rest) = rest.split_at(n);
                (if tag == 3 { ValueRef::Text(b) } else { ValueRef::Blob(b) }, rest)
            }
            _ => return Err("an unknown tag"),
        };
        *block = rest;
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::ValueRef;

    #[test]
    fn a_block_reads_back_what_was_written() {
        let values = [ValueRef::Null, ValueRef::Integer(-7), ValueRef::Real(2.5), ValueRef::Text("célad".as_bytes()), ValueRef::Blob(&[0, 255])];
        let mut block = Vec::new();
        values.iter().for_each(|v| v.put(&mut block));
        assert_eq!(block.len(), 1 + 9 + 9 + (5 + 6) + (5 + 2));
        let mut rest = &block[..];
        let read: Vec<_> = std::iter::from_fn(|| (!rest.is_empty()).then(|| ValueRef::take(&mut rest).unwrap())).collect();
        assert_eq!(read, values);
        assert_eq!(ValueRef::take(&mut &block[block.len() - 7..block.len() - 1]), Err("bytes cut short"));
        assert_eq!(ValueRef::take(&mut &[9u8][..]), Err("an unknown tag"));
    }
}
