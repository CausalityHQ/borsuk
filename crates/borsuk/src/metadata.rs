//! Per-record metadata: a schemaless, typed, recursive value model plus a
//! compact self-describing binary codec.
//!
//! Metadata rides on every [`crate::VectorRecord`] and is stored in one binary
//! column on the segment payload (never JSON — the index format stays compact
//! binary). Filtering and per-segment pruning stats build on the same types;
//! those live in later parts of this module.
// TODO(metadata): the codec/filter/stats here are consumed by the storage,
// query, and API stages that follow; drop this allowance once they are wired in.
#![allow(dead_code)]

use std::collections::BTreeMap;

use crate::error::{BorsukError, Result};

/// A single typed metadata value. Recursive: values may be lists or nested maps.
#[derive(Debug, Clone, PartialEq)]
pub enum MetaValue {
    /// Explicit null.
    Null,
    /// Boolean.
    Bool(bool),
    /// Exact 64-bit signed integer.
    Int(i64),
    /// 64-bit float.
    Float(f64),
    /// UTF-8 string.
    Str(String),
    /// UTC timestamp in epoch milliseconds.
    Timestamp(i64),
    /// Ordered list of values.
    List(Vec<MetaValue>),
    /// Nested map, keyed by string.
    Map(Metadata),
}

/// A record's metadata: an ordered map from key to typed value. An empty map
/// means "no metadata". Ordered (`BTreeMap`) so encoding is deterministic.
pub type Metadata = BTreeMap<String, MetaValue>;

// ---- Binary codec -------------------------------------------------------
//
// Layout: a value is `tag byte` + payload. Lengths and list/map counts are
// unsigned LEB128 varints; signed integers use zig-zag + LEB128. Strings are
// `varint(len)` + raw UTF-8 bytes. A `Metadata` map encodes as `varint(count)`
// followed by `(key, value)` pairs.

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_FLOAT: u8 = 3;
const TAG_STR: u8 = 4;
const TAG_TIMESTAMP: u8 = 5;
const TAG_LIST: u8 = 6;
const TAG_MAP: u8 = 7;

/// Encode a metadata map to compact bytes. An empty map encodes to a single
/// zero byte (count 0).
pub fn encode(meta: &Metadata) -> Vec<u8> {
    let mut out = Vec::new();
    encode_map(meta, &mut out);
    out
}

/// Decode a metadata map produced by [`encode`]. Empty input decodes to an
/// empty map.
pub fn decode(bytes: &[u8]) -> Result<Metadata> {
    if bytes.is_empty() {
        return Ok(Metadata::new());
    }
    let mut cursor = Cursor { bytes, pos: 0 };
    let map = decode_map(&mut cursor)?;
    if cursor.pos != bytes.len() {
        return Err(corrupt("trailing bytes after metadata"));
    }
    Ok(map)
}

fn encode_map(map: &Metadata, out: &mut Vec<u8>) {
    write_uvarint(map.len() as u64, out);
    for (key, value) in map {
        write_str(key, out);
        encode_value(value, out);
    }
}

fn encode_value(value: &MetaValue, out: &mut Vec<u8>) {
    match value {
        MetaValue::Null => out.push(TAG_NULL),
        MetaValue::Bool(b) => {
            out.push(TAG_BOOL);
            out.push(u8::from(*b));
        }
        MetaValue::Int(i) => {
            out.push(TAG_INT);
            write_ivarint(*i, out);
        }
        MetaValue::Float(f) => {
            out.push(TAG_FLOAT);
            out.extend_from_slice(&f.to_le_bytes());
        }
        MetaValue::Str(s) => {
            out.push(TAG_STR);
            write_str(s, out);
        }
        MetaValue::Timestamp(t) => {
            out.push(TAG_TIMESTAMP);
            write_ivarint(*t, out);
        }
        MetaValue::List(items) => {
            out.push(TAG_LIST);
            write_uvarint(items.len() as u64, out);
            for item in items {
                encode_value(item, out);
            }
        }
        MetaValue::Map(map) => {
            out.push(TAG_MAP);
            encode_map(map, out);
        }
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

fn decode_map(cursor: &mut Cursor) -> Result<Metadata> {
    let count = read_uvarint(cursor)?;
    let mut map = Metadata::new();
    for _ in 0..count {
        let key = read_str(cursor)?;
        let value = decode_value(cursor)?;
        map.insert(key, value);
    }
    Ok(map)
}

fn decode_value(cursor: &mut Cursor) -> Result<MetaValue> {
    let tag = read_byte(cursor)?;
    Ok(match tag {
        TAG_NULL => MetaValue::Null,
        TAG_BOOL => MetaValue::Bool(read_byte(cursor)? != 0),
        TAG_INT => MetaValue::Int(read_ivarint(cursor)?),
        TAG_FLOAT => MetaValue::Float(f64::from_le_bytes(read_array::<8>(cursor)?)),
        TAG_STR => MetaValue::Str(read_str(cursor)?),
        TAG_TIMESTAMP => MetaValue::Timestamp(read_ivarint(cursor)?),
        TAG_LIST => {
            let count = read_uvarint(cursor)?;
            let mut items = Vec::with_capacity(count.min(1024) as usize);
            for _ in 0..count {
                items.push(decode_value(cursor)?);
            }
            MetaValue::List(items)
        }
        TAG_MAP => MetaValue::Map(decode_map(cursor)?),
        other => return Err(corrupt(&format!("unknown metadata tag {other}"))),
    })
}

fn write_str(s: &str, out: &mut Vec<u8>) {
    write_uvarint(s.len() as u64, out);
    out.extend_from_slice(s.as_bytes());
}

fn read_str(cursor: &mut Cursor) -> Result<String> {
    let len = read_uvarint(cursor)? as usize;
    let bytes = read_bytes(cursor, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| corrupt("metadata string is not valid UTF-8"))
}

fn write_uvarint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn read_uvarint(cursor: &mut Cursor) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = read_byte(cursor)?;
        if shift >= 64 {
            return Err(corrupt("metadata varint overflow"));
        }
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

fn write_ivarint(value: i64, out: &mut Vec<u8>) {
    // zig-zag so small-magnitude negatives stay short.
    write_uvarint(((value << 1) ^ (value >> 63)) as u64, out);
}

fn read_ivarint(cursor: &mut Cursor) -> Result<i64> {
    let raw = read_uvarint(cursor)?;
    Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
}

fn read_byte(cursor: &mut Cursor) -> Result<u8> {
    let byte = *cursor
        .bytes
        .get(cursor.pos)
        .ok_or_else(|| corrupt("unexpected end of metadata"))?;
    cursor.pos += 1;
    Ok(byte)
}

fn read_bytes<'a>(cursor: &mut Cursor<'a>, len: usize) -> Result<&'a [u8]> {
    let end = cursor
        .pos
        .checked_add(len)
        .filter(|end| *end <= cursor.bytes.len())
        .ok_or_else(|| corrupt("metadata length exceeds buffer"))?;
    let slice = &cursor.bytes[cursor.pos..end];
    cursor.pos = end;
    Ok(slice)
}

fn read_array<const N: usize>(cursor: &mut Cursor) -> Result<[u8; N]> {
    let slice = read_bytes(cursor, N)?;
    let mut array = [0u8; N];
    array.copy_from_slice(slice);
    Ok(array)
}

fn corrupt(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("metadata: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Metadata {
        Metadata::from([
            ("b".to_string(), MetaValue::Bool(true)),
            ("neg".to_string(), MetaValue::Int(-5)),
            ("big".to_string(), MetaValue::Int(i64::MAX)),
            ("small".to_string(), MetaValue::Int(i64::MIN)),
            ("f".to_string(), MetaValue::Float(-1.5)),
            ("s".to_string(), MetaValue::Str("hello".to_string())),
            ("t".to_string(), MetaValue::Timestamp(1_700_000_000_000)),
            (
                "l".to_string(),
                MetaValue::List(vec![
                    MetaValue::Str("a".to_string()),
                    MetaValue::Int(2),
                    MetaValue::Null,
                ]),
            ),
            (
                "nested".to_string(),
                MetaValue::Map(Metadata::from([
                    ("k".to_string(), MetaValue::Bool(false)),
                    (
                        "deep".to_string(),
                        MetaValue::Map(Metadata::from([("x".to_string(), MetaValue::Int(9))])),
                    ),
                ])),
            ),
        ])
    }

    #[test]
    fn roundtrip_all_kinds() {
        let meta = sample();
        assert_eq!(decode(&encode(&meta)).unwrap(), meta);
    }

    #[test]
    fn empty_map_roundtrips() {
        let meta = Metadata::new();
        let bytes = encode(&meta);
        assert_eq!(bytes, vec![0]);
        assert_eq!(decode(&bytes).unwrap(), meta);
        assert_eq!(decode(&[]).unwrap(), meta);
    }

    #[test]
    fn encoding_is_deterministic() {
        let meta = sample();
        assert_eq!(encode(&meta), encode(&meta));
    }

    #[test]
    fn truncated_input_errors() {
        let bytes = encode(&sample());
        assert!(decode(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn trailing_bytes_error() {
        let mut bytes = encode(&sample());
        bytes.push(0);
        assert!(decode(&bytes).is_err());
    }
}
