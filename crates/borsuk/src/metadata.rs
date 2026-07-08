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
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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

// ---- Leaf-path flattening ----------------------------------------------

/// Visit every scalar leaf with its flattened dotted path. Nested maps dot-join
/// their keys; list elements are visited at their parent path (so a `tags` list
/// yields one visit per element at path `tags`). `Null` and empty containers
/// contribute nothing. Used to build per-segment pruning stats.
pub fn for_each_leaf(meta: &Metadata, mut visit: impl FnMut(&str, &MetaValue)) {
    for (key, value) in meta {
        walk_leaf(key, value, &mut visit);
    }
}

fn walk_leaf(path: &str, value: &MetaValue, visit: &mut impl FnMut(&str, &MetaValue)) {
    match value {
        MetaValue::Map(map) => {
            for (key, child) in map {
                walk_leaf(&format!("{path}.{key}"), child, visit);
            }
        }
        MetaValue::List(items) => {
            for item in items {
                walk_leaf(path, item, visit);
            }
        }
        MetaValue::Null => {}
        scalar => visit(path, scalar),
    }
}

/// The value at a dotted path, walking nested maps. Returns `None` if any
/// component is missing or a non-map is traversed.
pub fn value_at_path<'a>(meta: &'a Metadata, path: &str) -> Option<&'a MetaValue> {
    let mut parts = path.split('.');
    let mut current = meta.get(parts.next()?)?;
    for part in parts {
        match current {
            MetaValue::Map(map) => current = map.get(part)?,
            _ => return None,
        }
    }
    Some(current)
}

// ---- Typed comparison ---------------------------------------------------

/// Ordering between two values when the operator's type rule applies: numeric
/// family (`Int`/`Timestamp`/`Float`) compares numerically, `Str` compares
/// lexicographically. Any other/cross-kind pairing (or NaN) yields `None`.
fn compare(a: &MetaValue, b: &MetaValue) -> Option<std::cmp::Ordering> {
    use MetaValue::{Float, Int, Str, Timestamp};
    match (a, b) {
        (Int(x) | Timestamp(x), Int(y) | Timestamp(y)) => Some(x.cmp(y)),
        (Int(x) | Timestamp(x), Float(y)) => (*x as f64).partial_cmp(y),
        (Float(x), Int(y) | Timestamp(y)) => x.partial_cmp(&(*y as f64)),
        (Float(x), Float(y)) => x.partial_cmp(y),
        (Str(x), Str(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Typed equality: numeric/string via [`compare`]; other same-kind values via
/// structural equality; cross-kind is `false`.
fn value_eq(a: &MetaValue, b: &MetaValue) -> bool {
    match compare(a, b) {
        Some(ordering) => ordering == std::cmp::Ordering::Equal,
        None => a == b,
    }
}

// ---- Filter tree --------------------------------------------------------

/// Comparison operator for a leaf predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Equal to the operand.
    Eq,
    /// Not equal to the operand (logical negation of `Eq`).
    Ne,
    /// Greater than the operand (numeric/string order).
    Gt,
    /// Greater than or equal to the operand.
    Gte,
    /// Less than the operand.
    Lt,
    /// Less than or equal to the operand.
    Lte,
    /// The field's scalar value is a member of the operand list.
    In,
    /// The field's scalar value is not a member of the operand list.
    Nin,
    /// The field's list value contains the operand scalar.
    Contains,
}

/// A metadata filter predicate tree. Evaluation is total (never errors).
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// All sub-filters must match.
    And(Vec<Filter>),
    /// At least one sub-filter must match.
    Or(Vec<Filter>),
    /// The sub-filter must not match.
    Not(Box<Filter>),
    /// Compare the value at `path` against `value` under `op`.
    Cmp {
        /// Dotted path into the record's metadata.
        path: String,
        /// Comparison operator.
        op: Op,
        /// Operand to compare against.
        value: MetaValue,
    },
    /// Whether `path` is present (`present = true`) or absent.
    Exists {
        /// Dotted path into the record's metadata.
        path: String,
        /// Whether the path must be present.
        present: bool,
    },
}

impl Filter {
    /// Evaluate the filter against a record's metadata. See the design spec for
    /// the exact missing-path / negation / cross-type rules; in short: positive
    /// ops are `false` on a missing path, `Ne`/`Nin` are the logical negation of
    /// `Eq`/`In` (so a missing path satisfies them), and cross-kind compares are
    /// `false`.
    pub fn matches(&self, meta: &Metadata) -> bool {
        match self {
            Filter::And(children) => children.iter().all(|child| child.matches(meta)),
            Filter::Or(children) => children.iter().any(|child| child.matches(meta)),
            Filter::Not(child) => !child.matches(meta),
            Filter::Exists { path, present } => value_at_path(meta, path).is_some() == *present,
            Filter::Cmp { path, op, value } => eval_cmp(meta, path, *op, value),
        }
    }
}

fn eval_cmp(meta: &Metadata, path: &str, op: Op, operand: &MetaValue) -> bool {
    use std::cmp::Ordering::{Greater, Less};
    let found = value_at_path(meta, path);
    match op {
        Op::Eq => found.is_some_and(|value| value_eq(value, operand)),
        Op::Ne => !found.is_some_and(|value| value_eq(value, operand)),
        Op::Gt => found.and_then(|value| compare(value, operand)) == Some(Greater),
        Op::Gte => matches!(
            found.and_then(|value| compare(value, operand)),
            Some(Greater | std::cmp::Ordering::Equal)
        ),
        Op::Lt => found.and_then(|value| compare(value, operand)) == Some(Less),
        Op::Lte => matches!(
            found.and_then(|value| compare(value, operand)),
            Some(Less | std::cmp::Ordering::Equal)
        ),
        Op::In => found.is_some_and(|value| {
            operand_list(operand)
                .iter()
                .any(|item| value_eq(value, item))
        }),
        Op::Nin => !found.is_some_and(|value| {
            operand_list(operand)
                .iter()
                .any(|item| value_eq(value, item))
        }),
        Op::Contains => found.is_some_and(|value| match value {
            MetaValue::List(items) => items.iter().any(|item| value_eq(item, operand)),
            _ => false,
        }),
    }
}

fn operand_list(operand: &MetaValue) -> &[MetaValue] {
    match operand {
        MetaValue::List(items) => items,
        _ => std::slice::from_ref(operand),
    }
}

// ---- Pinecone-style JSON dict parser -----------------------------------

impl Filter {
    /// Parse a Pinecone/Mongo-style operator dict into a filter tree. A top-level
    /// object is an implicit `And` of its keys. Each field maps to either a bare
    /// value (implicit `$eq`) or an operator object (`{"$gte": 2020}`).
    /// `$and`/`$or` take arrays of sub-filters; `$not` takes one; `$exists` takes
    /// a bool. JSON numbers become `Int` when integral, else `Float`.
    pub fn from_json(value: &serde_json::Value) -> Result<Filter> {
        let object = value
            .as_object()
            .ok_or_else(|| invalid("filter must be a JSON object"))?;
        let mut clauses = Vec::new();
        for (key, sub) in object {
            match key.as_str() {
                "$and" => clauses.push(Filter::And(parse_filter_array(sub)?)),
                "$or" => clauses.push(Filter::Or(parse_filter_array(sub)?)),
                "$not" => clauses.push(Filter::Not(Box::new(Filter::from_json(sub)?))),
                _ => clauses.push(parse_field(key, sub)?),
            }
        }
        Ok(if clauses.len() == 1 {
            clauses.pop().expect("one clause")
        } else {
            Filter::And(clauses)
        })
    }
}

fn parse_filter_array(value: &serde_json::Value) -> Result<Vec<Filter>> {
    value
        .as_array()
        .ok_or_else(|| invalid("$and/$or expects an array"))?
        .iter()
        .map(Filter::from_json)
        .collect()
}

fn parse_field(path: &str, value: &serde_json::Value) -> Result<Filter> {
    // Bare value => implicit $eq.
    let Some(operators) = value.as_object() else {
        return Ok(Filter::Cmp {
            path: path.to_string(),
            op: Op::Eq,
            value: json_to_meta(value)?,
        });
    };
    if operators.is_empty() {
        return Err(invalid("empty operator object"));
    }
    let mut clauses = Vec::new();
    for (op_key, operand) in operators {
        let clause = match op_key.as_str() {
            "$eq" => Filter::Cmp {
                path: path.into(),
                op: Op::Eq,
                value: json_to_meta(operand)?,
            },
            "$ne" => Filter::Cmp {
                path: path.into(),
                op: Op::Ne,
                value: json_to_meta(operand)?,
            },
            "$gt" => Filter::Cmp {
                path: path.into(),
                op: Op::Gt,
                value: json_to_meta(operand)?,
            },
            "$gte" => Filter::Cmp {
                path: path.into(),
                op: Op::Gte,
                value: json_to_meta(operand)?,
            },
            "$lt" => Filter::Cmp {
                path: path.into(),
                op: Op::Lt,
                value: json_to_meta(operand)?,
            },
            "$lte" => Filter::Cmp {
                path: path.into(),
                op: Op::Lte,
                value: json_to_meta(operand)?,
            },
            "$in" => Filter::Cmp {
                path: path.into(),
                op: Op::In,
                value: json_to_meta(operand)?,
            },
            "$nin" => Filter::Cmp {
                path: path.into(),
                op: Op::Nin,
                value: json_to_meta(operand)?,
            },
            "$contains" => Filter::Cmp {
                path: path.into(),
                op: Op::Contains,
                value: json_to_meta(operand)?,
            },
            "$exists" => Filter::Exists {
                path: path.into(),
                present: operand
                    .as_bool()
                    .ok_or_else(|| invalid("$exists expects a bool"))?,
            },
            other => return Err(invalid(&format!("unknown filter operator `{other}`"))),
        };
        clauses.push(clause);
    }
    Ok(if clauses.len() == 1 {
        clauses.pop().expect("one clause")
    } else {
        Filter::And(clauses)
    })
}

/// Convert a JSON value to a `MetaValue`. Integral numbers become `Int`, other
/// numbers `Float`. There is no timestamp inference from JSON — numeric operands
/// compare against stored timestamps numerically.
pub fn json_to_meta(value: &serde_json::Value) -> Result<MetaValue> {
    Ok(match value {
        serde_json::Value::Null => MetaValue::Null,
        serde_json::Value::Bool(b) => MetaValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                MetaValue::Int(i)
            } else {
                MetaValue::Float(n.as_f64().ok_or_else(|| invalid("non-finite number"))?)
            }
        }
        serde_json::Value::String(s) => MetaValue::Str(s.clone()),
        serde_json::Value::Array(items) => {
            MetaValue::List(items.iter().map(json_to_meta).collect::<Result<_>>()?)
        }
        serde_json::Value::Object(map) => {
            let mut out = Metadata::new();
            for (key, sub) in map {
                out.insert(key.clone(), json_to_meta(sub)?);
            }
            MetaValue::Map(out)
        }
    })
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidSearchOptions(format!("metadata filter: {message}"))
}

// ---- Per-segment pruning stats -----------------------------------------
//
// Small, resident stats over a segment's rows, keyed by flattened leaf dotted
// path: numeric min/max and a presence bloom of string/tag values. `can_match`
// is SOUND — it returns `false` only when the stats prove no row in the segment
// can satisfy the filter — but not complete (bloom false positives cost reads,
// never wrong results). Stats are bounded: at most `MAX_STAT_PATHS` leaf paths
// are tracked; paths beyond the cap set `capped` and are never pruned.

const MAX_STAT_PATHS: usize = 64;
const STAT_BLOOM_BYTES: usize = 16;

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct LeafStat {
    min: Option<MetaValue>,
    max: Option<MetaValue>,
    bloom: [u8; STAT_BLOOM_BYTES],
}

fn is_numeric(value: &MetaValue) -> bool {
    matches!(
        value,
        MetaValue::Int(_) | MetaValue::Float(_) | MetaValue::Timestamp(_)
    )
}

impl LeafStat {
    fn observe(&mut self, value: &MetaValue) {
        use std::cmp::Ordering::{Greater, Less};
        if is_numeric(value) {
            if self
                .min
                .as_ref()
                .is_none_or(|min| compare(value, min) == Some(Less))
            {
                self.min = Some(value.clone());
            }
            if self
                .max
                .as_ref()
                .is_none_or(|max| compare(value, max) == Some(Greater))
            {
                self.max = Some(value.clone());
            }
        } else if let MetaValue::Str(s) = value {
            bloom_set(&mut self.bloom, s.as_bytes());
        }
    }

    /// Could some row's value at this leaf equal `operand`?
    fn eq_can_match(&self, operand: &MetaValue) -> bool {
        use std::cmp::Ordering::{Greater, Less};
        match operand {
            _ if is_numeric(operand) => match (&self.min, &self.max) {
                (Some(min), Some(max)) => {
                    compare(operand, min) != Some(Less) && compare(operand, max) != Some(Greater)
                }
                _ => false, // no numeric values at this leaf
            },
            MetaValue::Str(s) => bloom_maybe(&self.bloom, s.as_bytes()),
            _ => true, // bool/null/list/map operands are not tracked -> cannot prune
        }
    }

    /// Could some row's value satisfy the range op against `operand`?
    fn range_can_match(&self, op: Op, operand: &MetaValue) -> bool {
        use std::cmp::Ordering::{Equal, Greater, Less};
        if !is_numeric(operand) {
            return true; // only numeric ranges prune here
        }
        match op {
            Op::Gt => self
                .max
                .as_ref()
                .is_some_and(|max| compare(max, operand) == Some(Greater)),
            Op::Gte => self
                .max
                .as_ref()
                .is_some_and(|max| matches!(compare(max, operand), Some(Greater | Equal))),
            Op::Lt => self
                .min
                .as_ref()
                .is_some_and(|min| compare(min, operand) == Some(Less)),
            Op::Lte => self
                .min
                .as_ref()
                .is_some_and(|min| matches!(compare(min, operand), Some(Less | Equal))),
            _ => true,
        }
    }
}

/// Resident per-segment metadata stats used to prune segments before fetch.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MetadataStats {
    leaves: BTreeMap<String, LeafStat>,
    capped: bool,
}

impl MetadataStats {
    /// Build stats from a segment's rows (bounded by `MAX_STAT_PATHS`).
    pub fn from_rows<'a>(rows: impl IntoIterator<Item = &'a Metadata>) -> Self {
        let mut leaves: BTreeMap<String, LeafStat> = BTreeMap::new();
        let mut capped = false;
        for meta in rows {
            for_each_leaf(meta, |path, value| {
                if let Some(stat) = leaves.get_mut(path) {
                    stat.observe(value);
                } else if leaves.len() < MAX_STAT_PATHS {
                    let mut stat = LeafStat::default();
                    stat.observe(value);
                    leaves.insert(path.to_string(), stat);
                } else {
                    capped = true;
                }
            });
        }
        Self { leaves, capped }
    }

    /// Resident byte cost estimate for RAM accounting.
    pub fn resident_bytes_estimate(&self) -> usize {
        self.leaves
            .keys()
            .map(|path| path.len() + STAT_BLOOM_BYTES + 32)
            .sum::<usize>()
            + 1
    }

    /// SOUND prune predicate: `false` means no row in the segment can satisfy
    /// the filter, so the segment can be skipped without fetching it.
    pub fn can_match(&self, filter: &Filter) -> bool {
        match filter {
            Filter::And(children) => children.iter().all(|child| self.can_match(child)),
            Filter::Or(children) => children.iter().any(|child| self.can_match(child)),
            // Negation and existence are never pruned (a presence bloom can prove
            // "maybe present", never "absent from every row").
            Filter::Not(_) | Filter::Exists { .. } => true,
            Filter::Cmp { path, op, value } => self.cmp_can_match(path, *op, value),
        }
    }

    fn cmp_can_match(&self, path: &str, op: Op, operand: &MetaValue) -> bool {
        if matches!(op, Op::Ne | Op::Nin) {
            return true; // negated leaf: cannot prune
        }
        let Some(stat) = self.leaves.get(path) else {
            // Path has no scalar/list leaf in this segment. When stats are
            // complete (not capped), a scalar operand can never match here, so
            // prune; a map/list/null operand might still match an untracked
            // shape, so keep it.
            if self.capped {
                return true;
            }
            return !(is_numeric(operand) || matches!(operand, MetaValue::Str(_)));
        };
        match op {
            Op::Eq | Op::Contains => stat.eq_can_match(operand),
            Op::In => operand_list(operand)
                .iter()
                .any(|item| stat.eq_can_match(item)),
            Op::Gt | Op::Gte | Op::Lt | Op::Lte => stat.range_can_match(op, operand),
            Op::Ne | Op::Nin => true,
        }
    }

    /// Encode for persistence in the manifest.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(u8::from(self.capped));
        write_uvarint(self.leaves.len() as u64, &mut out);
        for (path, stat) in &self.leaves {
            write_str(path, &mut out);
            encode_opt_value(stat.min.as_ref(), &mut out);
            encode_opt_value(stat.max.as_ref(), &mut out);
            out.extend_from_slice(&stat.bloom);
        }
        out
    }

    /// Decode stats produced by [`MetadataStats::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Self::default());
        }
        let mut cursor = Cursor { bytes, pos: 0 };
        let capped = read_byte(&mut cursor)? != 0;
        let count = read_uvarint(&mut cursor)?;
        let mut leaves = BTreeMap::new();
        for _ in 0..count {
            let path = read_str(&mut cursor)?;
            let min = decode_opt_value(&mut cursor)?;
            let max = decode_opt_value(&mut cursor)?;
            let bloom = read_array::<STAT_BLOOM_BYTES>(&mut cursor)?;
            leaves.insert(path, LeafStat { min, max, bloom });
        }
        Ok(Self { leaves, capped })
    }
}

fn encode_opt_value(value: Option<&MetaValue>, out: &mut Vec<u8>) {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            encode_value(value, out);
        }
    }
}

fn decode_opt_value(cursor: &mut Cursor) -> Result<Option<MetaValue>> {
    Ok(if read_byte(cursor)? == 0 {
        None
    } else {
        Some(decode_value(cursor)?)
    })
}

fn bloom_hashes(bytes: &[u8]) -> [u64; 3] {
    let hash = blake3::hash(bytes);
    let raw = hash.as_bytes();
    let h1 = u64::from_le_bytes(raw[0..8].try_into().expect("8 bytes"));
    let h2 = u64::from_le_bytes(raw[8..16].try_into().expect("8 bytes"));
    [h1, h2, h1.wrapping_add(h2)]
}

fn bloom_set(bloom: &mut [u8; STAT_BLOOM_BYTES], bytes: &[u8]) {
    let bits = (STAT_BLOOM_BYTES * 8) as u64;
    for hash in bloom_hashes(bytes) {
        let bit = (hash % bits) as usize;
        bloom[bit / 8] |= 1 << (bit % 8);
    }
}

fn bloom_maybe(bloom: &[u8; STAT_BLOOM_BYTES], bytes: &[u8]) -> bool {
    let bits = (STAT_BLOOM_BYTES * 8) as u64;
    bloom_hashes(bytes).iter().all(|hash| {
        let bit = (*hash % bits) as usize;
        bloom[bit / 8] & (1 << (bit % 8)) != 0
    })
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

    fn doc() -> Metadata {
        Metadata::from([
            ("year".into(), MetaValue::Int(2021)),
            ("rating".into(), MetaValue::Float(4.5)),
            ("genre".into(), MetaValue::Str("comedy".into())),
            ("in_stock".into(), MetaValue::Bool(true)),
            ("posted_at".into(), MetaValue::Timestamp(1_700_000_000_000)),
            (
                "tags".into(),
                MetaValue::List(vec![
                    MetaValue::Str("award".into()),
                    MetaValue::Str("classic".into()),
                ]),
            ),
            (
                "author".into(),
                MetaValue::Map(Metadata::from([("rank".into(), MetaValue::Int(3))])),
            ),
        ])
    }

    fn parse(json: &str) -> Filter {
        Filter::from_json(&serde_json::from_str(json).unwrap()).unwrap()
    }

    #[test]
    fn flatten_visits_leaf_paths() {
        let meta = Metadata::from([
            (
                "a".into(),
                MetaValue::Map(Metadata::from([("b".into(), MetaValue::Int(1))])),
            ),
            (
                "tags".into(),
                MetaValue::List(vec![MetaValue::Str("x".into()), MetaValue::Str("y".into())]),
            ),
            ("n".into(), MetaValue::Null),
        ]);
        let mut leaves = Vec::new();
        for_each_leaf(&meta, |path, value| {
            leaves.push((path.to_string(), value.clone()))
        });
        assert_eq!(
            leaves,
            vec![
                ("a.b".into(), MetaValue::Int(1)),
                ("tags".into(), MetaValue::Str("x".into())),
                ("tags".into(), MetaValue::Str("y".into())),
            ]
        );
    }

    #[test]
    fn value_at_dotted_path() {
        let meta = doc();
        assert_eq!(
            value_at_path(&meta, "author.rank"),
            Some(&MetaValue::Int(3))
        );
        assert_eq!(value_at_path(&meta, "author.missing"), None);
        assert_eq!(value_at_path(&meta, "genre.x"), None); // through a non-map
    }

    #[test]
    fn comparison_operators() {
        let meta = doc();
        assert!(parse(r#"{"year":{"$gte":2020}}"#).matches(&meta));
        assert!(parse(r#"{"year":{"$gt":2020}}"#).matches(&meta));
        assert!(!parse(r#"{"year":{"$lt":2020}}"#).matches(&meta));
        assert!(parse(r#"{"year":{"$lte":2021}}"#).matches(&meta));
        assert!(parse(r#"{"genre":"comedy"}"#).matches(&meta)); // implicit $eq
        assert!(parse(r#"{"genre":{"$in":["comedy","drama"]}}"#).matches(&meta));
        assert!(!parse(r#"{"genre":{"$in":["drama"]}}"#).matches(&meta));
        assert!(parse(r#"{"tags":{"$contains":"award"}}"#).matches(&meta));
        assert!(!parse(r#"{"tags":{"$contains":"missing"}}"#).matches(&meta));
        assert!(parse(r#"{"author.rank":{"$gt":2}}"#).matches(&meta)); // dotted path
    }

    #[test]
    fn numeric_coercion_float_operand_vs_int_field() {
        // year is stored Int(2021); a JSON float operand still compares.
        assert!(parse(r#"{"year":{"$gte":2020.0}}"#).matches(&doc()));
        assert!(parse(r#"{"rating":{"$gt":4}}"#).matches(&doc())); // int operand vs float field
    }

    #[test]
    fn timestamp_compares_numerically_against_int_operand() {
        assert!(parse(r#"{"posted_at":{"$gte":1600000000000}}"#).matches(&doc()));
        assert!(!parse(r#"{"posted_at":{"$gt":1800000000000}}"#).matches(&doc()));
    }

    #[test]
    fn missing_field_and_negation_semantics() {
        let meta = doc();
        // positive ops on a missing path are false
        assert!(!parse(r#"{"nope":{"$eq":1}}"#).matches(&meta));
        assert!(!parse(r#"{"nope":{"$gt":1}}"#).matches(&meta));
        // Ne matches a missing path (Ne == Not(Eq))
        assert!(parse(r#"{"nope":{"$ne":1}}"#).matches(&meta));
        assert!(parse(r#"{"nope":{"$nin":[1,2]}}"#).matches(&meta));
        // Ne is false when the value IS equal
        assert!(!parse(r#"{"year":{"$ne":2021}}"#).matches(&meta));
        // $exists
        assert!(parse(r#"{"year":{"$exists":true}}"#).matches(&meta));
        assert!(parse(r#"{"nope":{"$exists":false}}"#).matches(&meta));
        assert!(!parse(r#"{"nope":{"$exists":true}}"#).matches(&meta));
    }

    #[test]
    fn ne_equals_not_eq_including_missing() {
        for path in ["year", "nope", "genre"] {
            let ne = Filter::Cmp {
                path: path.into(),
                op: Op::Ne,
                value: MetaValue::Int(2021),
            };
            let not_eq = Filter::Not(Box::new(Filter::Cmp {
                path: path.into(),
                op: Op::Eq,
                value: MetaValue::Int(2021),
            }));
            assert_eq!(ne.matches(&doc()), not_eq.matches(&doc()), "path {path}");
        }
    }

    #[test]
    fn cross_type_compare_is_false() {
        let meta = doc();
        assert!(!parse(r#"{"genre":{"$gt":5}}"#).matches(&meta)); // string vs number
        assert!(!parse(r#"{"year":{"$eq":"2021"}}"#).matches(&meta)); // int vs string
    }

    #[test]
    fn eq_on_list_is_not_element_match() {
        let meta = doc();
        // Eq against a scalar does not match a list element; that's $contains.
        assert!(!parse(r#"{"tags":{"$eq":"award"}}"#).matches(&meta));
        assert!(parse(r#"{"tags":{"$contains":"award"}}"#).matches(&meta));
        // Eq against the whole list matches.
        assert!(parse(r#"{"tags":{"$eq":["award","classic"]}}"#).matches(&meta));
    }

    #[test]
    fn logical_combinators() {
        let meta = doc();
        assert!(parse(r#"{"year":{"$gte":2020},"genre":{"$in":["comedy"]}}"#).matches(&meta)); // implicit AND
        assert!(parse(r#"{"$or":[{"year":{"$lt":2000}},{"genre":"comedy"}]}"#).matches(&meta));
        assert!(!parse(r#"{"$and":[{"year":{"$gte":2020}},{"genre":"drama"}]}"#).matches(&meta));
        assert!(parse(r#"{"$not":{"genre":"drama"}}"#).matches(&meta));
    }

    fn row(year: i64, genre: &str, tags: &[&str]) -> Metadata {
        Metadata::from([
            ("year".into(), MetaValue::Int(year)),
            ("genre".into(), MetaValue::Str(genre.into())),
            (
                "tags".into(),
                MetaValue::List(tags.iter().map(|t| MetaValue::Str((*t).into())).collect()),
            ),
        ])
    }

    #[test]
    fn stats_prune_soundness_over_many_filters() {
        // Two disjoint "segments" of rows.
        let seg_a = [
            row(2001, "comedy", &["award"]),
            row(2003, "drama", &["cult"]),
        ];
        let seg_b = [
            row(2020, "horror", &["gore"]),
            row(2024, "scifi", &["space"]),
        ];
        let stats_a = MetadataStats::from_rows(seg_a.iter());
        let stats_b = MetadataStats::from_rows(seg_b.iter());
        let filters = [
            r#"{"year":{"$gte":2020}}"#,
            r#"{"year":{"$lt":2005}}"#,
            r#"{"genre":"horror"}"#,
            r#"{"genre":{"$in":["comedy","drama"]}}"#,
            r#"{"tags":{"$contains":"space"}}"#,
            r#"{"tags":{"$contains":"award"}}"#,
            r#"{"year":{"$gte":2020},"genre":"scifi"}"#,
            r#"{"$or":[{"genre":"comedy"},{"year":{"$gt":2050}}]}"#,
        ];
        // Soundness: for every (segment, filter), if can_match==false, then no
        // row in that segment actually matches.
        for (stats, rows) in [(&stats_a, &seg_a[..]), (&stats_b, &seg_b[..])] {
            for f in filters {
                let filter = parse(f);
                if !stats.can_match(&filter) {
                    assert!(
                        !rows.iter().any(|r| filter.matches(r)),
                        "unsound prune of `{f}`"
                    );
                }
            }
        }
        // And it actually prunes: seg_a cannot match a 2020+ year filter.
        assert!(!stats_a.can_match(&parse(r#"{"year":{"$gte":2020}}"#)));
        assert!(stats_b.can_match(&parse(r#"{"year":{"$gte":2020}}"#)));
        // A genre absent from seg_a is pruned.
        assert!(!stats_a.can_match(&parse(r#"{"genre":"horror"}"#)));
    }

    #[test]
    fn stats_never_prune_negation_or_exists() {
        let stats = MetadataStats::from_rows([row(2001, "comedy", &["award"])].iter());
        assert!(stats.can_match(&parse(r#"{"genre":{"$ne":"comedy"}}"#)));
        assert!(stats.can_match(&parse(r#"{"genre":{"$nin":["comedy"]}}"#)));
        assert!(stats.can_match(&parse(r#"{"$not":{"genre":"comedy"}}"#)));
        assert!(stats.can_match(&parse(r#"{"missing":{"$exists":true}}"#)));
    }

    #[test]
    fn stats_cap_is_sound() {
        // A metadata map far wider than MAX_STAT_PATHS.
        let mut wide = Metadata::new();
        for i in 0..(MAX_STAT_PATHS + 20) {
            wide.insert(format!("k{i}"), MetaValue::Int(i as i64));
        }
        let stats = MetadataStats::from_rows([&wide]);
        // Even for an untracked key, pruning must stay sound: the row matches
        // k70==70, so can_match must not be false.
        let key = format!("k{}", MAX_STAT_PATHS + 10);
        let filter = parse(&format!(r#"{{"{key}":{{"$eq":{}}}}}"#, MAX_STAT_PATHS + 10));
        assert!(filter.matches(&wide));
        assert!(
            stats.can_match(&filter),
            "capped stats must not prune a real match"
        );
    }

    #[test]
    fn stats_bytes_roundtrip() {
        let stats = MetadataStats::from_rows(
            [
                row(2001, "comedy", &["award"]),
                row(2024, "scifi", &["space"]),
            ]
            .iter(),
        );
        assert_eq!(MetadataStats::from_bytes(&stats.to_bytes()).unwrap(), stats);
        assert_eq!(
            MetadataStats::from_bytes(&[]).unwrap(),
            MetadataStats::default()
        );
    }
}
