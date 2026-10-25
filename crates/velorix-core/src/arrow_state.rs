//! Typed runtime snapshots. Serde values are mapped to Arrow columns without a
//! JSON text representation. Encoded buffers and decoded arrays are shared.
use self::value::Value;
use arrow::array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::{
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
    CompressionType,
};
use arrow::record_batch::RecordBatch;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    io::Cursor,
    sync::Arc,
};

#[derive(Debug, thiserror::Error)]
#[error("invalid Arrow runtime state: {0}")]
pub struct StateError(String);
fn err(e: impl std::fmt::Display) -> StateError {
    StateError(e.to_string())
}
mod deserializer;
mod serializer;
mod value;

const FORMAT: &str = "velorix-runtime-arrow-v2";
const MAX_DEPTH: usize = 128;

#[derive(Clone, Default)]
pub struct ArrowState(Option<Arc<Inner>>);
impl std::fmt::Debug for ArrowState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowState")
            .field("nodes", &self.0.as_ref().map_or(0, |s| s.batch.num_rows()))
            .field("expanded_bytes", &self.len())
            .field("encoded_bytes", &self.as_bytes().len())
            .finish()
    }
}
#[derive(Debug)]
struct Inner {
    batch: RecordBatch,
    bytes: Vec<u8>,
    ends: Vec<usize>,
    size: usize,
}
impl PartialEq for ArrowState {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl Eq for ArrowState {}
impl Serialize for ArrowState {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(self.as_bytes())
    }
}
impl<'de> Deserialize<'de> for ArrowState {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = ArrowState;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("Arrow IPC runtime state bytes")
            }
            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<ArrowState, E> {
                if v.is_empty() {
                    Ok(ArrowState::default())
                } else {
                    ArrowState::from_ipc(v, 64 * 1024 * 1024).map_err(E::custom)
                }
            }
            fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<ArrowState, E> {
                self.visit_bytes(&v)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> Result<ArrowState, A::Error> {
                let mut v = Vec::new();
                while let Some(b) = a.next_element::<u8>()? {
                    if v.len() >= 64 * 1024 * 1024 {
                        return Err(serde::de::Error::custom("nested state exceeds limit"));
                    }
                    v.push(b);
                }
                self.visit_bytes(&v)
            }
        }
        d.deserialize_bytes(Visitor)
    }
}
#[derive(Default)]
struct Node {
    kind: u8,
    children: u32,
    boolean: Option<bool>,
    signed: Option<i64>,
    unsigned: Option<u64>,
    float: Option<f64>,
    text: Option<String>,
    binary: Option<Vec<u8>>,
}
fn fields() -> Vec<Field> {
    vec![
        Field::new("kind", DataType::UInt8, false),
        Field::new("children", DataType::UInt32, false),
        Field::new("boolean", DataType::Boolean, true),
        Field::new("signed", DataType::Int64, true),
        Field::new("unsigned", DataType::UInt64, true),
        Field::new("float", DataType::Float64, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("binary", DataType::Binary, true),
    ]
}
fn flatten(
    value: Value,
    nodes: &mut Vec<Node>,
    ends: &mut Vec<usize>,
    depth: usize,
    limit: usize,
    size: &mut usize,
) -> Result<(), StateError> {
    if depth > MAX_DEPTH {
        return Err(err("depth limit"));
    }
    let index = nodes.len();
    let mut node = Node::default();
    let mut children = Vec::new();
    match value {
        Value::Unit => {}
        Value::Bool(v) => {
            node.kind = 1;
            node.boolean = Some(v)
        }
        Value::I64(v) => {
            node.kind = 2;
            node.signed = Some(v)
        }
        Value::U64(v) => {
            node.kind = 3;
            node.unsigned = Some(v)
        }
        Value::I128(v) => {
            node.kind = 9;
            node.binary = Some(v.to_le_bytes().to_vec());
        }
        Value::U128(v) => {
            node.kind = 10;
            node.binary = Some(v.to_le_bytes().to_vec());
        }
        Value::F64(v) => {
            node.kind = 4;
            node.float = Some(v)
        }
        Value::String(v) => {
            node.kind = 5;
            node.text = Some(v)
        }
        Value::Bytes(v) => {
            node.kind = 8;
            node.binary = Some(v)
        }
        Value::Seq(v) => {
            node.kind = 6;
            children = v
        }
        Value::Map(v) => {
            node.kind = 7;
            for (k, v) in v {
                children.push(k);
                children.push(v)
            }
        }
    }
    node.children = u32::try_from(children.len()).map_err(err)?;
    *size = size
        .checked_add(48)
        .and_then(|n| n.checked_add(node.text.as_ref().map_or(0, String::len)))
        .and_then(|n| n.checked_add(node.binary.as_ref().map_or(0, Vec::len)))
        .filter(|n| *n <= limit)
        .ok_or_else(|| err("runtime_checkpoint_bytes"))?;
    nodes.push(node);
    ends.push(0);
    for child in children {
        flatten(child, nodes, ends, depth + 1, limit, size)?;
    }
    ends[index] = nodes.len();
    if nodes[index].float.is_some_and(|v| !v.is_finite()) {
        return Err(err("nonfinite number"));
    }
    Ok(())
}
fn schema(metadata: HashMap<String, String>) -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(fields(), metadata))
}
fn base_metadata() -> HashMap<String, String> {
    [("format".into(), FORMAT.into())].into()
}
fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, StateError> {
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .and_then(|o| o.try_with_compression_level(Some(3)))
        .map_err(err)?;
    let mut bytes = Vec::new();
    {
        let mut w = StreamWriter::try_new_with_options(&mut bytes, &batch.schema(), options)
            .map_err(err)?;
        w.write(batch).map_err(err)?;
        w.finish().map_err(err)?;
    }
    Ok(bytes)
}
/// Bounds compressed declarations before Arrow allocates any expanded buffers.
fn preflight(bytes: &[u8], limit: usize) -> Result<usize, StateError> {
    if bytes.len() > limit.saturating_mul(2).saturating_add(16 * 1024) {
        return Err(err("encoded byte limit"));
    }
    let mut rest = bytes;
    let mut messages = 0;
    let mut expanded = 0usize;
    let mut schema_end = 0;
    loop {
        let prefix = rest.get(..8).ok_or_else(|| err("truncated frame"))?;
        if prefix[..4] != [255; 4] {
            return Err(err("IPC continuation"));
        }
        let n = u32::from_le_bytes(prefix[4..].try_into().map_err(err)?) as usize;
        rest = &rest[8..];
        if n == 0 {
            return if messages == 2 && rest.is_empty() {
                Ok(schema_end)
            } else {
                Err(err("extra IPC data"))
            };
        }
        let m = arrow::ipc::root_as_message(rest.get(..n).ok_or_else(|| err("frame size"))?)
            .map_err(err)?;
        let size = usize::try_from(m.bodyLength()).map_err(err)?;
        let end = n.checked_add(size).ok_or_else(|| err("frame overflow"))?;
        let body = rest.get(n..end).ok_or_else(|| err("body size"))?;
        if messages == 0 {
            if m.header_as_schema().is_none() || size != 0 {
                return Err(err("schema frame"));
            }
            schema_end = 8 + n;
        } else if messages == 1 {
            let b = m
                .header_as_record_batch()
                .ok_or_else(|| err("batch frame"))?;
            let rows = usize::try_from(b.length()).map_err(err)?;
            if rows == 0
                || rows > limit / 48
                || b.compression().is_none_or(|c| {
                    c.codec() != CompressionType::ZSTD
                        || c.method() != arrow::ipc::BodyCompressionMethod::BUFFER
                })
            {
                return Err(err("batch bounds or compression"));
            }
            for b in b.buffers().ok_or_else(|| err("buffers"))? {
                let start = usize::try_from(b.offset()).map_err(err)?;
                let len = usize::try_from(b.length()).map_err(err)?;
                let data = body
                    .get(
                        start
                            ..start
                                .checked_add(len)
                                .ok_or_else(|| err("buffer overflow"))?,
                    )
                    .ok_or_else(|| err("buffer bounds"))?;
                if len == 0 {
                    continue;
                }
                let n = i64::from_le_bytes(
                    data.get(..8)
                        .ok_or_else(|| err("buffer prefix"))?
                        .try_into()
                        .map_err(err)?,
                );
                let n = if n == -1 {
                    len - 8
                } else {
                    usize::try_from(n).map_err(err)?
                };
                expanded = expanded
                    .checked_add(n)
                    .filter(|n| *n <= limit.saturating_mul(2).saturating_add(1024))
                    .ok_or_else(|| err("expanded byte limit"))?;
            }
        } else {
            return Err(err("extra batch"));
        }
        rest = &rest[end..];
        messages += 1;
    }
}
impl ArrowState {
    /// Validate a candidate before commit without building or compressing IPC.
    pub fn measure_serializable<T: Serialize + ?Sized>(
        value: &T,
        limit: usize,
    ) -> Result<usize, StateError> {
        let mut budget = serializer::Budget { size: 0, limit };
        value.serialize(serializer::Serializer {
            budget: &mut budget,
            depth: 0,
        })?;
        Ok(budget.size)
    }
    /// Read only requested top-level fields, skipping retained state subtrees.
    pub fn project(&self, fields: &[&str]) -> Result<serde_json::Value, StateError> {
        let mut object = serde_json::Map::new();
        for key in fields {
            if let Some(value) = self.field::<serde_json::Value>(key)? {
                object.insert((*key).into(), value);
            }
        }
        Ok(serde_json::Value::Object(object))
    }
    pub fn from_serializable<T: Serialize + ?Sized>(value: &T) -> Result<Self, StateError> {
        Self::from_serializable_bounded(value, 64 * 1024 * 1024)
    }
    pub fn from_serializable_bounded<T: Serialize + ?Sized>(
        value: &T,
        limit: usize,
    ) -> Result<Self, StateError> {
        let mut budget = serializer::Budget { size: 0, limit };
        let value = value.serialize(serializer::Serializer {
            budget: &mut budget,
            depth: 0,
        })?;
        let mut nodes = Vec::new();
        let mut ends = Vec::new();
        let mut size = 0;
        flatten(value, &mut nodes, &mut ends, 0, limit, &mut size)?;
        let batch = RecordBatch::try_new(
            schema(base_metadata()),
            vec![
                Arc::new(UInt8Array::from_iter_values(nodes.iter().map(|n| n.kind))),
                Arc::new(UInt32Array::from_iter_values(
                    nodes.iter().map(|n| n.children),
                )),
                Arc::new(BooleanArray::from_iter(nodes.iter().map(|n| n.boolean))),
                Arc::new(Int64Array::from_iter(nodes.iter().map(|n| n.signed))),
                Arc::new(UInt64Array::from_iter(nodes.iter().map(|n| n.unsigned))),
                Arc::new(Float64Array::from_iter(nodes.iter().map(|n| n.float))),
                Arc::new(StringArray::from_iter(
                    nodes.iter().map(|n| n.text.as_deref()),
                )),
                Arc::new(BinaryArray::from_iter(
                    nodes.iter().map(|n| n.binary.as_deref()),
                )),
            ],
        )
        .map_err(err)?;
        let bytes = encode_batch(&batch)?;
        Ok(Self(Some(Arc::new(Inner {
            batch,
            bytes,
            ends,
            size,
        }))))
    }
    pub fn from_ipc(bytes: &[u8], limit: usize) -> Result<Self, StateError> {
        preflight(bytes, limit)?;
        let mut reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(err)?;
        if reader.schema().metadata().get("format").map(String::as_str) != Some(FORMAT) {
            return Err(err("format"));
        }
        let batch = reader
            .next()
            .transpose()
            .map_err(err)?
            .ok_or_else(|| err("empty state"))?;
        if reader.next().is_some() {
            return Err(err("extra batch"));
        }
        let bytes = Self::replace_metadata(bytes, base_metadata(), limit)?;
        let batch =
            RecordBatch::try_new(schema(base_metadata()), batch.columns().to_vec()).map_err(err)?;
        Self::validated(batch, bytes, limit)
    }
    fn validated(batch: RecordBatch, bytes: Vec<u8>, limit: usize) -> Result<Self, StateError> {
        if batch.schema().fields() != Schema::new(fields()).fields()
            || batch.num_rows() == 0
            || batch.column(0).null_count() != 0
            || batch.column(1).null_count() != 0
        {
            return Err(err("typed schema"));
        }
        struct Columns<'a> {
            kind: &'a UInt8Array,
            children: &'a UInt32Array,
            boolean: &'a BooleanArray,
            signed: &'a Int64Array,
            unsigned: &'a UInt64Array,
            float: &'a Float64Array,
            text: &'a StringArray,
            binary: &'a BinaryArray,
        }
        let c = Columns {
            kind: batch.column(0).as_any().downcast_ref().unwrap(),
            children: batch.column(1).as_any().downcast_ref().unwrap(),
            boolean: batch.column(2).as_any().downcast_ref().unwrap(),
            signed: batch.column(3).as_any().downcast_ref().unwrap(),
            unsigned: batch.column(4).as_any().downcast_ref().unwrap(),
            float: batch.column(5).as_any().downcast_ref().unwrap(),
            text: batch.column(6).as_any().downcast_ref().unwrap(),
            binary: batch.column(7).as_any().downcast_ref().unwrap(),
        };
        fn walk(
            c: &Columns<'_>,
            ends: &mut [usize],
            i: &mut usize,
            depth: usize,
            size: &mut usize,
            limit: usize,
        ) -> Result<(), StateError> {
            if depth > MAX_DEPTH || *i >= ends.len() {
                return Err(err("tree bounds"));
            }
            let id = *i;
            *i += 1;
            let kind = c.kind.value(id);
            let count = c.children.value(id) as usize;
            if kind > 10
                || ((kind != 6 && kind != 7) && count != 0)
                || (kind == 7 && !count.is_multiple_of(2))
            {
                return Err(err("tree node"));
            }
            let slot = match kind {
                1 => Some(0),
                2 => Some(1),
                3 => Some(2),
                4 => Some(3),
                5 => Some(4),
                8..=10 => Some(5),
                _ => None,
            };
            let nulls = [
                c.boolean.is_null(id),
                c.signed.is_null(id),
                c.unsigned.is_null(id),
                c.float.is_null(id),
                c.text.is_null(id),
                c.binary.is_null(id),
            ];
            if nulls
                .iter()
                .enumerate()
                .any(|(j, n)| *n == (slot == Some(j)))
            {
                return Err(err("scalar slots"));
            }
            if kind == 4 && !c.float.value(id).is_finite() {
                return Err(err("nonfinite float"));
            }
            if (kind == 9 || kind == 10) && c.binary.value(id).len() != 16 {
                return Err(err("128-bit scalar width"));
            }
            let extra = if kind == 5 {
                c.text.value(id).len()
            } else if kind >= 8 {
                c.binary.value(id).len()
            } else {
                0
            };
            *size = size
                .checked_add(48)
                .and_then(|n| n.checked_add(extra))
                .filter(|n| *n <= limit)
                .ok_or_else(|| err("runtime_checkpoint_bytes"))?;
            for _ in 0..count {
                walk(c, ends, i, depth + 1, size, limit)?;
            }
            ends[id] = *i;
            Ok(())
        }
        let mut ends = vec![0; batch.num_rows()];
        let mut i = 0;
        let mut size = 0;
        walk(&c, &mut ends, &mut i, 0, &mut size, limit)?;
        if i != ends.len() {
            return Err(err("trailing tree nodes"));
        }
        Ok(Self(Some(Arc::new(Inner {
            batch,
            bytes,
            ends,
            size,
        }))))
    }
    /// Replace only the schema message. Compressed buffers are copied verbatim.
    pub fn replace_metadata(
        bytes: &[u8],
        mut metadata: HashMap<String, String>,
        limit: usize,
    ) -> Result<Vec<u8>, StateError> {
        let end = preflight(bytes, limit)?;
        metadata.insert("format".into(), FORMAT.into());
        let mut out = Vec::new();
        {
            let _writer = StreamWriter::try_new(&mut out, &schema(metadata)).map_err(err)?;
        }
        out.extend_from_slice(&bytes[end..]);
        Ok(out)
    }
    pub fn metadata(bytes: &[u8], limit: usize) -> Result<HashMap<String, String>, StateError> {
        preflight(bytes, limit)?;
        let r = StreamReader::try_new(Cursor::new(bytes), None).map_err(err)?;
        Ok(r.schema().metadata().clone())
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref().map_or(&[], |s| s.bytes.as_slice())
    }
    /// Conservative bytes retained by the shared decoded state allocation.
    /// Includes encoded storage, Arrow arrays and schema allocations, and the
    /// row-boundary index capacity. Excludes the inline ArrowState Arc.
    pub fn allocated_retained_bytes(&self) -> usize {
        let Some(inner) = &self.0 else {
            return 0;
        };
        let schema = inner.batch.schema();
        let schema_bytes = std::mem::size_of_val(schema.as_ref())
            .saturating_add(std::mem::size_of_val(&schema.fields))
            .saturating_add(schema.fields.len() * std::mem::size_of::<arrow::datatypes::Field>())
            .saturating_add(
                schema
                    .fields
                    .iter()
                    .map(|field| {
                        field.name().capacity().saturating_add(
                            field
                                .metadata()
                                .iter()
                                .map(|(key, value)| key.capacity() + value.capacity() + 128)
                                .sum::<usize>(),
                        )
                    })
                    .sum::<usize>(),
            )
            .saturating_add(
                schema
                    .metadata
                    .iter()
                    .map(|(key, value)| key.capacity() + value.capacity() + 128)
                    .sum::<usize>(),
            );
        std::mem::size_of_val(inner)
            .saturating_add(inner.bytes.capacity())
            .saturating_add(inner.ends.capacity() * std::mem::size_of::<usize>())
            .saturating_add(inner.batch.get_array_memory_size())
            .saturating_add(schema_bytes)
    }
    /// Expanded typed-tree bytes; compression must never bypass resource budgets.
    pub fn len(&self) -> usize {
        self.0.as_ref().map_or(0, |s| s.size)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
    pub fn clear(&mut self) {
        self.0 = None;
    }
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, StateError> {
        let state = self.0.as_ref().ok_or_else(|| err("empty state"))?;
        T::deserialize(deserializer::Deserializer {
            state,
            index: 0,
            key: false,
        })
    }
    fn field_index(&self, key: &str) -> Result<Option<usize>, StateError> {
        let s = self.0.as_ref().ok_or_else(|| err("empty state"))?;
        let kinds = s
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        if kinds.value(0) != 7 {
            return Ok(None);
        }
        let strings = s
            .batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut i = 1;
        while i < s.ends[0] {
            let value = s.ends[i];
            if kinds.value(i) == 5 && strings.value(i) == key {
                return Ok(Some(value));
            }
            i = s.ends[value];
        }
        Ok(None)
    }
    pub fn has_non_null_field(&self, key: &str) -> bool {
        self.field_index(key).ok().flatten().is_some_and(|i| {
            self.0
                .as_ref()
                .unwrap()
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(i)
                != 0
        })
    }
    pub fn field<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, StateError> {
        self.field_index(key)?
            .map(|index| {
                T::deserialize(deserializer::Deserializer {
                    state: self.0.as_ref().unwrap(),
                    index,
                    key: false,
                })
            })
            .transpose()
    }
}
pub fn to_state<T: Serialize + ?Sized>(value: &T) -> Result<ArrowState, StateError> {
    ArrowState::from_serializable(value)
}
pub fn from_state<T: DeserializeOwned>(state: &ArrowState) -> Result<T, StateError> {
    state.decode()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_allocation_weight_includes_arrow_arrays_indexes_and_encoded_capacity() {
        let state = ArrowState::from_serializable(&vec!["compressible".repeat(8192); 8]).unwrap();
        let inner = state.0.as_ref().unwrap();
        let minimum = std::mem::size_of_val(inner)
            + inner.bytes.capacity()
            + inner.ends.capacity() * std::mem::size_of::<usize>()
            + inner.batch.get_array_memory_size();

        assert!(state.allocated_retained_bytes() >= minimum);
        assert!(inner.batch.get_array_memory_size() > 0);
        assert!(inner.bytes.capacity() >= inner.bytes.len());
        assert!(inner.ends.capacity() >= inner.ends.len());
    }
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct State {
        runtime_kind: String,
        rows: Vec<(i64, u64, f64, Option<String>)>,
        by_time: BTreeMap<i64, String>,
    }
    fn fixture(n: usize) -> State {
        State {
            runtime_kind: "synthetic".into(),
            rows: (0..n)
                .map(|i| {
                    (
                        i as i64,
                        u64::MAX,
                        if i == 0 { -0.0 } else { 1.0000000000000002 },
                        Some(format!("row-{i}")),
                    )
                })
                .collect(),
            by_time: [(17, "retained".into())].into(),
        }
    }
    #[test]
    fn typed_roundtrip_and_bounds() {
        let value = fixture(100);
        let state = to_state(&value).unwrap();
        let bytes = state.as_bytes();
        assert!(std::str::from_utf8(bytes).is_err());
        assert_eq!(from_state::<State>(&state).unwrap(), value);
        assert_eq!(
            state.field::<String>("runtime_kind").unwrap().as_deref(),
            Some("synthetic")
        );
        let restored = ArrowState::from_ipc(bytes, state.len()).unwrap();
        assert_eq!(restored, state);
        assert_eq!(
            restored.decode::<State>().unwrap().rows[0].2.to_bits(),
            (-0.0_f64).to_bits()
        );
        assert_eq!(to_state(&value).unwrap(), state);
        assert_eq!(
            ArrowState::measure_serializable(&value, state.len()).unwrap(),
            state.len()
        );
        assert!(ArrowState::measure_serializable(&value, state.len() - 1).is_err());
        assert!(ArrowState::from_ipc(bytes, state.len() - 1).is_err());
        assert!(ArrowState::from_ipc(b"{\"old\":true}", usize::MAX).is_err());
        assert!(ArrowState::from_serializable_bounded(&value, 128).is_err());
        let mut extra = bytes.to_vec();
        extra.push(0);
        assert!(ArrowState::from_ipc(&extra, state.len()).is_err());
        let metadata = [("owner".into(), "other".into())].into();
        let enveloped = ArrowState::replace_metadata(bytes, metadata, state.len()).unwrap();
        assert_eq!(
            ArrowState::from_ipc(&enveloped, state.len()).unwrap(),
            state
        );
        let nested = to_state(&(state.clone(), state.clone())).unwrap();
        let (a, b): (ArrowState, ArrowState) = nested.decode().unwrap();
        assert_eq!(a, state);
        assert_eq!(a, b);
    }
    #[test]
    fn structs_enums_and_numeric_map_keys_use_direct_arrow_deserialization() {
        for value in [i128::MIN, i128::MAX, -7, 0, 7] {
            let state = to_state(&value).unwrap();
            assert_eq!(state.decode::<i128>().unwrap(), value);
            assert_eq!(
                ArrowState::from_ipc(state.as_bytes(), state.len())
                    .unwrap()
                    .decode::<i128>()
                    .unwrap(),
                value
            );
        }
        let state = to_state(&u128::MAX).unwrap();
        assert_eq!(state.decode::<u128>().unwrap(), u128::MAX);
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        enum Shape {
            Unit,
            Newtype(i64),
            Tuple(),
            Pair(i64, String),
            Named { value: Option<u64> },
        }
        for shape in [
            Shape::Unit,
            Shape::Newtype(-7),
            Shape::Tuple(),
            Shape::Pair(3, "text".into()),
            Shape::Named { value: None },
            Shape::Named {
                value: Some(u64::MAX),
            },
        ] {
            let state = to_state(&shape).unwrap();
            assert_eq!(state.decode::<Shape>().unwrap(), shape);
        }
        let state = to_state(&fixture(2)).unwrap();
        let value: serde_json::Value = state.decode().unwrap();
        assert_eq!(value["by_time"]["17"], "retained");
        assert_eq!(
            to_state(&value).unwrap().decode::<State>().unwrap(),
            fixture(2)
        );
        let invalid =
            to_state(&serde_json::json!({"9999999999999999999999999999999999999999": 1})).unwrap();
        assert!(invalid.decode::<BTreeMap<i64, u64>>().is_err());
        assert!(state.has_non_null_field("rows"));
        assert!(!state.has_non_null_field("missing"));
        assert_eq!(
            state.project(&["runtime_kind"]).unwrap(),
            serde_json::json!({"runtime_kind":"synthetic"})
        );
    }
    #[test]
    fn state_recovers_in_fresh_process() {
        const FILE: &str = "VELORIX_RUNTIME_ARROW_TEST_FILE";
        if let Some(path) = std::env::var_os(FILE) {
            let state = ArrowState::from_ipc(&std::fs::read(path).unwrap(), 1024 * 1024).unwrap();
            assert_eq!(state.decode::<State>().unwrap(), fixture(100));
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.arrow");
        std::fs::write(&path, to_state(&fixture(100)).unwrap().as_bytes()).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "arrow_state::tests::state_recovers_in_fresh_process",
            ])
            .env(FILE, &path)
            .status()
            .unwrap();
        assert!(status.success());
        directory.close().unwrap();
        assert!(!path.exists());
    }
    #[test]
    #[ignore = "explicit performance measurement"]
    fn snapshot_benchmark() {
        use std::{hint::black_box, time::Instant};
        let value = fixture(25000);
        let n = 10;
        let json = serde_json::to_vec(&value).unwrap();
        let state = to_state(&value).unwrap();
        let start = Instant::now();
        for _ in 0..n {
            black_box(serde_json::to_vec(black_box(&value)).unwrap());
        }
        let json_encode = start.elapsed();
        let start = Instant::now();
        for _ in 0..n {
            black_box(to_state(black_box(&value)).unwrap());
        }
        let arrow_encode = start.elapsed();
        let start = Instant::now();
        for _ in 0..n {
            black_box(serde_json::from_slice::<State>(black_box(&json)).unwrap());
        }
        let json_decode = start.elapsed();
        let start = Instant::now();
        for _ in 0..n {
            black_box(state.decode::<State>().unwrap());
        }
        let arrow_decode = start.elapsed();
        let start = Instant::now();
        for _ in 0..n * 100 {
            black_box(state.field::<String>("runtime_kind").unwrap());
        }
        let fields = start.elapsed();
        let compressed_json = zstd::bulk::compress(&json, 3).unwrap();
        let start = Instant::now();
        for _ in 0..n {
            black_box(
                zstd::bulk::compress(&serde_json::to_vec(black_box(&value)).unwrap(), 3).unwrap(),
            );
        }
        let json_zstd_encode = start.elapsed();
        let start = Instant::now();
        for _ in 0..n {
            black_box(
                serde_json::from_slice::<State>(
                    &zstd::bulk::decompress(black_box(&compressed_json), json.len()).unwrap(),
                )
                .unwrap(),
            );
        }
        let json_zstd_decode = start.elapsed();
        let start = Instant::now();
        for _ in 0..n {
            black_box(
                ArrowState::from_ipc(black_box(state.as_bytes()), state.len())
                    .unwrap()
                    .decode::<State>()
                    .unwrap(),
            );
        }
        let arrow_cold_decode = start.elapsed();
        let start = Instant::now();
        for _ in 0..n {
            black_box(ArrowState::measure_serializable(black_box(&value), state.len()).unwrap());
        }
        let validation = start.elapsed();
        eprintln!("json_zstd3_bytes={} json_zstd_encode_ms={:.3} json_zstd_decode_ms={:.3} arrow_cold_decode_ms={:.3} candidate_validation_ms={:.3}", compressed_json.len(), json_zstd_encode.as_secs_f64()*1000.0/n as f64,json_zstd_decode.as_secs_f64()*1000.0/n as f64,arrow_cold_decode.as_secs_f64()*1000.0/n as f64,validation.as_secs_f64()*1000.0/n as f64);
        eprintln!("rows=25000 rounds={n} json_bytes={} arrow_zstd_bytes={} expanded_bytes={} json_encode_ms={:.3} arrow_encode_ms={:.3} json_decode_ms={:.3} arrow_decode_ms={:.3} field_us={:.3}",json.len(),state.as_bytes().len(),state.len(),json_encode.as_secs_f64()*1000.0/n as f64,arrow_encode.as_secs_f64()*1000.0/n as f64,json_decode.as_secs_f64()*1000.0/n as f64,arrow_decode.as_secs_f64()*1000.0/n as f64,fields.as_secs_f64()*1e6/(n*100) as f64);
    }
}
