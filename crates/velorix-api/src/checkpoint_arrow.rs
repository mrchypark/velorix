//! The sole durable runtime-state body codec. The ordered tree is stored as
//! typed Arrow columns, not as a JSON string or a binary JSON envelope.
use super::*;
use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::{
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
    CompressionType,
};
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::{
    collections::HashMap,
    fmt,
    io::{Cursor, Write},
};

const FORMAT: &str = "velorix-checkpoint-arrow-zstd-v1";
const MAX_DEPTH: usize = 128;
const NULL: u8 = 0;
const BOOL: u8 = 1;
const SIGNED: u8 = 2;
const UNSIGNED: u8 = 3;
const FLOAT: u8 = 4;
const STRING: u8 = 5;
const ARRAY: u8 = 6;
const OBJECT: u8 = 7;

pub(super) fn encoded_limit(state_bytes: usize) -> usize {
    state_bytes.saturating_mul(48).saturating_add(16 * 1024)
}

fn invalid() -> ApiError {
    ApiError::bad_request("invalid Arrow/ZSTD checkpoint body")
}

#[derive(Default)]
struct Node {
    kind: u8,
    name: Option<String>,
    children: u32,
    boolean: Option<bool>,
    signed: Option<i64>,
    unsigned: Option<u64>,
    float: Option<f64>,
    text: Option<String>,
}

struct Tree {
    nodes: Vec<Node>,
    limit: usize,
}
struct Seed<'a> {
    tree: &'a mut Tree,
    name: Option<String>,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        if self.depth > MAX_DEPTH || self.tree.nodes.len() >= self.tree.limit {
            return Err(serde::de::Error::custom(
                "checkpoint tree exceeds structural limits",
            ));
        }
        d.deserialize_any(self)
    }
}
impl Seed<'_> {
    fn leaf(self, mut node: Node) {
        node.name = self.name;
        self.tree.nodes.push(node);
    }
}
impl<'de> Visitor<'de> for Seed<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("structured checkpoint state")
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        self.leaf(Node::default());
        Ok(())
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<(), E> {
        self.leaf(Node {
            kind: BOOL,
            boolean: Some(value),
            ..Default::default()
        });
        Ok(())
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<(), E> {
        self.leaf(Node {
            kind: SIGNED,
            signed: Some(value),
            ..Default::default()
        });
        Ok(())
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<(), E> {
        self.leaf(Node {
            kind: UNSIGNED,
            unsigned: Some(value),
            ..Default::default()
        });
        Ok(())
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<(), E> {
        if !value.is_finite() {
            return Err(E::custom("nonfinite checkpoint number"));
        }
        self.leaf(Node {
            kind: FLOAT,
            float: Some(value),
            ..Default::default()
        });
        Ok(())
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<(), E> {
        self.visit_string(value.into())
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<(), E> {
        self.leaf(Node {
            kind: STRING,
            text: Some(value),
            ..Default::default()
        });
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        let id = self.tree.nodes.len();
        self.tree.nodes.push(Node {
            kind: ARRAY,
            name: self.name,
            ..Default::default()
        });
        let mut children = 0u32;
        while sequence
            .next_element_seed(Seed {
                tree: self.tree,
                name: None,
                depth: self.depth + 1,
            })?
            .is_some()
        {
            children = children
                .checked_add(1)
                .ok_or_else(|| serde::de::Error::custom("checkpoint child overflow"))?;
        }
        self.tree.nodes[id].children = children;
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let id = self.tree.nodes.len();
        self.tree.nodes.push(Node {
            kind: OBJECT,
            name: self.name,
            ..Default::default()
        });
        let mut children = 0u32;
        while let Some(name) = map.next_key::<String>()? {
            map.next_value_seed(Seed {
                tree: self.tree,
                name: Some(name),
                depth: self.depth + 1,
            })?;
            children = children
                .checked_add(1)
                .ok_or_else(|| serde::de::Error::custom("checkpoint child overflow"))?;
        }
        self.tree.nodes[id].children = children;
        Ok(())
    }
}

fn fields() -> Vec<Field> {
    vec![
        Field::new("kind", DataType::UInt8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("children", DataType::UInt32, false),
        Field::new("boolean", DataType::Boolean, true),
        Field::new("signed", DataType::Int64, true),
        Field::new("unsigned", DataType::UInt64, true),
        Field::new("float", DataType::Float64, true),
        Field::new("text", DataType::Utf8, true),
    ]
}

pub(super) fn encode(record: &StandingRuntimeStatePayloadRecord) -> Result<Vec<u8>, ApiError> {
    let payload = &record.payload.payload;
    let metadata: HashMap<String, String> = [
        ("format", FORMAT.into()),
        ("schema_version", record.schema_version.to_string()),
        ("record_kind", record.record_kind.clone()),
        ("tenant_id", record.tenant_id.clone()),
        ("program_id", record.program_id.clone()),
        ("view_id", record.view_id.clone()),
        ("logical_epoch", record.logical_epoch.to_string()),
        (
            "checkpoint_codec_identity",
            record.checkpoint_codec_identity.clone(),
        ),
        ("state_content_hash", record.state_content_hash.clone()),
        ("source_kind", record.source_kind.clone()),
        (
            "payload_codec_identity",
            record.payload.codec_identity.clone(),
        ),
        ("state_bytes", payload.len().to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), v))
    .collect();
    encode_tree(payload, metadata)
}

fn encode_tree(payload: &str, metadata: HashMap<String, String>) -> Result<Vec<u8>, ApiError> {
    let mut tree = Tree {
        nodes: Vec::new(),
        limit: payload.len().saturating_add(1),
    };
    let mut parser = serde_json::Deserializer::from_str(payload);
    Seed {
        tree: &mut tree,
        name: None,
        depth: 0,
    }
    .deserialize(&mut parser)
    .map_err(ApiError::bad_request)?;
    parser.end().map_err(ApiError::bad_request)?;
    let nodes = &tree.nodes;
    let schema = Arc::new(Schema::new_with_metadata(fields(), metadata));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt8Array::from_iter_values(nodes.iter().map(|n| n.kind))),
            Arc::new(StringArray::from_iter(
                nodes.iter().map(|n| n.name.as_deref()),
            )),
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
        ],
    )
    .map_err(ApiError::internal)?;
    // Preserve the existing runtime commitment exactly, including object order
    // and floating-point bits. Never silently canonicalize a different state.
    if render(&batch, payload.len())? != payload {
        return Err(ApiError::bad_request(
            "checkpoint state is not in the runtime's canonical representation",
        ));
    }
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .and_then(|o| o.try_with_compression_level(Some(3)))
        .map_err(ApiError::internal)?;
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new_with_options(&mut bytes, &schema, options)
            .map_err(ApiError::internal)?;
        writer.write(&batch).map_err(ApiError::internal)?;
        writer.finish().map_err(ApiError::internal)?;
    }
    Ok(bytes)
}

/// Check compressed buffer declarations before Arrow allocates decompressed
/// arrays. Also reject extra batches, dictionaries and trailing bytes.
fn preflight(mut bytes: &[u8], max_state_bytes: usize) -> Result<(), ApiError> {
    let mut messages = 0;
    let mut expanded = 0usize;
    let max_arrow_bytes = encoded_limit(max_state_bytes);
    loop {
        let prefix = bytes.get(..8).ok_or_else(invalid)?;
        if prefix[..4] != [255; 4] {
            return Err(invalid());
        }
        let n = u32::from_le_bytes(prefix[4..].try_into().map_err(|_| invalid())?) as usize;
        bytes = &bytes[8..];
        if n == 0 {
            return if messages == 2 && bytes.is_empty() {
                Ok(())
            } else {
                Err(invalid())
            };
        }
        let message = arrow::ipc::root_as_message(bytes.get(..n).ok_or_else(invalid)?)
            .map_err(ApiError::bad_request)?;
        let size = usize::try_from(message.bodyLength()).map_err(ApiError::bad_request)?;
        let body = bytes
            .get(n..n.checked_add(size).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        match messages {
            0 => {
                if message.header_as_schema().is_none() || size != 0 {
                    return Err(invalid());
                }
            }
            1 => {
                let batch = message.header_as_record_batch().ok_or_else(invalid)?;
                let rows = usize::try_from(batch.length()).map_err(ApiError::bad_request)?;
                if rows == 0
                    || rows > max_state_bytes.saturating_add(1)
                    || batch.compression().is_none_or(|c| {
                        c.codec() != CompressionType::ZSTD
                            || c.method() != arrow::ipc::BodyCompressionMethod::BUFFER
                    })
                {
                    return Err(invalid());
                }
                for buffer in batch.buffers().ok_or_else(invalid)? {
                    let start = usize::try_from(buffer.offset()).map_err(ApiError::bad_request)?;
                    let len = usize::try_from(buffer.length()).map_err(ApiError::bad_request)?;
                    let data = body
                        .get(start..start.checked_add(len).ok_or_else(invalid)?)
                        .ok_or_else(invalid)?;
                    if len == 0 {
                        continue;
                    }
                    let length = i64::from_le_bytes(
                        data.get(..8)
                            .ok_or_else(invalid)?
                            .try_into()
                            .map_err(|_| invalid())?,
                    );
                    let length = if length == -1 {
                        len - 8
                    } else {
                        usize::try_from(length).map_err(ApiError::bad_request)?
                    };
                    expanded = expanded
                        .checked_add(length)
                        .filter(|n| *n <= max_arrow_bytes)
                        .ok_or_else(invalid)?;
                }
            }
            _ => return Err(invalid()),
        }
        bytes = bytes
            .get(n.checked_add(size).ok_or_else(invalid)?..)
            .ok_or_else(invalid)?;
        messages += 1;
    }
}

struct Columns<'a> {
    kind: &'a UInt8Array,
    name: &'a StringArray,
    children: &'a UInt32Array,
    boolean: &'a BooleanArray,
    signed: &'a Int64Array,
    unsigned: &'a UInt64Array,
    float: &'a Float64Array,
    text: &'a StringArray,
}

struct LimitedOutput {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(
                "checkpoint state exceeds decoded byte limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl LimitedOutput {
    fn byte(&mut self, byte: u8) -> Result<(), ApiError> {
        self.write_all(&[byte]).map_err(ApiError::bad_request)
    }
}
impl Columns<'_> {
    fn node(
        &self,
        index: &mut usize,
        depth: usize,
        output: &mut LimitedOutput,
    ) -> Result<(), ApiError> {
        if depth > MAX_DEPTH || *index >= self.kind.len() {
            return Err(invalid());
        }
        let i = *index;
        *index += 1;
        let kind = self.kind.value(i);
        let nulls = [
            self.boolean.is_null(i),
            self.signed.is_null(i),
            self.unsigned.is_null(i),
            self.float.is_null(i),
            self.text.is_null(i),
        ];
        let slot = match kind {
            BOOL => Some(0),
            SIGNED => Some(1),
            UNSIGNED => Some(2),
            FLOAT => Some(3),
            STRING => Some(4),
            NULL | ARRAY | OBJECT => None,
            _ => return Err(invalid()),
        };
        if nulls
            .iter()
            .enumerate()
            .any(|(j, null)| *null == (slot == Some(j)))
        {
            return Err(invalid());
        }
        let count = self.children.value(i);
        if kind != ARRAY && kind != OBJECT && count != 0 {
            return Err(invalid());
        }
        match kind {
            NULL => output.write_all(b"null").map_err(ApiError::bad_request)?,
            BOOL => serde_json::to_writer(&mut *output, &self.boolean.value(i))
                .map_err(ApiError::bad_request)?,
            SIGNED => serde_json::to_writer(&mut *output, &self.signed.value(i))
                .map_err(ApiError::bad_request)?,
            UNSIGNED => serde_json::to_writer(&mut *output, &self.unsigned.value(i))
                .map_err(ApiError::bad_request)?,
            FLOAT => {
                let value = self.float.value(i);
                if !value.is_finite() {
                    return Err(invalid());
                }
                serde_json::to_writer(&mut *output, &value).map_err(ApiError::bad_request)?;
            }
            STRING => serde_json::to_writer(&mut *output, self.text.value(i))
                .map_err(ApiError::bad_request)?,
            ARRAY | OBJECT => {
                output.byte(if kind == ARRAY { b'[' } else { b'{' })?;
                for child in 0..count {
                    if *index >= self.kind.len() || self.name.is_null(*index) != (kind == ARRAY) {
                        return Err(invalid());
                    }
                    if child > 0 {
                        output.byte(b',')?;
                    }
                    if kind == OBJECT {
                        serde_json::to_writer(&mut *output, self.name.value(*index))
                            .map_err(ApiError::bad_request)?;
                        output.byte(b':')?;
                    }
                    self.node(index, depth + 1, output)?;
                }
                output.byte(if kind == ARRAY { b']' } else { b'}' })?;
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }
}

fn render(batch: &RecordBatch, limit: usize) -> Result<String, ApiError> {
    fn column<T: 'static>(batch: &RecordBatch, index: usize) -> Result<&T, ApiError> {
        batch
            .column(index)
            .as_any()
            .downcast_ref()
            .ok_or_else(invalid)
    }
    if batch.schema().fields() != Schema::new(fields()).fields() || batch.num_rows() == 0 {
        return Err(invalid());
    }
    let c = Columns {
        kind: column(batch, 0)?,
        name: column(batch, 1)?,
        children: column(batch, 2)?,
        boolean: column(batch, 3)?,
        signed: column(batch, 4)?,
        unsigned: column(batch, 5)?,
        float: column(batch, 6)?,
        text: column(batch, 7)?,
    };
    if c.kind.null_count() != 0 || c.children.null_count() != 0 || !c.name.is_null(0) {
        return Err(invalid());
    }
    let mut bytes = LimitedOutput {
        bytes: Vec::new(),
        limit,
    };
    let mut index = 0;
    c.node(&mut index, 0, &mut bytes)?;
    if index != batch.num_rows() {
        return Err(invalid());
    }
    String::from_utf8(bytes.bytes).map_err(ApiError::bad_request)
}

pub(super) fn decode(
    bytes: &[u8],
    max_state_bytes: usize,
) -> Result<StandingRuntimeStatePayloadRecord, ApiError> {
    let (payload, m) = decode_tree(bytes, max_state_bytes)?;
    let get = |key: &str| m.get(key).cloned().ok_or_else(invalid);
    if m.len() != 12 {
        return Err(invalid());
    }
    Ok(StandingRuntimeStatePayloadRecord {
        schema_version: get("schema_version")?
            .parse()
            .map_err(ApiError::bad_request)?,
        record_kind: get("record_kind")?,
        tenant_id: get("tenant_id")?,
        program_id: get("program_id")?,
        view_id: get("view_id")?,
        logical_epoch: get("logical_epoch")?
            .parse()
            .map_err(ApiError::bad_request)?,
        checkpoint_codec_identity: get("checkpoint_codec_identity")?,
        state_content_hash: get("state_content_hash")?,
        source_kind: get("source_kind")?,
        payload: RuntimeCheckpointStatePayload {
            codec_identity: get("payload_codec_identity")?,
            payload,
        },
    })
}

fn decode_tree(
    bytes: &[u8],
    max_state_bytes: usize,
) -> Result<(String, HashMap<String, String>), ApiError> {
    preflight(bytes, max_state_bytes)?;
    let mut reader =
        StreamReader::try_new(Cursor::new(bytes), None).map_err(ApiError::bad_request)?;
    let schema = reader.schema();
    let m = schema.metadata();
    let get = |key: &str| m.get(key).cloned().ok_or_else(invalid);
    if get("format")? != FORMAT {
        return Err(invalid());
    }
    let state_bytes: usize = get("state_bytes")?.parse().map_err(ApiError::bad_request)?;
    if state_bytes > max_state_bytes {
        return Err(invalid());
    }
    let batch = reader
        .next()
        .transpose()
        .map_err(ApiError::bad_request)?
        .ok_or_else(invalid)?;
    if reader.next().is_some() {
        return Err(invalid());
    }
    let payload = render(&batch, state_bytes)?;
    if payload.len() != state_bytes
        || stable_bytes_hash(payload.as_bytes()) != get("state_content_hash")?
    {
        return Err(invalid());
    }
    Ok((payload, m.clone()))
}

pub(super) fn encode_retained_state(
    reference: &EventTimeStateRef,
    payload: &str,
) -> Result<Vec<u8>, ApiError> {
    let metadata = [
        ("format", FORMAT.to_string()),
        ("record_kind", "retained-state-v1".into()),
        ("object_key", reference.state_root.object_key.clone()),
        ("window_key", reference.window_key.clone()),
        ("logical_epoch", reference.logical_epoch.to_string()),
        (
            "state_content_hash",
            reference.state_root.content_hash.clone(),
        ),
        ("state_bytes", payload.len().to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), v))
    .collect();
    encode_tree(payload, metadata)
}

pub(super) fn decode_retained_state(
    bytes: &[u8],
    reference: &EventTimeStateRef,
    max_state_bytes: usize,
) -> Result<String, ApiError> {
    let (payload, m) = decode_tree(bytes, max_state_bytes)?;
    let epoch = reference.logical_epoch.to_string();
    for (key, expected) in [
        ("record_kind", "retained-state-v1"),
        ("object_key", reference.state_root.object_key.as_str()),
        ("window_key", reference.window_key.as_str()),
        (
            "state_content_hash",
            reference.state_root.content_hash.as_str(),
        ),
        ("logical_epoch", epoch.as_str()),
    ] {
        if m.get(key).is_none_or(|v| v != expected) {
            return Err(invalid());
        }
    }
    if m.len() != 7 {
        return Err(invalid());
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(payload: String) -> StandingRuntimeStatePayloadRecord {
        let mut checkpoint = crate::tests::test_runtime_checkpoint(Vec::new());
        checkpoint.state_root.content_hash = stable_bytes_hash(payload.as_bytes());
        checkpoint.state_payload.as_mut().unwrap().payload = payload;
        checkpoint_publication::standing_runtime_state_payload_record_for_checkpoint(
            &checkpoint,
            "purchases_by_user",
        )
        .unwrap()
        .1
    }

    #[test]
    fn typed_state_roundtrip_preserves_order_scalars_and_hash_exactly() {
        for payload in [
            "null",
            "true",
            "-9223372036854775808",
            "18446744073709551615",
            "-0.0",
            "1.0000000000000002",
            "1e-200",
            r#"{"z":[null,true,false,-7,18446744073709551615,-0.0,1.0000000000000002],"a":{"nested":[],"empty":{},"text":"한글\n\""}}"#,
        ] {
            let record = record(payload.into());
            let bytes = encode(&record).unwrap();
            assert!(bytes.starts_with(&[255; 4]));
            assert!(serde_json::from_slice::<Value>(&bytes).is_err());
            assert_eq!(encode(&record).unwrap(), bytes, "idempotent publication");
            assert_eq!(decode(&bytes, payload.len()).unwrap(), record);
            let mut reader = StreamReader::try_new(Cursor::new(&bytes), None).unwrap();
            let batch = reader.next().unwrap().unwrap();
            assert_eq!(batch.schema().fields(), Schema::new(fields()).fields());
            let strings = batch
                .column(7)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(
                !strings.iter().flatten().any(|s| s == payload),
                "no opaque state JSON column"
            );
        }
    }

    #[test]
    fn compressed_state_is_bounded_and_rejects_old_json_and_broken_frames() {
        let state = record(json!({"retained": "x".repeat(256 * 1024)}).to_string());
        let bytes = encode(&state).unwrap();
        assert!(bytes.len() < state.payload.payload.len() / 10);
        assert!(
            preflight(&bytes, 128).is_err(),
            "decompressed buffers bounded before Arrow allocation"
        );
        assert!(decode(&bytes, 128).is_err());
        assert!(
            decode(&serde_json::to_vec(&state).unwrap(), usize::MAX).is_err(),
            "no JSON compatibility reader"
        );
        for n in [0, 8, bytes.len() / 2, bytes.len() - 1] {
            assert!(decode(&bytes[..n], state.payload.payload.len()).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode(&trailing, state.payload.payload.len()).is_err());
    }

    #[test]
    fn state_body_recovers_in_fresh_process() {
        const TEST_FILE: &str = "VELORIX_CHECKPOINT_ARROW_TEST_FILE";
        let expected = record(
            r#"{"retained":[{"key":"a","value":7},{"key":"b","value":null}],"epoch":19}"#.into(),
        );
        if let Some(path) = std::env::var_os(TEST_FILE) {
            let bytes = std::fs::read(path).unwrap();
            assert_eq!(decode(&bytes, 128 * 1024).unwrap(), expected);
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.arrow");
        std::fs::write(&path, encode(&expected).unwrap()).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "checkpoint_arrow::tests::state_body_recovers_in_fresh_process",
            ])
            .env(TEST_FILE, &path)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "new process must restore from persisted Arrow bytes alone"
        );
        directory.close().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn changed_typed_values_fail_the_runtime_commitment() {
        let state = record("{\"amount\":1.25}".into());
        let bytes = encode(&state).unwrap();
        let batch = StreamReader::try_new(Cursor::new(&bytes), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut columns = batch.columns().to_vec();
        columns[6] = Arc::new(Float64Array::from(vec![None, Some(9.0)]));
        let changed = RecordBatch::try_new(batch.schema(), columns).unwrap();
        let mut bytes = Vec::new();
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .unwrap()
            .try_with_compression_level(Some(3))
            .unwrap();
        let mut writer =
            StreamWriter::try_new_with_options(&mut bytes, &batch.schema(), options).unwrap();
        writer.write(&changed).unwrap();
        writer.finish().unwrap();
        drop(writer);
        assert!(decode(&bytes, state.payload.payload.len()).is_err());
    }
}
