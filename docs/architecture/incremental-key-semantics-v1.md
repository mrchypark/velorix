# Incremental Key Semantics V1

This document defines equality and deterministic encoding for keys admitted by
the current native incremental runtime. It also fixes the SQL NULL boundary for
future grouping and join work.

## Identity and Encoding

The v1 key codec identity is `velorix-incremental-key-semantics-v1`.

- A single-column key is encoded as its canonical JSON scalar.
- A composite key is encoded as a JSON object whose field names are stable
  catalog column IDs. Object fields are ordered lexicographically by column ID.
- Nested JSON object fields are recursively sorted. Array order is significant.
- Equality and ordered state lookup compare these canonical encodings. An
  implementation that introduces hashing must hash the exact same bytes with a
  versioned Velorix domain; hash equality alone is never row equality.
- Encoded-byte order is not SQL order. `ORDER BY`, Top-K, MIN, and MAX use the
  admitted physical type's comparison rules and deterministic tie breakers.

The admitted scalar encodings are:

| Physical type | Canonical key value |
| --- | --- |
| Boolean | JSON boolean |
| Signed and unsigned integers | Exact JSON integer |
| Float32/Float64 | Finite JSON number; `-0` is normalized to `0`; NaN and infinities fail closed |
| Decimal128 | Exact fixed-scale decimal string |
| Utf8 and dictionary Utf8 | Decoded UTF-8 string |
| Binary | Lower-case hexadecimal string |
| Date32 | Signed day count |
| Time64Nanosecond | Signed nanosecond count |
| TimestampNanosecond | Signed UTC epoch-nanosecond count; the schema retains timezone identity |
| JsonUtf8 | Parsed JSON with recursive object-field canonicalization; numeric JSON representations are not implicitly coerced |

List, struct, and map keys are not admitted. Adding one requires a new codec or
proof that the v1 encoding is unambiguous and stable.

## Types, Coercion, and Overflow

- Key identity includes the relation schema fingerprint and therefore the
  declared logical and physical type. Two different physical types are not
  equal merely because their JSON text is the same.
- Join admission requires identical physical key types. There is no implicit
  numeric or string coercion in v1.
- Integer, decimal, date, time, and timestamp conversion must be exact. A value
  outside its declared Arrow type or Decimal128 precision fails before state
  mutation.
- Dictionary indices are not key identity; their decoded string values are.

## NULL Semantics

Registered primary-key values are non-null at ingest. The currently admitted
incremental grouping and join paths also require non-null runtime keys.

When nullable expression keys are implemented:

- `GROUP BY` treats NULL components as not distinct, so two composite grouping
  keys with NULL in the same positions belong to one group.
- An ordinary equi-join never matches a key if either corresponding component is
  NULL. Raw key-codec equality must not bypass this SQL rule.
- Null-safe equality, if added, is a distinct predicate/operator capability.

Until an operator carries this distinction explicitly, nullable grouping and
join keys must fail closed during admission.

## Aggregate Output Identity

An admitted aggregate output has one of two explicit identity shapes:

- `Singleton` for a global aggregate with no grouping expressions.
- `GroupKey(non_empty_grouping_expressions)` for an ordinary grouped
  aggregate.

`Singleton` is not an empty `CandidateKey`, a missing identity, or a hidden SQL
column. The operator contract must represent it as a distinct tagged variant
with an `ExactlyOne` committed-cardinality guarantee. Its physical state and
publication keys use non-empty, versioned, domain-separated tokens under the
view, execution, and operator identities. State and output-publication tokens
must use different domains. Unknown or missing variants and codec mismatches
fail closed; restore never infers `Singleton` from an empty vector or empty
bytes.

A global aggregate exposes only the SQL-selected aggregate columns. Empty input
still produces exactly one row: `COUNT(*)` is zero and `SUM`, `MIN`, `MAX`, and
`AVG` are NULL. Updates publish a consolidated retraction of the prior
singleton row and insertion of the new row. Retracting the final input restores
the empty-input row rather than deleting the singleton output. Checkpoint and
publication identity determine whether that row has already been published so
restart cannot duplicate or omit it.

`GroupKey` contains the exact ordered vector of admitted grouping expressions
and uses SQL `IS NOT DISTINCT FROM` equality component by component. The plan,
state requirement, checkpoint identity, and output identity bind that order;
`GROUP BY a, b` and `GROUP BY b, a` therefore have different durable plan
identities even though order is not an `ORDER BY` promise. For the current
catalog-column key codec, canonical objects continue to sort stable column IDs
lexicographically; the enclosing plan/operator namespace binds the ordered
expression identity without changing existing single-key bytes. Duplicate
grouping expressions, omitted grouping identity, or grouping columns removed
from the public projection remain fail-closed until a versioned normalization
or out-of-band publication contract is implemented.

When a grouped aggregate loses its final contributing row, its output group is
retracted. This deliberately differs from `Singleton`, whose empty-input row
always remains. `GROUP BY ()` may lower to `Singleton` only when explicitly
admitted. `ROLLUP`, `CUBE`, and `GROUPING SETS` require a separate grouping-set
identity and remain unsupported.

Introducing the tagged singleton variant requires a new capability and
implementation identity. Existing non-empty grouped plans retain their current
wire form, plan hash, public column ordinals, canonical key bytes, and compatible
checkpoint identity. If that byte-for-byte preservation is not possible, the
plan/codec version must increase and restore must require an explicit migration
or rebuild.

## Version Binding

The key semantics identity must participate in the logical plan hash and the
standing-program/checkpoint identity. Restore must fail closed if the expected
identity differs, even when state bytes happen to deserialize.

Binding this identity into every plan and checkpoint is tracked separately in
Foundation 0A so this document does not claim restore compatibility before the
code enforces it.
