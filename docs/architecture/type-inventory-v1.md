# Type Inventory V1

This document inventories the scalar and aggregate state requirements by type
for Velorix's materialized-view runtime. It defines type-specific overflow,
NaN, and handling rules, type promotion and coercion rules, and version
expression encoding in checkpoint state.

## Supported Types

### Int64 (Fully Implemented)

**Scalar state**:
- 8 bytes per value
- Checked arithmetic: overflow returns error
- Division by zero returns error

**Aggregate state**:
- SUM: 8 bytes (running sum)
- COUNT: 8 bytes (running count)
- MIN/MAX: 8 bytes each (running extrema with value multiset)
- AVG: 16 bytes (running sum + count)
- COUNT(DISTINCT): Unbounded (HashSet of values)

**Key/Order**: Fully supported with canonical JSON encoding.

**Null handling**: NULL propagates through all operations. NULL compared
to any value returns NULL (unknown).

**Checkpoint encoding**: `DeltaValue::Int64(i64)` serialized as JSON number.

### Int8, Int16, Int32 (Fully Implemented)

**Scalar state**: 1, 2, 4 bytes respectively.
**Aggregate state**: Promoted to Int64 for SUM/AVG. COUNT is always Int64.
**Key/Order**: Fully supported.
**Checkpoint encoding**: Promoted to Int64 in delta batches.

### UInt8, UInt16, UInt32, UInt64 (Fully Implemented)

**Scalar state**: 1, 2, 4, 8 bytes respectively.
**Aggregate state**: Promoted to Int64/UInt64 for SUM/AVG.
**Key/Order**: Fully supported.
**Checkpoint encoding**: Promoted to Int64/UInt64 in delta batches.

### Float32, Float64 (Partially Implemented)

**Scalar state**: 4, 8 bytes respectively.
**Aggregate state**:
- SUM: Float64 (8 bytes)
- COUNT: Int64 (8 bytes)
- MIN/MAX: Float64 with NaN-aware ordering
- AVG: Float64 (sum/count)

**NaN handling**:
- NaN comparisons: NaN != NaN, NaN is not less/greater than any value
- NaN in MIN/MAX: NaN is never the result (filtered out)
- NaN in SUM/AVG: Propagates (NaN + x = NaN)

**Infinity handling**:
- +Infinity and -Infinity are valid values
- Overflow to infinity is silent (not an error)
- Division by zero returns +/-Infinity

**Key/Order**: Not supported as group key or join key.
**Checkpoint encoding**: `DeltaValue::F64(f64)` serialized as JSON number.

### Decimal128 (Partially Implemented)

**Scalar state**: 16 bytes (128-bit integer with fixed scale).
**Aggregate state**:
- SUM: Decimal128 with overflow detection
- COUNT: Int64
- MIN/MAX: Decimal128
- AVG: Float64 (lossy conversion)

**Precision/Scale rules**:
- Default: precision=38, scale=9
- Overflow: Returns error when result exceeds precision
- Division: Scale expansion to avoid precision loss

**Key/Order**: Not supported as group key or join key.
**Checkpoint encoding**: `Decimal128` serialized as string.

### Bool (Fully Implemented)

**Scalar state**: 1 byte (true/false).
**Aggregate state**: Not directly aggregatable.
**Key/Order**: Fully supported.
**Checkpoint encoding**: `DeltaValue::Bool(bool)` serialized as JSON boolean.

### String/Utf8 (Fully Implemented)

**Scalar state**: Variable length (heap allocated).
**Aggregate state**:
- COUNT: Int64
- MIN/MAX: String comparison (lexicographic)
- SUM/AVG: Not supported

**Key/Order**: Fully supported with lexicographic ordering.
**Checkpoint encoding**: `DeltaValue::String(String)` serialized as JSON string.

### Binary/Varbinary (Partially Implemented)

**Scalar state**: Variable length.
**Aggregate state**: COUNT only.
**Key/Order**: Not supported.
**Checkpoint encoding**: `DeltaValue::Binary(Vec<u8>)` serialized as base64.

### Date32 (Partially Implemented)

**Scalar state**: 4 bytes (days since epoch).
**Aggregate state**: MIN/MAX only.
**Key/Order**: Supported with chronological ordering.
**Checkpoint encoding**: `DeltaValue::Date32(i32)` serialized as ISO string.

### TimestampNanosecond (Partially Implemented)

**Scalar state**: 8 bytes (nanoseconds since epoch).
**Aggregate state**: MIN/MAX only.
**Key/Order**: Supported with chronological ordering.
**Checkpoint encoding**: `DeltaValue::TimestampNanosecond(i64)` serialized as ISO string.

### Interval (Not Implemented)

**Scalar state**: 12 bytes (months, days, nanoseconds).
**Aggregate state**: Not supported.
**Key/Order**: Not supported.
**Checkpoint encoding**: Not implemented.

### Array, Struct, Map (Not Implemented)

**Scalar state**: Variable (nested).
**Aggregate state**: Not supported.
**Key/Order**: Not supported.
**Checkpoint encoding**: Not implemented.

## Type Promotion Rules

### Arithmetic Operations

| Operation | Left Type | Right Type | Result Type |
|-----------|-----------|------------|-------------|
| +, -, * | Int64 | Int64 | Int64 |
| +, -, * | Float64 | Float64 | Float64 |
| +, -, * | Decimal128 | Decimal128 | Decimal128 |
| +, -, * | Int64 | Float64 | Float64 |
| / | Int64 | Int64 | Float64 |
| / | Float64 | Float64 | Float64 |
| / | Decimal128 | Decimal128 | Decimal128 |
| % | Int64 | Int64 | Int64 |
| % | Float64 | Float64 | Float64 |

### Aggregate Input Promotion

| Aggregate | Input Type | Internal Type | Output Type |
|-----------|------------|---------------|-------------|
| SUM | Int8-64 | Int64 | Int64 |
| SUM | UInt8-64 | UInt64 | UInt64 |
| SUM | Float32/64 | Float64 | Float64 |
| SUM | Decimal128 | Decimal128 | Decimal128 |
| COUNT | Any | Int64 | Int64 |
| MIN/MAX | Any | Same as input | Same as input |
| AVG | Int8-64 | Float64 | Float64 |
| AVG | Float32/64 | Float64 | Float64 |
| AVG | Decimal128 | Float64 | Float64 |

### Comparison Rules

| Left Type | Right Type | Comparison Type |
|-----------|------------|-----------------|
| Int* | Int* | Promote to wider Int |
| UInt* | UInt* | Promote to wider UInt |
| Int* | UInt* | Error (unsigned comparison) |
| Float* | Float* | Float64 comparison |
| Decimal128 | Decimal128 | Decimal128 comparison |
| String | String | Lexicographic |
| Date32 | Date32 | Chronological |
| Timestamp* | Timestamp* | Chronological |

## Expression Encoding Versioning

### Checkpoint State Version

All expression state is versioned in the checkpoint codec identity:
- `velorix-materialized-view-state-v1`: Current version
- Future versions will increment this when adding new types

### Expression Tree Version

The `SupportedProjectionExpr` enum is versioned through the logical plan
hash. Adding a new expression variant changes the plan hash, ensuring
checkpoint compatibility.

### State Layout Version

The `OperatorDagContractV1` includes state layout versioning. New type
support changes the state layout version, requiring checkpoint migration.

## Overflow and Boundary Rules

### Int64 Overflow
- Checked arithmetic: overflow returns error
- Aggregate overflow: error before state mutation

### Float64 Overflow
- Silent overflow to +/-Infinity
- NaN propagation through all operations
- Division by zero: +/-Infinity (not error)

### Decimal128 Overflow
- Checked arithmetic: overflow returns error
- Scale expansion during division to preserve precision

### String Overflow
- No overflow (variable length)
- Memory bounded by state quota

## Null Handling Rules

### SQL NULL Semantics
- NULL compared to any value: NULL (unknown)
- NULL in arithmetic: NULL
- NULL in aggregation: ignored (except COUNT(*))
- NULL in GROUP BY: NULL groups together

### Nullable Column Rules
- Output schema declares nullable columns
- Delta batches preserve null values
- Arrow arrays use null bitmaps

## Checkpoint Compatibility

### Version Migration
- New types require checkpoint version bump
- Old checkpoints remain readable (backward compatible)
- New checkpoints require new runtime version

### State Reconstruction
- State is reconstructed from checkpoint on restart
- Missing fields use default values
- Extra fields are ignored (forward compatible)
