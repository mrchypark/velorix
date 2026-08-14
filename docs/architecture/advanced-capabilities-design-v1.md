# Advanced Capabilities Design V1

This document outlines the design for Phase 8: Deferred Advanced Capabilities.
Each capability requires separate design decisions and pre-implementation artifacts.

## Pre-Implementation Requirements

Each Phase 8 item requires:

1. **Worst-case state growth specification**
2. **Retraction algorithm specification**
3. **Replay determinism proof**
4. **Checkpoint compatibility proof**
5. **Real workload demonstration**
6. **Dedicated design document**
7. **Benchmark budget approval**

## 8.1 Analytic Window Frames and Navigation Functions

### Window Frame Specification

**ROWS frame**: Physical row offset from current row
- `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`
- Frame includes exactly 3 rows (2 before + current)

**RANGE frame**: Logical value range around current row
- `RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW`
- Frame includes all rows within the interval

**GROUPS frame**: Group offset from current group
- `GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING`
- Frame includes 3 groups

### Navigation Functions

**LAG(column, offset, default)**: Value at offset rows before current
- Default: NULL if no row at offset

**LEAD(column, offset, default)**: Value at offset rows after current
- Default: NULL if no row at offset

**FIRST_VALUE(column)**: First value in frame
**LAST_VALUE(column)**: Last value in frame
**NTH_VALUE(column, n)**: Nth value in frame

### State Requirements

- Window frame: BTreeMap<(partition_key, frame_start), Vec<row>>
- Navigation functions: In-memory buffer of frame rows
- Retraction: Remove row from frame, recompute navigation values

### Checkpoint Codec

- Serialize frame state as ordered row buffer
- Include partition key and frame boundaries
- Version with window frame codec identity

## 8.2 Exact Percentile, Median, and Ordered-Set Aggregates

### Percentile Design

**PERCENTILE_CONT(p)**: Continuous percentile interpolation
- Linear interpolation between adjacent values
- Returns Float64

**PERCENTILE_DISC(p)**: Discrete percentile selection
- Returns actual value at percentile position
- Returns same type as input

**MEDIAN**: Convenience alias for PERCENTILE_CONT(0.5)

### State Requirements

- Maintain sorted value multiset per group
- Size: O(n) where n is group size
- Retraction: Remove value, recompute percentile

### Checkpoint Codec

- Serialize sorted value multiset
- Include group key and value ordering
- Version with percentile codec identity

### Workload

- Analytics dashboards with percentile reporting
- Real-time monitoring with percentile alerts
- Data quality checks with percentile thresholds

## 8.3 Non-Equality, Interval, and Temporal Joins

### CROSS JOIN

-笛卡尔積 of two relations
- No join condition
- State: O(m * n) where m, n are input sizes
- Retraction: Remove matched pairs

### Interval Joins

**Overlap predicate**: `start_a < end_b AND start_b < end_a`
- State: Interval tree per side
- Retraction: Remove overlapping intervals

**Temporal containment**: `start_a <= start_b AND end_a >= end_b`
- State: Nested interval tracking
- Retraction: Remove contained intervals

### As-Of Joins

- Match by temporal proximity
- State: Time-ordered buffer per side
- Retraction: Remove time-proximate matches

### State Requirements

- Interval tree: O(n log n) construction, O(log n + k) query
- As-of: O(n) buffer with time-ordered insertion
- Retraction: Update interval tree/buffer

## 8.4 Deterministic User-Defined Functions

### UDF Contract

```rust
trait DeterministicUDF {
    /// Unique identifier for this UDF
    fn udf_id() -> &'static str;
    
    /// Version for upgrade compatibility
    fn version() -> u32;
    
    /// Input types
    fn input_types() -> Vec<SqlDataType>;
    
    /// Output type
    fn output_type() -> SqlDataType;
    
    /// Evaluate the function
    fn evaluate(args: &[Value]) -> Result<Value, UDFError>;
    
    /// Check determinism (same input → same output)
    fn is_deterministic() -> bool;
}
```

### Registration

- Register UDF at startup with metadata authority
- Validate determinism through testing
- Persist UDF identity in plan and checkpoint

### Versioning

- UDF version changes require checkpoint migration
- Old UDF versions remain callable for backward compatibility
- Upgrade path: deploy new version, migrate checkpoints, remove old version

## 8.5 Recursive and Mutually Recursive CTEs

### Fixpoint Computation

```sql
WITH RECURSIVE cte AS (
    -- Base case
    SELECT * FROM t WHERE condition
    UNION ALL
    -- Recursive case
    SELECT * FROM cte JOIN t ON cte.id = t.id WHERE condition
)
SELECT * FROM cte;
```

### Termination Guarantees

- Maximum recursion depth (configurable)
- Maximum state size (bounded by retention)
- No-cycle detection (graph cycle detection)
- Progress check (state must change each iteration)

### State Requirements

- Iteration state: Current frontier of new rows
- Total state: All materialized rows
- Retraction: Track which rows are still active

### Checkpoint Codec

- Serialize iteration frontier and materialized rows
- Include recursion depth and termination condition
- Version with recursive CTE codec identity

## Risk Assessment

| Capability | Risk | Impact | Mitigation |
|------------|------|--------|------------|
| Window frames | Unbounded frame size | High | Limit frame size, implement LRU eviction |
| Percentile | Exact computation expensive | Medium | Consider approximate algorithms |
| Non-equi joins | Unbounded state growth | High | Require bounded join conditions |
| UDFs | Non-deterministic functions | High | Validate determinism at registration |
| Recursive CTEs | Infinite recursion | High | Enforce termination guarantees |

## Verification Commands

```bash
# Phase 8
cargo test -p velorix-core --test view_plan window_frame
cargo test -p velorix-core --test view_plan percentile
cargo test -p velorix-core --test view_plan non_equi_join
cargo test -p velorix-core --test view_plan udf
cargo test -p velorix-core --test view_plan recursive_cte
```
