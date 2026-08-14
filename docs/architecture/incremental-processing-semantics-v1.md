# Incremental Processing Semantics V1

This document fixes the processing-time delta and bag semantics that every
native Velorix materialized-view operator must share. It does not widen the SQL
surface. Event-time progress is a separate contract.

## Delta Weights and Consolidation

- An input or output delta weight is a signed `i64`. Positive weights add row
  occurrences and negative weights retract them.
- A zero-weight record has no semantic effect. Epoch consolidation removes it.
- Consolidation identity is the pair `(logical key, complete row value)`, not
  the key alone. All records with that identity in an epoch are summed before
  the result is observed.
- Consolidation accumulates in `i128` and fails the epoch if the final value
  cannot be represented as `i64`.
- Operators must use checked addition, negation, and multiplication. An inner
  join output weight is `left_weight * right_weight`; overflow fails the epoch.
- A failed operator application must not mutate its previously committed state.

`DeltaBatch` is a signed change representation, so negative records are valid
inside an uncommitted epoch and in emitted retraction batches. They are not
proof that a negative source or output bag is valid.

## Committed Bag Rules

- A committed source relation and a state collection representing row
  occurrences must never contain a negative consolidated multiplicity.
- A keyed materialized output is a table snapshot: every published `(key,
  value)` row has weight exactly `1`. Zero-weight rows are absent, and any other
  committed weight fails closed.
- Retractions may temporarily make an epoch's change batch negative. Applying
  the complete epoch to the prior committed bag must yield non-negative
  multiplicities.
- An over-retraction, arithmetic overflow, or invalid restored multiplicity
  aborts the whole epoch. Its input frontier, operator state, output object,
  checkpoint manifest, and authoritative pointer must remain at the prior
  committed generation.
- Replay of an already committed epoch is identified by the standing-program
  idempotency key and must return the existing durable result rather than apply
  its weights again.

The current runtime already validates unit-weight published snapshots and uses
checked delta/join arithmetic. Uniform non-negative validation for every
stateful operator remains part of the Foundation 0A exit gate.

## Key Semantics Boundary

Key encoding is versioned independently from weight semantics. Until the key
codec contract is complete, new key-capable types and nullable grouping keys
remain fail-closed. In particular:

- registered primary-key input values are non-null;
- SQL `GROUP BY` NULL equality and SQL join NULL non-matching are distinct and
  must not be inferred from raw key-byte equality;
- join keys must have identical admitted physical types; implicit cross-type
  coercion is unsupported;
- deterministic encoding is required for equality, state lookup, checkpoint
  restore, and plan identity, while SQL ordering must use typed comparison and
  not encoded-byte order.

The exact per-type encoding and NULL composite-key rules are the next
Foundation 0A checklist item.

## Outer Joins Are Dynamic General-Retract Tables

At every committed epoch, a left outer join materializes the SQL bag `L LEFT
JOIN R`, rather than an append-only history of matches. For each occurrence of
a left row, let `m` be the consolidated multiplicity of matching right rows:

- when `m = 0`, the output contains one null-extended row for that left-row
  occurrence;
- when `m > 0`, the null-extended row is absent and every matching pair has the
  checked product of its left and right multiplicities;
- a right-side transition from zero to positive matches retracts the
  null-extended row before inserting matched rows;
- a transition from positive matches back to zero retracts the final matched
  rows and inserts the null-extended row.

Those changes are signed `general_retract` deltas. The output does not become
append-only or final merely because no watermark is present. The operator must
retain and checkpoint both input bags so restore preserves the zero/nonzero
match boundary exactly. Right outer join uses the same semantics after operand
swapping. Full outer join applies the same zero/nonzero boundary symmetrically:
each side emits its null-extended occurrences while the opposite match count is
zero, retracts them when the first opposite match arrives, and restores them
when the final opposite match is removed.

The consolidated delta emitted for an epoch is the bag difference between the
previous committed outer-join snapshot and the current committed snapshot.
Ordinary SQL `=` join predicates apply: a NULL join key does not match another
NULL join key because the predicate is UNKNOWN, so the preserved occurrence is
null-extended when it has no other match. A preserved-side update is the same
semantic transition as retracting its old occurrence and inserting its new
occurrence.

In the V1 native left-join operator, the checkpointed right-side multiset is the
authority for match multiplicity. The operator derives the checked total for
each touched key before and after an epoch; it restores a null-extended row only
when that total crosses from positive to zero. A separately cached count is not
part of the checkpoint and cannot disagree with the retained multiset.

Columns from the non-preserved side are nullable after null extension, even if
their source columns are non-null. Candidate keys are dropped unless uniqueness
is separately proved over the null-extended result.

For the admitted grouped-left-join family, a top-level `WHERE` predicate that
references the right side is evaluated after null extension. It is not pushed
into the right input: doing so would change NULL-accepting predicates such as
`right_value IS NULL` by turning a real, filtered match into a synthetic
unmatched row. Right-side CTE and derived-source predicates remain fail-closed
until the plan records their pre-join provenance separately.

Grouped aggregates distinguish group presence from aggregate argument
presence. A null-extended row keeps the group and contributes to `COUNT(*)`,
while NULL right arguments do not contribute to `COUNT(expr)` or
`COUNT(DISTINCT expr)`. If no qualifying non-NULL argument remains, including
when an aggregate `FILTER` is always FALSE or UNKNOWN, `SUM`, `AVG`, `MIN`, and
`MAX` publish SQL NULL rather than an accumulator's internal zero. These
presence counts are checkpointed only for the extended right-dependent plan
shapes; legacy narrow-left plans retain their existing state representation.

## Semi Joins Preserve Left Bag Multiplicity

A binary semi join materializes each left-row occurrence exactly when the
consolidated total multiplicity of matching right rows is positive. The right
bag is evidence of existence only: a second or later matching right occurrence
must not multiply the left output. A zero-to-positive right match-count
transition inserts the retained left bag for that key, and a positive-to-zero
transition retracts it. Left deltas pass through unchanged while the key has a
positive right match count and remain retained without output otherwise.

The native semi-join checkpoint retains both input bags under the versioned
`velorix-native-semi-join-v1` codec. It derives checked match counts from the
right bag instead of persisting a second count cache. This is an internal
key-level operator contract; public SQL admission must separately enforce SQL
NULL equality semantics. Ordinary anti join and null-aware anti join remain
distinct contracts.

An ordinary binary anti join materializes each left-row occurrence exactly
while the consolidated matching-right multiplicity is zero. Its right-side
boundary transitions are the inverse of semi join: zero-to-positive retracts
the current retained left bag and positive-to-zero inserts it. Left deltas pass
through only while the right count is zero. Additional right duplicates and
partial duplicate deletions are silent. `velorix-native-anti-join-v1` is a
separate codec identity, although its checkpoint also retains both authoritative
bags. Null-aware anti remains unimplemented and must not be represented as an
ordinary anti mode.

The bounded public SQL V1 decorrelates direct `EXISTS` and `NOT EXISTS` only
when the correlation is one equality between the complete, non-null scalar
primary keys of two distinct registered relations. It lowers those forms to
the ordinary `SemiEquiJoin` and `AntiEquiJoin` logical nodes. This restriction
makes SQL `=` NULL behavior unambiguous without adding a nullable mode to either
operator. All residual, nullable, non-primary, composite, and broader subquery
forms fail closed.

## Durable Output Identity and Publication

Durable identities are monotonic and content-addressed:

- state payload identity is `(tenant, program, view, logical epoch, state
  content hash)`;
- output delta/page/manifest identity is `(tenant, program, view, logical
  epoch, output content hash)` plus page index where applicable;
- checkpoint record identity is `(tenant, program, view, logical epoch, state
  content hash)` and binds its state and output references;
- the authoritative checkpoint pointer contains the selected checkpoint key,
  epoch, content hash, manifest hash, and output refs.

Publication order is output delta objects, state payload object, immutable
checkpoint record, authoritative metadata pointer, and finally the object-store
latest cache. Immutable objects use create-only writes. Repeating an identical
write succeeds only when the existing bytes are identical; the same key with a
different body is a conflict.

When a metadata store is configured, its fenced compare-and-swap pointer is the
only visibility authority. Objects written before a failed pointer advance are
orphans and must not become query or recovery input through listing or the
latest cache. A stale writer or stale expected pointer conflicts. Repeating the
same candidate is idempotent. The object-store latest record is a cache and may
lag the metadata pointer.

In local mode without a metadata store, the latest object write is the final
visibility step and a failure is returned to the caller. No acknowledgement may
precede the applicable final authority update.

For an ingest epoch, the durable acknowledgement record is
`ingest_epoch_view_convergence_v2`. It binds the ingest manifest and view to the
authoritative checkpoint key, state hash, exact output object references, and
`velorix-durable-output-publication-v1`. Retry validation compares all of those
fields to the selected local or metadata-backed checkpoint authority. A stale,
tampered, or partially published convergence record is not an acknowledgement.

## View Bootstrap Frontier

View creation uses a persisted frontier-vector barrier, not a boolean derived
from an ingest-log scan.

1. Admission assigns a bootstrap generation and captures an authoritative,
   versioned source cut `B`, represented by relation/catalog generations and a
   per-stream-generation, per-partition-generation frontier vector `F`, while
   making the admitted view discoverable to subsequent ingest. Each `F[p]` is a
   sealed contiguous frontier on an immutable range boundary, not a reservation
   or maximum-observed-offset high-water mark.
2. The registry persists `F`, the view spec/plan hash, and the lifecycle state
   `bootstrapping` as one metadata decision. A view in this state is not
   queryable.
3. Bootstrap replays the deterministic source snapshot consisting of all
   committed ranges at or below `F`. Each range is validated through the normal
   ingest envelope and schema path; source tables are not recomputed through a
   separate SQL engine.
4. Deltas committed after `F` are the tail. They are either durably queued for
   this bootstrap generation or discovered from the authoritative ingest index,
   then applied through the same native operator path only after the snapshot
   prefix for their partition.
5. After a checkpoint covers `B`, admission captures and persists one immutable
   activation target `A`. The view becomes queryable only when one fenced
   control-record compare-and-swap publishes a checkpoint pointer that covers
   `A` and changes the lifecycle to `running`.
6. A stream/partition generation first observed after `B`'s input-catalog epoch
   is tail input starting at zero. Reuse of a numeric stream or partition ID
   with a new generation never inherits the old generation's frontier.
7. A crash resumes from the last authoritative view checkpoint and the persisted
   bootstrap generation. It must not recapture `F`, replay a covered range, or
   promote lifecycle state from object listing.

An unresolved offset reservation creates a hole. `F[p]` stops at the last
sealed range before that hole even if higher ranges have already committed;
both the later resolution of the hole and those higher ranges remain tail.
Cancellation requires a durable terminal skip record before the seal may
advance. Once a frontier is sealed, ingest must reject a later commit below it.
Consequently, a backend that can only read reservation maxima or maximum
committed offsets cannot implement this contract.

The source cut also contains an input-catalog epoch (or an equivalent
linearizable enumeration token), so omission from the frontier map proves that
an input identity did not exist at the cut. The bootstrap record and source cut
must be serialized by the same multi-writer metadata authority; an object-store
active-view write performed separately from ingest admission is not that
linearization point.

Promotion uses the fixed authoritative cut `A`; it must not chase a moving
ingest head. The lifecycle transition to `running` succeeds only as a fenced
metadata compare-and-swap that verifies the published checkpoint pointer
belongs to the bootstrap generation and plan hash, preserves checkpoint
lineage, and covers `A`'s catalog epoch and every input identity. Ingest committed
after `A` remains ordinary active tail lag and is rediscovered from the log.
Notifications may wake workers but are never correctness authority. If
lifecycle and checkpoint-pointer conditions cannot be checked by one authority
operation, promotion must fail closed rather than infer completion from an
empty log scan.

The bootstrap control record pins every required immutable ingest range until
activation, unless the source-cut mechanism already provides an equivalent
retention lease. Missing input below the pinned retention floor is a hard
bootstrap failure, never a live-only fallback.

For each partition, ranges are applied in offset order. Across independent
partitions and relations, any deterministic interleaving is valid only when it
produces the same consolidated bag and checkpoint-bound frontier vector. Public
multi-relation ingest remains a sequence of per-relation frontier advances, not
an atomic cross-relation transaction.

The current `standing_runtime_create_requires_backfill` boolean log scan does
not implement this barrier: it can race between scanning committed input and
making the view visible, and completion of a later backfill can race with new
ingest. It remains compatibility scaffolding until admission persists `F` and
the snapshot/tail race tests pass. Existing late-view and crash-window tests are
useful replay evidence but are not proof of this stronger contract.

## Evidence

- `crates/velorix-core/src/delta.rs` implements checked consolidation and
  zero-weight removal.
- `crates/velorix-core/src/operator.rs` implements checked join-weight
  multiplication and copy-before-apply side-state updates.
- `NativeLeftJoinOperator` emits and checkpoints the zero-to-positive and
  positive-to-zero match transitions as signed general-retract deltas.
- `NativeFullJoinOperator` applies those transitions symmetrically to both
  checkpointed bags; its exact-delta test covers duplicate multiplicity,
  partial/final deletes, and restore.
- `NativeSemiJoinOperator` checkpoints both bags and proves that duplicate
  right matches do not multiply left output across zero/nonzero transitions and
  restore.
- `NativeAntiJoinOperator` uses a separate codec and proves exact inverse
  zero/nonzero transitions, blocked left changes, right duplicates, and restore.
- The shared semi/anti mutation matrix proves a two-step checkpoint/restart
  sequence across a left key update, duplicate right insertion, partial right
  deletion, and final right deletion.
- The public `EXISTS`/`NOT EXISTS` admission-to-restart matrix exercises the
  same native nodes through the runtime factory, including duplicate right
  matches, a left update, two restores, and the final match deletion.
- The REST recovery test creates both view forms, materializes and queries them,
  restores fresh API state from durable checkpoints, and proves the inverse
  match transition after restart.
- `crates/velorix-runtime/src/materialized_view_runtime.rs` validates published
  materialized snapshots as unit-weight rows.
- `crates/velorix-core/tests/operators.rs` covers signed deltas, state
  compaction, and atomic join-overflow rejection.
- `standing_runtime_checkpoint_publish_is_linearizable_and_idempotent` proves
  fenced pointer publication and duplicate handling.
- `standing_runtime_checkpoint_read_keeps_old_meta_pointer_when_new_checkpoint_is_orphaned`
  proves that an immutable object written without a pointer advance is not
  visible.
- `standing_runtime_checkpoint_read_uses_meta_pointer_when_latest_cache_is_stale`
  proves that the object-store latest cache is not metadata authority.
