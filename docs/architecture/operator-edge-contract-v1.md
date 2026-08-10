# Operator and Edge Capability Contract V1

Status: internal semantic contract. It does not make the family-specific runtime
executors public or independently extensible.

## Purpose

Velorix describes an incremental relational DAG with three distinct objects:

- an output port records facts guaranteed by its producer;
- an input port records facts required by its consumer;
- an edge only identifies which output port feeds which input port.

Capabilities are not copied onto every edge. This keeps one producer guarantee
authoritative when an output fans out to multiple consumers. Planner-generated
contracts are proof artifacts, not user assertions. Admission must derive the
expected contract from operator semantics and reject a persisted contract that
does not match it before constructing a runtime.

The wire identity is `velorix-operator-dag-contract-v1`. Unknown versions and
unknown serialized fields fail closed.

## Producer guarantees

Every output port carries:

- a stable column ID, logical type identity, and `non_null` or `nullable` state
  for every column;
- changelog mode: `append_only`, `upsert(identity_key)`, or
  `general_retract`;
- candidate keys and their equality semantics (`non_null_equality` or
  `sql_not_distinct`);
- an explicit uniqueness guarantee;
- replay determinism;
- a processing-frontier guarantee;
- an event-time watermark guarantee, independently of processing progress.

A relation schema primary key is not automatically a stream uniqueness proof.
Likewise, an upsert identity is not inferred from a candidate key: a V1 upsert
identity must be an explicitly guaranteed, non-null candidate key.

## Consumer requirements

Every input port states the accepted changelog strength plus required columns,
nullability, candidate keys, replay determinism, processing frontier, and
watermark. Edge compatibility is `producer guarantee >= consumer requirement`.

The changelog order is:

```text
append_only >= upsert(exact identity) >= general_retract
```

Two upsert identities are compatible only when they match exactly. A producer
key on `(a)` satisfies a consumer uniqueness requirement on `(a, b)`, because
uniqueness of `(a)` implies uniqueness of `(a, b)`; the reverse is invalid.
Key equality semantics must match. `non_null` satisfies a nullable requirement,
but nullable does not satisfy a non-null requirement.

Processing frontiers prove checkpoint/replay completeness. Watermarks prove
event-time completeness. Neither can satisfy a requirement for the other.

## Stateful operators

State belongs to an operator, not an edge. A state contract records one of:

- statically bounded with a positive row bound;
- retention bounded with a positive retention interval;
- watermark bounded with an event-time column and allowed lateness;
- unbounded.

It also records a non-empty, positive-version checkpoint codec identity.
Bounded output does not imply bounded state: Top-K over a general retract stream
may expose only K rows while retaining unbounded internal history.

## Determinism and null extension

`replay_deterministic` may be claimed only when replay and input permutation
produce the same consolidated state and output. Top-K and ranking require a
strict total order; arrival order and hash-map iteration are not tie breakers.
Outer joins must recompute nullability for the null-extended side and should
drop candidate-key guarantees unless uniqueness is proved after null extension.
Their output changelog is `general_retract`: a first match retracts the prior
null-extended row, and removal of the last match inserts it again. Consequently,
an unbounded outer join cannot satisfy an append-only or final-only consumer
without a separate progress proof and closing transformation.

V1 has no closing/finalization transformation and no implicit conversion from
processing progress to finality. Therefore an append-only input requirement on
an outer-join edge fails capability validation. A future exception must be an
explicit bounded or watermark-progress operator whose output independently
guarantees append-only changes; changing the consumer declaration is not proof.

## Versioning and rollout

The operator contract has its own version. It is a required field of logical
plan wire version 2 with capability version
`velorix-logical-view-capabilities-v2`. Admission rejects wire-version-1 plans
and serialized plans missing the contract; it never synthesizes permissive
default capabilities.

The V1 Rust types and compatibility validator live in
`velorix_core::operator_contract`. Logical-plan finalization derives the complete
contract from admitted nodes and the selected execution family. Validation
derives it again and requires an exact match before runtime construction, in
addition to checking every port-to-port edge and rejecting disconnected nodes.

## Native execution and recovery contract

`velorix_core::native_operator` provides the first common physical execution
primitive for filter, project, aggregate, Top-K, binary inner equi-join, and
left equi-join nodes.
Nodes accept signed `DeltaBatch` inputs on named ports and emit signed deltas;
edges route one node's consolidated output to a consumer port. Top-K receives a
planner-compiled byte order key and adds the canonical record encoding as its
stable tie breaker, so it does not depend on arrival or hash iteration order.

`NativeOperatorGraph::apply_epoch` sorts external inputs, requires a strictly
increasing logical epoch, runs the acyclic graph, and consolidates each sink's
output. Operator expressions use non-stateful `Fn` callbacks. A failed node
application restores the complete pre-epoch graph checkpoint, including its
logical epoch, before returning the original error.

All six node kinds use `NativeOperatorGraphCheckpointV1`. The envelope carries
the schema version, logical epoch, node identity, node-local codec identity and
version, and stateless, unary, or binary signed state. Restore requires the
checkpoint operator set to match the graph, validates every node codec and
state shape, and rolls back the complete graph if any node fails validation.
The wire form is serde round-trip tested. Stateful node restore also constructs
and validates replacement state before committing it.

The admitted plan separately persists a versioned execution implementation
identity, state-codec identity, checkpoint-manifest version, output-codec
identity, durable-output-publication protocol identity, and physical DAG hash.
The physical identity hashes all of those physical boundaries together with the
typed nodes, edge contract, state requirements, and execution lowering. Join
admission distinguishes keyed inner aggregate, general aggregate, and
narrow-left specializations. Plan validation re-derives this identity and fails
closed on a missing or changed selector; restore cannot silently choose a
different implementation.

Keyed inner sum/count, left-only narrow-left sum/count, and general inner
aggregate-join specializations now have differential generic-DAG evidence at
the consolidated-delta, canonical-state, checkpoint, restore, and
continued-tail boundaries. General coverage includes aggregate filters, input
expressions on either side, right-side aggregates, count-distinct, min/max/avg,
HAVING, and Top-K publication. The comparison graph is isolated and never owns
production publication; the persisted specialization remains authoritative.

Join planning uses `lower_join_chain_to_binary_dag`. It folds an ordered list of
join steps into a left-deep chain of ordinary `InnerEquiJoin` or `LeftEquiJoin`
nodes, so adding a third or later input does not introduce an N-way logical or
physical operator. The admitted two-input SQL path uses the same fold with one
step. Public N-way SQL remains fail closed until the general-join phase supplies
catalog binding, semantic join-order proofs, runtime construction, and end-to-end
evidence; this Foundation item fixes the required lowering shape only.

`RIGHT [OUTER] JOIN` does not have a logical node or runtime state machine.
Admission swaps its SQL operands, aliases, source predicates, and key-column
bindings, then applies the narrow-left rules. Output bindings retain the SQL
right-side column identities. Thus unmatched-row maintenance, checkpointing,
and replay use the existing `LeftEquiJoin` path.

There is likewise no subquery logical or physical operator. Identity, direct
projection, and filter-only CTE/derived sources are admitted only after they
have been inlined into the existing scan/filter/project graph. Scalar,
correlated, aggregate-derived, and otherwise undecorrelated subqueries fail
closed. A future decorrelator may broaden SQL only by producing the same typed
relational nodes and passing the complete edge-capability validator.
