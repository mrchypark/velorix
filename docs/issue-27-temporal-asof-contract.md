# Temporal ASOF join contract

Velorix admits only a deliberately narrow temporal join shape:

```sql
SELECT l.ride_id, l.booking_start, l.event_time,
       p.price, p.event_time AS price_event_time
FROM rides l
ASOF JOIN prices p
MATCH_CONDITION (p.event_time <= l.event_time)
ON l.ride_id = p.vehicle_id
WHERE p.vehicle_id IS NOT NULL
```

Admission requires the explicit SQL `ASOF JOIN` AST form, one normalized
`MATCH_CONDITION` expressing right event time `<=` left event time, and one
`ON` equality between the two single, non-nullable primary keys. The flipped
operand/operator form is accepted when it is exactly equivalent. The explicit
`WHERE right_pk IS NOT NULL` removes unmatched-row null padding, keeping the
runtime's matched-row output contract explicit.

Composite keys, missing or compound `ON` predicates, `USING`/`NATURAL` joins,
ordinary `INNER JOIN` temporal inequalities, nullable keys/event-time columns,
incompatible key or event-time physical types, nullable projections, extra
`WHERE` predicates, and non-event-time conditions fail closed. Temporal output
columns are direct non-nullable source columns only.

Only a single UTF-8 primary key is supported because the runtime join index uses
canonical string keys; non-UTF-8 primary-key representations are rejected.

At each right-side floor event time, the runtime chooses one deterministic
winner: the lexicographically smallest canonical JSON row. Right-side
multiplicity does not multiply output weight; left-side multiplicity is
preserved. Restored checkpoints revalidate the explicit SQL contract and
compiled plan, so older ambiguous temporal checkpoints require a full replay;
no automatic migration is claimed.

This syntax follows the explicit `ASOF JOIN ... MATCH_CONDITION` form described
by [Snowflake's ASOF JOIN documentation](https://docs.snowflake.com/en/sql-reference/constructs/asof-join).
