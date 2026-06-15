# Production Readiness Status

This matrix is the release-status validator input. Each required contract must
be complete with no blocking tasks before the release gate can pass.

| Contract | Evidence | Scope | Status | Blocking tasks |
| --- | --- | --- | --- | --- |
| ingest | REST ingest and object log checks | schema-bound relation appends | complete | none |
| relation catalog | catalog registry checks | durable relation schemas | complete | none |
| object-store capability | startup capability checks | create/read/list authority | complete | none |
| ownership | runtime fencing checks | single active writer/runtime ownership | complete | none |
| checkpoint lifecycle | checkpoint publish/recover checks | restart recovery | complete | none |
| state substrate | state checkpoint checks | materialized view state | complete | none |
| DataFusion policy | query policy checks | bounded SQL/query execution | complete | none |
| table registry | table registry checks | persisted table specs | complete | none |
| materialized view runtime | standing runtime checks | internal jarless materialized views | complete | none |
| benchmark gate | benchmark evidence | release performance guard | complete | none |
| S3-compatible tests | S3-compatible harness | object-store compatibility | complete | none |
| Kubernetes operator | operator startup checks | deployed control-plane lifecycle | complete | none |
| GC | GC evidence | retention and cleanup | complete | none |
| dependency governance | cargo-deny/governance evidence | package policy | complete | none |
