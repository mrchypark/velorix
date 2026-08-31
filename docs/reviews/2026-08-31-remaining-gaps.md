# Velorix Remaining Gaps Audit — 2026-08-31

## 1. 신규 모듈 미통합 (가장 큰 격차)

**16개 신규 모듈이 모두 standalone 상태** — 실제 runtime/storage 코드에 통합되지 않음.

| 모듈 | 상태 | 필요한 통합 |
|------|------|-------------|
| `epoch_overlay.rs` | standalone | `apply_view_change`에서 full clone 대신 overlay 사용 |
| `recursive_frontier.rs` | standalone | `recompute_closure_from`에서 frontier 기반 evaluation |
| `join_index.rs` | standalone | `TwoInputJoinRuntime`에서 per-key index 사용 |
| `window_partition_state.rs` | standalone | `AnalyticWindowFrameRuntime`에서 partition state 사용 |
| `arrow_batch_operator.rs` | standalone | filter/project 경로에서 JSON 대신 Arrow 사용 |
| `compiled_expression.rs` | standalone | expression 평가에서 compiled expr 사용 |
| `json_api_boundary.rs` | standalone | API 경계에서만 JSON 사용 강제 |
| `hot_path_metrics.rs` | standalone | object store 호출 시 metrics 기록 |
| `upload_receipt.rs` | standalone | PUT 후 receipt 저장, HEAD 대체 |
| `partition_owner_head.rs` | standalone | ownership/head 관리에서 CAS 사용 |
| `compressed_chunk.rs` | standalone | output persistence에서 256행 대신 chunk 사용 |
| `reachability_gc.rs` | standalone | GC에서 hash 대신 reachability 사용 |
| `checkpoint_compaction.rs` | standalone | checkpoint compaction에서 plan 사용 |
| `collision_audit.rs` | standalone | v1 encoding 감사 도구 |
| `state_replay_plan.rs` | standalone | recovery에서 replay 계획 사용 |

## 2. 누락된 테스트

| 모듈 | 현재 테스트 | 필요한 테스트 |
|------|-------------|---------------|
| `epoch_overlay.rs` | 8개 | nested COW 성능 테스트, concurrent access 테스트 |
| `recursive_frontier.rs` | 7개 | negative weight handling, convergence 테스트 |
| `join_index.rs` | 4개 | semi/anti join, temporal join index 테스트 |
| `window_partition_state.rs` | 6개 | partition boundary, frame calculation 테스트 |
| `arrow_batch_operator.rs` | 4개 | large batch, mixed types 테스트 |
| `compiled_expression.rs` | 6개 | complex expression tree, error cases 테스트 |
| `json_api_boundary.rs` | 6개 | nested object, array validation 테스트 |
| `reachability_gc.rs` | 5개 | concurrent modification, cycle detection 테스트 |
| `compressed_chunk.rs` | 4개 | compression ratio edge cases 테스트 |
| `hot_path_metrics.rs` | 3개 | concurrent counter 테스트 |

## 3. 회귀 위험

### 정확성 회귀
- `join_index.rs`: `apply_left_delta`가 weight를 합산하지 않고 push만 함 → multiset 의미론 위반 가능
- `window_partition_state.rs`: compact가 weight==0만 제거 → net-zero pair 보존
- `recursive_frontier.rs`: frontier advance 후에도 all에 행이 남음 → 메모리 누수 가능

### 성능 회귀
- `compressed_chunk.rs`: `compression_ratio: f64` → `Eq` 불가로 `PartialEq`만 구현
- `log.rs` cache: `Arc<[T]>` 전환 시 `cache_committed`에서 매번 전체 복사 → append 비용 증가

## 4. 아키텍처 격차

### 미해결 P0
| 항목 | 상태 |
|------|------|
| TypedBinaryKey collision | v2 codec 추가됨, 기존 v1 경로 미전환 |
| Runtime rollback 불완전 | ScalarAggregateFilter, AnalyticWindow만 수정, 나머지 미해결 |
| Temporal eviction floor predecessor | orphan 분기만 수정, left-exists 분기 미검증 |

### 미해결 P1
| 항목 | 상태 |
|------|------|
| manifest v2 relation identity writer | 구조만 추가, production writer 미구현 |
| gRPC/Hiqlite atomic commit | proto에 RPC 없음 |
| Query concurrency limit | 기본값 변경됨, production validation 미적용 |
| Idempotency durable source range | in-memory 1024개만 |

### 구조적 이슈
| 항목 | 상태 |
|------|------|
| EpochOverlay → 실제 runtime 미연결 | standalone |
| RecursiveFrontier → recursive_fixpoint 미연결 | standalone |
| JoinIndex → two_input_join 미연결 | standalone |
| WindowPartitionState → analytic_window_frame 미연결 | standalone |
| ArrowBatchOperator → filter/project 미연결 | standalone |

## 5. 우선순위별行动计划

### 즉시 수정 (이번 세션에서)
1. `epoch_overlay.rs`를 `apply_view_change`에 통합
2. `recursive_frontier.rs`를 `recompute_closure_from`에 통합
3. `join_index.rs`를 `TwoInputJoinRuntime`에 통합
4. 회귀 테스트 추가 (multiset weight, partition frame, frontier convergence)

### 단기 (다음 세션)
5. `window_partition_state.rs`를 `AnalyticWindowFrameRuntime`에 통합
6. `arrow_batch_operator.rs`를 filter/project에 통합
7. manifest v2 relation identity writer 구현
8. `hot_path_metrics.rs`를 object store 호출에 통합

### 중기
9. gRPC/Hiqlite atomic commit proto 추가
10. Query concurrency limit production validation
11. Idempotency durable source range
12. Cache deep clone 완전 제거
