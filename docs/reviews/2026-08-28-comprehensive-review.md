# Velorix Comprehensive Code Review — 2026-08-28

Review SHA: `c86c5299e5f0a158f68c29fd1daaecb18442623e`

## 핵심 발견

### P0: Key/Value encoding 충돌
- `encode_kv_ordered`가 type tag, length framing 없이 key+value를 단순 연결
- `(key=1, value=23)`과 `(key=12, value=3)`이 같은 `"123"` 생성 가능
- 서로 다른 행이 조용히 합쳐지고 weight가 합산됨
- 수정: v2 self-delimiting typed codec 필요

### 구조적 문제
1. **전체 상태 clone**: epoch마다 operator graph 전체 복제 → O(S) 비용
2. **전체 상태 재계산**: delta 크기와 무관한 recompute → O(S) 레이턴시
3. **S3 LIST 비용**: append/fencing이 역사 길이 H에 비례 → O(H) 증가
4. **256행 JSON page**: 매 epoch 전체 output 재작성 → 막대한 PUT 수
5. **Checkpoint/query 결합**: 동일 리소스에서 경쟁

## 수정 단계

### Phase 0 (즉시)
- [x] v2 self-delimiting key codec
- [x] injectivity property test
- [ ] raw log collision audit
- [ ] state replay 계획

### Phase 1 (낮은 위험, 큰 효과)
- [x] S3 bounded concurrency (buffer_unordered)
- [x] manifest validation typed contract (ValidatedCheckpointManifest)
- [x] checkpoint size limit (16 MiB)
- [ ] cache deep clone 제거 (`Vec<T>` → `Arc<[T]>`)
- [ ] upload receipt 전달 (중복 HEAD 제거)
- [ ] 256행 page → compressed byte-size chunk

### Phase 2 (S3 비용 구조)
- [x] compact metadata index (PartitionAdmissionHead)
- [x] content-addressed output chunk (ObjectKey::output_chunk)
- [ ] partition/owner head with CAS
- [ ] hot path LIST 금지

### Phase 3 (Incremental engine)
- [x] EpochOverlay write-set/COW (lazy per-key COW)
- [x] RecursiveFrontier semi-naive evaluation (all/delta/next_delta)
- [x] JoinIndex per-key indexed state (equi-join)
- [x] WindowPartitionState per-partition sorted Top-K/window

### Phase 4 (Typed/vectorized)
- [ ] Arrow RecordBatch operator
- [ ] compiled expression
- [ ] JSON API boundary화

### Phase 5 (Recovery/GC)
- [x] replay byte limit (checkpoint size limit)
- [ ] manifest tree/catalog
- [ ] reachability GC
- [ ] checkpoint compaction

## 합격 기준

| 경로 | 현재 | 목표 |
|------|------|------|
| aggregate | 전체 state clone | touched key에 비례 |
| append | history LIST/GET | LIST 0, 원격 요청 3~5회 |
| ownership | 전체 claim scan | owner head 1회 |
| checkpoint | 전체 output rewrite | changed chunk만 PUT |
| output | 256행 JSON | 압축 4~16 MiB chunk |
| RSS | 다중 state 복사 | resident state × 1.3 |
| recovery | history 전체 메모리 | replay byte 상한 |
| idempotency | 최근 1024개 | durable source range |
