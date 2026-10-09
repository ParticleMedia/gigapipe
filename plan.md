# Loki local-shard read/write plan

## Overview

Make the **Loki log pipeline shard-aware on both write and read**, using one shared ClickHouse topology derived from `system.clusters` so reader and writer always agree on shard layout. For the Loki HTTP endpoints (`/loki/api/v1/push`, `/loki/api/v1/query_range`, `/loki/api/v1/query`, `/loki/api/v1/series`, `/loki/api/v1/label(s)`, `/loki/api/v1/label/{name}/values`, `/loki/api/v1/index/stats`, `/loki/api/v1/tail`), stop using the pre-created `_dist` log tables (`samples_v3_dist`, `time_series_dist`, `time_series_gin_dist`, `metrics_15s_dist`, `patterns_dist`).

- **Write:** partition parsed `time_series` and `samples_v3` rows by `shardIndex = fingerprint % shardCount` and insert into each shard's **local** tables. Index `i` → ClickHouse `shard_num = i+1`.
- **Read:** resolve LogQL queries against the live shard set rather than a static Distributed table, so a topology change is reflected immediately and identically on both sides. Keep ClickHouse responsible for cross-shard scatter/merge/aggregation (see decision below) so LogQL aggregation semantics stay correct.

This is **opt-in** via `CLICKHOUSE_LOCAL_SHARD_WRITES` (name retained for continuity; it now gates the whole Loki shard-aware pipeline, read included). Default off = today's behavior. Only the Loki log pipeline changes; traces, profiles, Prometheus/PromQL, OTLP/Influx/Datadog ingestion, and their read paths keep their existing `_dist` behavior. Historical data placement is not preserved.

## Read-path design decision (core of this revision)

The reader today selects distributed log tables centrally in `reader/utils/tables/tables.go:72-125` (`PopulateTableNames`): in cluster mode it sets `SamplesDistTableName`/`TimeSeriesDistTableName`/`TimeSeriesGinDistTableName`/`Metrics15sDistTableName`/`PatternsDistTable` to the `*_dist` tables and the planners emit `GLOBAL IN` across them (`reader/logql/logql_transpiler/clickhouse_planner/planner_labels_joiner.go:10-30`, `sql_misc.go:241-244`). Replacing the static `_dist` log tables must preserve that scatter/merge/aggregation, because a LogQL `rate(...)`/`sum by(...)` is only correct when samples for a series are aggregated together.

**Recommended approach — live `cluster()` table function.** For Loki log tables in shard-aware mode, resolve the "distributed" name to the ClickHouse `cluster('<CLUSTER_NAME>', '<db>', '<local_table>')` table function (or `clusterAllReplicas` for read spread) instead of a baked `*_dist` table. This:
  - keeps the existing planner, `GLOBAL IN`, and aggregation SQL unchanged (only the source expression differs, set once in `PopulateTableNames`);
  - reads cluster membership live from the same `system.clusters` the writer topology uses, so **no `_dist` table can drift** and reader/writer share one source of truth;
  - needs no `_dist` log tables created for the Loki pipeline.

**Alternative (heavier, documented not chosen by default):** application-side fan-out — the reader opens the same per-shard snapshot, runs the local-table query on every shard, and merges/aggregates in Go. This re-implements ClickHouse's distributed aggregation (partial-aggregate states, `GLOBAL IN` fingerprint windows, label/series dedup, `index/stats` summation, tail multiplexing) and is error-prone for LogQL metric queries. Keep it only if `cluster()` is unavailable or disallowed in the target environment; call it out as an open question rather than building both.

**Open correctness points either way:**
- **`time_series_gin` co-sharding (hard requirement).** LogQL resolves label matchers to a fingerprint set via a `samples.fingerprint IN (SELECT fingerprint FROM time_series_gin/time_series ...)` sub-select (`reader/logql/logql_transpiler/clickhouse_planner/planner_fingerprint_filter.go`). A shard-local join is only correct if `time_series_gin` is sharded by the **same** `fp % N` as `samples_v3`/`time_series`. The write plan currently moves only `time_series` + `samples_v3` local; `time_series_gin` must also be co-sharded (write it local by `fp % N`) or the fingerprint sub-select must run against the cluster-wide source, or rows are silently dropped. `cluster()` on every log source keeps this correct; app-side fan-out would have to co-shard gin explicitly.
- **Non-fingerprint queries.** label/series/`label values`/`detected_*` carry no fingerprint and must read every shard and de-duplicate (`UNION DISTINCT`); `cluster()` handles this, app-side fan-out must implement it.
- **Aggregation state.** `index/stats` uses `uniqExact(fingerprint)` / `uniqExact(fingerprint, day)` (`reader/service/query_range.go:844-849`) which must be **merged, not summed**; `metrics_15s` stores `AggregateFunction`/`SimpleAggregateFunction` state and its local table + MV must exist on every shard. These are trivial under `cluster()` and require `-State`/`-Merge` handling under app-side fan-out.
- **`tail`** polls incrementally (`reader/service/query_range.go:25-27`) and would need per-shard cursors and a continuous merge-sort if fanned out in-app.

These are the reasons the recommended approach keeps ClickHouse's distributed execution via `cluster()` rather than re-implementing it.

## Topology source (shared by reader and writer)

Query `system.clusters` for `CLUSTER_NAME` from a reachable seed resolved from `CLICKHOUSE_SERVER`; it already backs `writer/plugin/utils.go:136-153` (shard count) and is a read-only `SELECT`. Use `SELECT shard_num, replica_num, host_name, host_address, port FROM system.clusters WHERE cluster = ? ORDER BY shard_num, replica_num` (confirm columns on the deployed version; cluster name is always a bound parameter, never interpolated). Resolve all A/AAAA answers of `CLICKHOUSE_SERVER` and cross-reference normalized IPs against the rows as an eligibility check (not proof of the full shard set). No ZooKeeper/Keeper client, znode watch, or `KEEPER_SERVER` setting. Refresh by bounded periodic polling; build and validate a complete mapping, then swap atomically. A replica-only change can refresh automatically after validation; **a shard-count change requires an explicit operator-coordinated cutover** because `fp % N` remaps fingerprints and in-flight readers/writers may observe different `N`. Put this discovery in a package shared by reader and writer (e.g. under `shared/`) so both consume one snapshot and `shardCount` for writes and the same cluster/epoch for reads.

## Existing path and constraints

- Write: `writer/router/insert.go:8` → `writer/controller/insert.go:48-59` (`PushStreamV2`, JSON + protobuf). Fingerprints are parsed into aligned slices (`writer/model/insert_request.go:158-190`, computed in `writer/utils/unmarshal/builder.go:309-389`); do not re-hash. Per-node series flush precedes samples (`writer/plugin/qryn_writer_db.go:125-151`). `_dist` chosen in `writer/service/insert/time_series.go:59-75` and `samples.go:61-78`. Service selection is random/unordered (`writer/service/registry/static.go:32-103`) and must be replaced for Loki by an indexed shard map.
- Read: Loki routes at `reader/router/query_range.go:21-33` and `reader/router/select_labels.go:18-21`. Table selection centralized in `reader/utils/tables/tables.go:72-125`; dist-name consumers include `planner_main_init.go`, `planner_time_series_init.go`, `planner_series.go`, `planner_patterns.go`, `planner_metrics15s_shortcut.go` and `reader/service/{query_range,query_abels,metadata,prom_queryable}.go`. Reader picks one random DB node per request (`reader/registry/static.go:32-37`) and relies on the Distributed engine for fan-out; `suffix` logic lives in `shared/distconfig` and `reader/service/utils.go:8-13`.
- `CLICKHOUSE_READ_DIST_SUFFIX` (`shared/distconfig/distconfig.go`) must stay honored for all non-Loki read paths (traces/profiles/PromQL). Only the Loki log tables switch to the live-topology source. **Feature collision:** the existing cross-cluster read feature (docs "Cross-Cluster Deployment") also rewrites Loki log table names via the suffix; shard-aware Loki reads and a custom `CLICKHOUSE_READ_DIST_SUFFIX` for Loki are mutually exclusive. The plan must define precedence (proposed: shard-aware mode wins for Loki log tables and the suffix is ignored for them, still applying to traces/profiles) and reject a contradictory config at startup.

## Files to Change

### Shared topology
| Path | Change |
| --- | --- |
| `cmd/gigapipe/main.go` | Parse disabled-by-default `CLICKHOUSE_LOCAL_SHARD_WRITES` (now gates reads too) and optional bounded `CLICKHOUSE_SHARD_REFRESH_INTERVAL`; require `CLUSTER_NAME` + `CLICKHOUSE_SERVER` when enabled. Applies to `all`/`reader`/`writer` modes so a split deployment stays consistent. |
| `shared/distconfig/distconfig.go` (or a new `shared/shardtopo` package) | Hold the shared `system.clusters` topology snapshot, DNS reconciliation, validation, periodic refresh, and accessors for `shardCount` + the live cluster/db identifiers used to build `cluster()` sources. Keep `Suffix()` unchanged for non-Loki tables. |

### Write path
| Path | Change |
| --- | --- |
| `writer/plugin/qryn_writer.go` | Own topology-refresh lifecycle and Loki-only per-shard clients/services; leave other routes’ init untouched. |
| `writer/plugin/qryn_writer_db.go` | Build/retire indexed per-shard local `time_series`/`samples_v3` services; preserve series-before-samples flush and default service maps. |
| `writer/controller/insert.go` | Gate shard routing at `PushStreamV2` only (JSON + protobuf). |
| `writer/controller/middleware.go` | Loki-specific pre-parse marker so opted-in Loki skips random node pinning; others unchanged. |
| `writer/controller/builder.go` | Pin one snapshot per request; partition aligned `TimeSeriesData`/`TimeSamplesData` by `fp % N`, route to matching shard services, await all promises, propagate errors; leave pattern side effect as-is (see open question on pattern reads/writes). |
| `writer/service/insert/time_series.go` (gin) | **Co-shard `time_series_gin` by the same `fp % N`** so the reader's fingerprint sub-select is correct, or the read side must source gin cluster-wide. Confirm how gin rows are produced (local MV vs explicit insert) before choosing; this is required for read correctness, not optional. |
| `writer/service/insert/time_series.go`, `writer/service/insert/samples.go` | Per-service local-table target for opted-in Loki; keep normal `_dist` choice otherwise. |
| `writer/model/insert_request.go` | Extend `InsertServiceOpts` with the local-table choice if needed. |

### Read path
| Path | Change |
| --- | --- |
| `reader/utils/tables/tables.go` | In `PopulateTableNames` (lines 72-125), when Loki shard-aware mode is on, set the log dist-name fields (`SamplesDistTableName`, `TimeSeriesDistTableName`, `TimeSeriesGinDistTableName`, `Metrics15sDistTableName`, `PatternsDistTable`) to a live `cluster('<cluster>','<db>','<local>')` source from the shared snapshot instead of `*_dist`. Keep honoring the `plugins.GetTableNamesPlugin()` override hook (`tables.go:62-70`). Leave trace/profile names on `distconfig.Suffix()`. |
| `reader/config` / `reader.go` / `reader/registry/registry.go` | Initialize the shared topology for the reader (it is currently writer-initialized), gate the Loki source swap on the opt-in flag, and ensure `GLOBAL IN` cluster-mode paths still trigger. |
| `docs/configuration.md` | Document the unified opt-in, that Loki read+write bypass `_dist`, the `cluster()` source, refresh/cutover rules, and that `CLICKHOUSE_READ_DIST_SUFFIX` applies only to non-Loki reads. |

Do **not** change trace/profile/PromQL read planners, non-Loki ingestion, or SQL schemas. Reuse the existing planner/`GLOBAL IN` machinery rather than rewriting LogQL SQL generation.

## Files to Create

| Path | Purpose |
| --- | --- |
| `shared/shardtopo/shardtopo.go` (if not folding into distconfig) | Shared topology snapshot, SQL discovery, DNS reconciliation, refresh, `cluster()` source builder. |
| `shared/shardtopo/shardtopo_test.go` | Fake ClickHouse + DNS tests: seed failover, ordering, ambiguous/missing IPs, refresh/atomic swap, shard-count hold. |
| `writer/controller/loki_sharding.go` + `_test.go` | Pure `fp % N` helper and aligned-array partitioner; routing/tenant/error/concurrency tests. |
| `reader/utils/tables/loki_cluster_source_test.go` | Assert Loki log dist-names resolve to the live `cluster()` source (not `*_dist`) in shard-aware mode and to existing behavior when off. |

Add a two-shard integration fixture only after confirming `scripts/test/e2e/clickhouse/config.xml:707-724` (two distinct hosts) is reusable; the federated fixture is one shard.

## Implementation Steps

1. Confirm on the deployed ClickHouse: `system.clusters` columns, that `cluster()`/`clusterAllReplicas()` are permitted, that local `time_series`/`samples_v3`/`metrics_15s`/`time_series_gin`/`patterns` and their materialized views exist on every shard, and whether `CLICKHOUSE_SERVER` resolves to replica IPs or a VIP. Block if `cluster()` is disallowed or metadata is ambiguous.
2. Implement the shared topology package: random reachable seed from resolved `CLICKHOUSE_SERVER`, parameterized `system.clusters` query, complete `1..N` shard/replica mapping with DNS cross-check, deterministic replica choice, bounded periodic refresh, atomic immutable swap, shard-count-hold. Expose `shardCount` and a `cluster()`-source builder.
3. Write path: build indexed per-shard local services; gate `PushStreamV2`; partition aligned rows by `fp % N`; preserve flush order; await all promises. No silent `_dist` fallback under the flag.
4. Read path: in shard-aware mode, point only the Loki log dist-name fields at the live `cluster()` source in `PopulateTableNames`; keep `GLOBAL IN` and all planner logic intact; ensure the reader initializes and refreshes the shared topology. Verify label/series/values, `index/stats`, and `tail` traverse all shards correctly.
5. Keep reader and writer bound to the same snapshot/epoch; on shard-count change, hold routing and read source until an operator-coordinated cutover confirms all nodes converged. Replica-only changes may auto-refresh after validation.
6. Tests: unit (topology, partition, source resolution) + a real two-shard integration asserting write placement at `(fp % N)+1`, LogQL query/aggregation correctness across shards, label/series/stats/tail, and parity between shard-aware reads and the legacy `_dist` path on the same data.
7. Documentation and rollout: enable reader and writer together per cluster; monitor per-shard writes and query correctness; define the shard-count cutover runbook.

## Dependencies

- No new Go/Keeper dependency; existing ClickHouse clients, stdlib DNS, timers. The recommended read design needs ClickHouse `cluster()`/`clusterAllReplicas()` enabled for the service account.
- Read access to `system.clusters`; writable connectivity to one eligible replica per shard for writes; local Loki tables + MVs present on every shard. `CLICKHOUSE_SERVER` must resolve to replica IPs (or an explicit mapping agreed).
- A genuine two-shard ClickHouse test topology; the one-shard fixtures cannot prove placement or cross-shard reads.

## Testing & Validation

1. `go test ./writer/... ./reader/... ./cmd/gigapipe/... ./shared/...` then `go test ./...` (CI `.github/workflows/go.yml`). Cover topology edge cases, write partitioning, and that Loki read names resolve to `cluster()` (not `_dist`) only in shard-aware mode.
2. `gofmt` changed files; `git diff --check`; `go build -o gigapipe cmd/gigapipe/main.go` (CI also `CGO_ENABLED=0 go build -ldflags="-extldflags=-static" ...`).
3. `./test/integration/run.sh` (Docker) as a regression gate (`go test -tags integration -count=1 -v ./test/integration/...`); one node, not proof of sharding.
4. Two-shard manual/integration: push Loki JSON + protobuf with fingerprints of different remainders; assert local `time_series`/`samples_v3` placement at `(fp % N)+1`. Then run LogQL `/loki/api/v1/query_range` metric and log queries, `/series`, `/labels`, `/label/*/values`, `/index/stats`, and `/tail`; compare results and aggregates against the same data served by the legacy `_dist` path to prove correctness. Flag off: Loki uses `_dist` for both read and write; non-Loki signals unchanged in both modes.
5. Topology faults/scale: change DNS/seed availability and cluster config; confirm refresh validates, reader and writer stay in sync, shard-count changes are held until cutover, and no incorrect-shard write or wrong-topology read occurs. Include mixed-batch partial-success/retry behavior.

## Rollback Plan

Disable `CLICKHOUSE_LOCAL_SHARD_WRITES` on all reader and writer instances and restart to restore the legacy `_dist` read+write path for Loki. Non-Loki paths never changed. Local rows and `_dist`/read tables remain intact; disabling does **not** move data. Because reads switch back to `_dist`, previously local-written rows remain visible (same underlying local tables the `_dist` engine spans). After a shard-count cutover, roll back with the same coordinated procedure; reverting `N` alone can split a fingerprint across shards. Multi-shard writes may partially succeed, so retries can duplicate rows. Revert code only after disabling the flag everywhere.

## Open Questions

1. Is the ClickHouse `cluster()`/`clusterAllReplicas()` table function available and permitted in the target deployment? If not, must the reader do app-side fan-out + merge (and is the added aggregation complexity acceptable), or may a live-topology Distributed table be recreated instead?
2. For LogQL metric queries, is `cluster(local_table)` + existing `GLOBAL IN` confirmed to preserve current aggregation results versus `samples_v3_dist`? What is the acceptance test for parity?
3. `metrics_15s` and `patterns`: are their local tables + materialized views present on every shard so the Loki read shortcut and pattern reads work per-shard? Should Loki pattern **writes** also move local (currently proposed to stay on `patterns_dist`), given reads must be consistent?
4. Does `CLICKHOUSE_SERVER` resolve to individual replica IPs or a VIP/service address? A VIP cannot be cross-referenced to replicas without an explicit mapping.
5. Reader/writer convergence: what signal proves every reader and writer sees the same `system.clusters` before a shard-count cutover, and what is the rebalancing/migration plan for historical fingerprints placed under the old `N`?
6. `tail` and `index/stats` across shards: confirm acceptable streaming/summation (`uniqExact` merge) semantics when reading all shards directly rather than one `_dist` table.
7. `time_series_gin` co-sharding: confirm how gin rows are generated and that co-sharding them by `fp % N` (or sourcing gin cluster-wide on read) is acceptable, since the fingerprint sub-select correctness depends on it.
8. Precedence vs `CLICKHOUSE_READ_DIST_SUFFIX` for Loki: confirm shard-aware mode overrides the suffix for Loki log tables and that the combination should be rejected rather than silently one-wins.
