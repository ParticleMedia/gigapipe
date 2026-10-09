# Loki local-shard write plan

## Overview

Add an opt-in mode **only for Loki log ingestion at `POST /loki/api/v1/push`**. Partition parsed log `time_series` and `samples_v3` rows by `shardIndex = fingerprint % shardCount`, sending each partition to the corresponding shard's local tables. Distributed-table writes remain the default; all other ingest routes, Loki log-pattern side effects, readers, and schemas retain existing behavior. Index `i` maps to ClickHouse `shard_num = i + 1`; the new mapping need not preserve historical placement.

**Topology source: ClickHouse SQL, not Keeper.** Connect to one randomly selected, reachable ClickHouse seed IP resolved from `CLICKHOUSE_SERVER` (the current endpoint may be reused if reachable). Query `system.clusters` for `CLUSTER_NAME` to fetch the cluster's shard/replica rows, including `shard_num`, `replica_num`, `host_name`, `host_address`, and `port` (verify column availability on the deployed version). Resolve every A/AAAA answer of `CLICKHOUSE_SERVER` and cross-reference those IPs against the addresses of the rows in `system.clusters`. No `KEEPER_SERVER`, ZooKeeper client, znode watch, or new topology metadata table is needed: `system.clusters` is the built-in table populated from ClickHouse's cluster configuration. A SQL query is read-only (`SELECT`, not a DML mutation).

Refresh by bounded periodic SQL polling (and optionally a refresh on failed endpoint connection), because an SQL result cannot itself be watched. Build and validate a complete replacement mapping before an atomic swap. A new replica can become eligible automatically after validation; **a new shard count requires a coordinated cutover**, because `fingerprint % N` remaps existing fingerprints and readers and writers may see different cluster configurations during rollout.

## Existing path and constraints

- `writer/router/insert.go:8` binds Loki push to `writer/controller/insert.go:48-59` (`PushStreamV2`), which runs JSON or protobuf decoding in the shared builder. `writer/controller/middleware.go:227-253` currently picks one node before parsing; `writer/controller/builder.go:201-240` pushes whole parsed batches. Changes must be gated at Loki's handler rather than globally affecting shared middleware/builder.
- `writer/model/insert_request.go:158-190` holds parallel per-row fingerprint and data slices. The fingerprint is already calculated during parsing in `writer/utils/unmarshal/builder.go:309-389`; do not hash again. Per-node series flushes before samples (`writer/plugin/qryn_writer_db.go:125-151`).
- `writer/service/insert/time_series.go:59-75` and `writer/service/insert/samples.go:61-78` choose `_dist` with a cluster configured. `writer/service/registry/static.go:32-103` selects services from an unordered map/randomly; local routing must instead use an indexed, immutable shard-to-service mapping.
- ClickHouse already reads `system.clusters` in `writer/plugin/utils.go:136-153` for the shard count. `cmd/gigapipe/main.go:84-115` configures a single node from env; its `CLICKHOUSE_SERVER` DNS answers are seed candidates, not an ordered shard mapping. ClickHouse cluster hosts in this repository's fixture are configured under `<remote_servers>` (`test/federated-integration/clickhouse/config.d/cluster.xml:53-64`).
- `writer/controller/builder.go:197-199,230-232` asynchronously generates patterns. Leave pattern inserts on the existing `patterns_dist` path for this narrowly scoped change. `ctrl/qryn/sql/log.sql` defines local tables and materialized views; `ctrl/qryn/sql/log_dist.sql:18-33,70-81` defines distributed equivalents.

## Files to Change

| Path | Planned change |
| --- | --- |
| `cmd/gigapipe/main.go` | Parse disabled-by-default `CLICKHOUSE_LOCAL_SHARD_WRITES`; require `CLUSTER_NAME` and `CLICKHOUSE_SERVER` when enabled, with no Keeper settings. Optionally parse a bounded `CLICKHOUSE_SHARD_REFRESH_INTERVAL` (or use a documented fixed interval). |
| `writer/plugin/qryn_writer.go` | Own lifecycle and cleanup for periodic ClickHouse topology refresh and Loki-only direct shard clients/services. Do not change initialization of other ingest routes. |
| `writer/plugin/qryn_writer_db.go` | Create/retire per-shard `time_series` and `samples_v3` insert services from validated indexed topology, preserving series-before-samples flush and default service maps. |
| `writer/controller/insert.go` | Enable routing specifically for opted-in `PushStreamV2`, covering both Loki JSON and protobuf without changing other handlers. |
| `writer/controller/middleware.go` | Add a Loki-specific pre-parse marker/bypass of random node selection; retain shared middleware behavior elsewhere. |
| `writer/controller/builder.go` | Pin one topology snapshot for a Loki request, partition aligned parsed series/sample rows after decode, wait for every shard's write and propagate errors. Leave existing pattern side effect distributed. |
| `writer/service/insert/time_series.go` | Per-service local table choice `time_series` for opted-in Loki only. |
| `writer/service/insert/samples.go` | Per-service local table choice `samples_v3` for opted-in Loki only. |
| `writer/model/insert_request.go` | Extend `InsertServiceOpts` with local table choice if necessary; preserve existing row models and default behavior. |
| `docs/configuration.md` | Document opt-in, SQL discovery from a random reachable seed, DNS reconciliation, refresh interval/failures, scope, scale cutover and rollback. |

Do **not** change the read path, SQL schemas, `writer/service/insert/{metrics,pattern,tempo,profile}.go`, or other ingestion handlers. Keep `writer/service/registry/static.go`'s global behavior intact by using a separate indexed Loki router. The existing `getShardsNum` query in `writer/plugin/utils.go` need not change if the new snapshot query lives in a dedicated file.

## Files to Create

| Path | Purpose |
| --- | --- |
| `writer/plugin/clickhouse_topology.go` | SQL discovery from a randomly chosen resolved seed, DNS/IP reconciliation, topology validation, periodic refresh and atomic immutable Loki snapshot publication. |
| `writer/plugin/clickhouse_topology_test.go` | Fake ClickHouse query and DNS resolver tests for seed fallback, mapping and refresh consistency. |
| `writer/controller/loki_sharding.go` | Pure modulo helper and partitioner for aligned Loki series/sample arrays. |
| `writer/controller/loki_sharding_test.go` | JSON/protobuf Loki routing, row alignment, tenant propagation, error and concurrent snapshot swap tests. |

Only add a two-shard integration fixture after verifying whether `scripts/test/e2e/clickhouse/config.xml:707-724` can be reused; the federated fixture has one shard.

## Implementation Steps

1. Verify deployed ClickHouse `system.clusters` schema and `CLUSTER_NAME` rows with read-only SQL. Identify exactly how `host_name`, `host_address`, `port`, `shard_num`, `replica_num` and optional internal-replication fields represent the target deployment. Confirm `CLICKHOUSE_SERVER` DNS answers refer to replica nodes rather than a load-balancer VIP. If they do not, require an explicit endpoint-to-replica mapping instead of guessing.
2. Parse the opt-in flag. Resolve **all** A/AAAA answers of `CLICKHOUSE_SERVER`, select a seed at random among those candidates (try other seeds on connection/query failure with bounded timeouts), and run `SELECT shard_num, replica_num, host_name, host_address, port FROM system.clusters WHERE cluster = ? ORDER BY shard_num, replica_num` using the existing ClickHouse client. Never interpolate the cluster name into SQL. Do not add Keeper access.
3. Construct a complete mapping of shard `1..N` to replicas. Normalize and cross-reference every seed DNS IP against resolved replica endpoints, reject unknown/ambiguous/duplicate matches, verify every shard has at least one reachable writable replica, and make deterministic replica selection per shard (e.g. lowest eligible `replica_num`). DNS is an eligibility check, **not** proof that all shards are represented; require the SQL rows to define the full set. Verify local tables exist on target nodes. Fail startup for opted-in Loki writes if no valid snapshot; never silently fall back to `_dist` under the opt-in flag.
4. Poll SQL at a bounded configured interval, using a randomly selected reachable seed for each refresh. Re-resolve DNS, build and validate the *entire* candidate mapping off-path, and atomically replace the immutable snapshot after connections and services are ready. Keep the last-known-good snapshot on a transient failed refresh only while its endpoints remain healthy; otherwise reject affected Loki writes. Ensure in-flight requests finish against their pinned snapshot before retiring services. Query different seed nodes in tests to detect configuration divergence; do not activate a candidate from a stale node.
5. For `PushStreamV2` alone, pin a snapshot for the request. Partition **all aligned row fields** in `TimeSeriesData` and `TimeSamplesData` by existing `MFingerprint % uint64(N)` while retaining request-level `MOid` and metadata; reject malformed arrays instead of silently dropping rows. Use the same shard-local service bundle for series and samples, preserve flush ordering, collect/wait every promise even if one fails, and propagate errors. Leave patterns and all other routes on their current services and table targets.
6. On replica-only topology changes, allow validated routing refresh without changing `N`. On shard addition/removal, detect and expose the candidate but **hold the previous active `N`** until an explicit operator-coordinated cutover confirms all writers and distributed readers use converged `system.clusters`; plan rebalancing/historical placement separately. SQL polling detects changes but cannot by itself guarantee every node is converged.
7. Add unit and two-shard integration tests, and document operational enablement, query-based refresh, seed failure behavior, scale cutover and rollback.

## Dependencies

- No new Go or Keeper dependency: use existing ClickHouse clients, standard-library DNS resolution and ticker/timers.
- Read access to `system.clusters` and direct writable connectivity/credentials to one eligible replica for **every** shard. A DNS name resolving to seed replica IPs; if `CLICKHOUSE_SERVER` points at a VIP, agree on an explicit mapping first.
- A two-shard ClickHouse test topology. Polling follows ClickHouse's cluster configuration and can only detect a changed shard after the queried seed has reloaded its `system.clusters` view.

## Testing & Validation

1. `go test ./writer/... ./cmd/gigapipe/...` then `go test ./...` (CI `.github/workflows/go.yml`). Test unordered `system.clusters` rows, multiple A/AAAA answers, seed failover, unknown/ambiguous IPs, invalid/zero/gapped shard IDs, replica addition, divergent/stale seed views, failed refresh, concurrent immutable swaps, row-field alignment, tenant preservation and partial shard failures.
2. `gofmt` modified Go files; `git diff --check`; `go build -o gigapipe cmd/gigapipe/main.go` (CI also runs `CGO_ENABLED=0 go build -ldflags="-extldflags=-static" -o gigapipe cmd/gigapipe/main.go`).
3. `./test/integration/run.sh` with Docker available (executes `go test -tags integration -count=1 -v ./test/integration/...`); this existing one-node suite is a regression check, not proof of shard placement.
4. On two distinct shards, send JSON and protobuf Loki pushes with fingerprints of different modulo remainders. Inspect local `time_series` and `samples_v3` on each shard for `(fp % N)+1` placement, row counts and tenant/metadata; verify unchanged Distributed reads see all rows. Flag off: Loki writes via `_dist`. Flag on: OTLP, Influx, Datadog, metrics, traces, profiles and Loki patterns retain original write targets.
5. Change DNS/seed availability and ClickHouse cluster configuration: verify refresh eventually sees validated replicas, rejects mismatches without incorrect-shard inserts, and holds shard-count changes until coordinated cutover. Include mixed-batch partial-success and retry behavior.

## Rollback Plan

Disable `CLICKHOUSE_LOCAL_SHARD_WRITES` across all writers and restart to restore existing Loki Distributed-table writes. Other routes remain unchanged. Retain local rows and Distributed/read tables; disabling the flag does **not** move historical data. If a shard-count cutover occurred, coordinate rollback with the same migration/convergence procedure: simply restoring old `N` can split fingerprints across shards. Multi-shard requests may partially succeed and a full retry may duplicate rows. Revert code only after disabling the flag.

## Open Questions

1. Does `CLICKHOUSE_SERVER` resolve to individual ClickHouse replica IPs, or to a VIP/service address? If VIPs, what explicit mapping should replace the requested IP cross-reference?
2. Is the deployed ClickHouse version's `system.clusters` schema compatible with the proposed SQL, and do all seed nodes present the same `CLUSTER_NAME` topology after a config change? What is the convergence signal for distributed readers?
3. What polling interval is appropriate, and should a shard-count cutover use an explicit enable/epoch setting? SQL polling alone cannot atomically switch all writers from modulo `N` to `N+1`.
4. Are Loki-derived async patterns allowed to remain on `patterns_dist` as proposed while primary Loki log writes (`time_series` and `samples_v3`) go local?
5. Are possible duplicates on whole-request retry after partial multi-shard success acceptable under the current ingestion contract?
