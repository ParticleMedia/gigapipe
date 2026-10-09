# Opt-in fingerprint routing to local ClickHouse tables

## Overview

Add an **opt-in writer mode** that partitions rows by `uint64(fingerprint) % uint64(shard_num)` and inserts into the selected ClickHouse shard's **local** tables instead of asking ClickHouse Distributed tables to choose a shard. Here `shard_num` in the formula means the **count of shards**, not ClickHouse's per-shard `shard_num` identifier: zero-based remainder `i` maps to ClickHouse shard `i+1`. The existing distributed-table write path remains the default. No promise is made to preserve the current ClickHouse sharding order or placement of existing rows. Keep read-side distributed tables and schema creation unchanged; a reader must still scan all shards to see old and new rows.

Scope this first implementation to the fingerprint-bearing log/metric time-series, sample, and pattern writes. Traces and profiles are explicitly called out below because their current keys and table topology are different; **do not silently write these to a randomly selected local table**. In local mode, trace writes can continue using Distributed tables until a trace-key routing design is approved; profiles already insert into a local input table.

## Existing path and design constraints

- `cmd/gigapipe/main.go:84-115` creates only one `DATABASE_DATA` entry from the usual `CLICKHOUSE_*` environment variables, even with `CLUSTER_NAME`. Thus this feature needs shard endpoints discovered from `system.clusters`, or an explicit per-shard config; it cannot infer a shard endpoint from the current single connection.
- `writer/plugin/qryn_writer_db.go:31-69,114-231` creates connections and the per-node write services. `writer/service/registry/static.go:32-103` currently chooses among services without a stable shard order. An indexed shard-to-service mapping must not be built from Go map iteration or from availability/random selection.
- `writer/controller/middleware.go:166-279` chooses a node and services **before** parsing. `writer/controller/builder.go:201-240` parses batches and pushes whole time-series/sample requests to these services. Fingerprints are available in parallel row slices after parsing (`writer/model/insert_request.go:158-190`); partition the parsed rows there before enqueueing, keeping matching time-series and samples on the same shard. Preserve the per-node time-series flush-before-samples behavior from `writer/plugin/qryn_writer_db.go:125-151`.
- `writer/service/insert/time_series.go`, `samples.go`, `metrics.go`, and `pattern.go` select `_dist` tables with `ClusterName` set; `writer/service/generic_insert.go:249-310` sends the buffered insert query. Use a per-service table choice, not a global text rewrite, so legacy and local paths can coexist.
- `writer/pattern/controller/controller.go:132-177` generates pattern writes through its own service selection; it must use the same fingerprint mapping in local mode. `writer/service/insert/tempo.go` and `ctrl/qryn/sql/traces_dist.sql` use a trace-based sharding key rather than the time-series fingerprint. `writer/service/insert/profile.go` already inserts into `profiles_input`, which materialized views distribute downstream (`ctrl/qryn/sql/profiles.sql`).

## Files to Change

| Path | Change |
| --- | --- |
| `cmd/gigapipe/main.go` | Read a new opt-in Boolean environment variable, proposed `CLICKHOUSE_LOCAL_SHARD_WRITES`, validate its value, and pass it into writer startup without changing reader defaults. Keep the normal single-node environment configuration functional. |
| `writer/plugin/qryn_writer_db.go` | At writer startup, resolve and validate a stable, complete index of shard endpoints for the configured `CLUSTER_NAME`; initialize a matching local-table service set and propagate it to routing. Preserve existing service setup when disabled and per-shard time-series-before-samples flush. Fail startup in opt-in mode if a required shard cannot be reached; never silently route to another shard. |
| `writer/plugin/utils.go` | Reuse or extend the existing `system.clusters` lookup (`getShardsNum` at lines 136-153) to obtain endpoint, shard number and replica details instead of only a count. Validate contiguous unique `1..N` shard identifiers, `N > 0`, and a deterministic replica choice per shard. |
| `writer/service/registry/static.go` | Expose explicit index-based access to each shard's services; leave existing random/default routing unchanged for normal mode. Do not rely on map-to-slice conversion for shard positions. |
| `writer/controller/middleware.go` | In opt-in mode, do not pin fingerprint-bearing batches to a random node before parsing; keep legacy routing and unrelated request paths unchanged. |
| `writer/controller/builder.go` | After parsing, split fingerprint-bearing time-series and sample rows into per-shard requests, route each nonempty partition to its matching shard services, collect **all** promises/errors, and preserve tenant (`MOid`) and row-aligned fields. Keep the parser's fingerprint-cache usage safe when it no longer has one random node. |
| `writer/service/insert/time_series.go` | Select `time_series` instead of `time_series_dist` for local-mode services. |
| `writer/service/insert/samples.go` | Select `samples_v3` instead of `samples_v3_dist` for local-mode services. |
| `writer/service/insert/metrics.go` | Use the local target for the metric insert service and preserve its row format/ordering behavior. |
| `writer/service/insert/pattern.go` | Select local `patterns` for local-mode services. |
| `writer/pattern/controller/controller.go` | Route each generated fingerprint-bearing pattern row to its deterministic shard rather than a random service in local mode. |
| `docs/configuration.md` | Describe the opt-in variable, exact modulo mapping, shard-endpoint discovery/replica assumptions, default fallback, partial-failure behavior, and effects of changing shard count or toggling modes. |

If an existing options type must carry the table selection, extend `writer/model/insert_request.go`'s `InsertServiceOpts` rather than adding a package-global setting. If startup needs to pass opt-in configuration from another verified writer entrypoint, update that call site as part of the `writer/plugin/qryn_writer_db.go` change; avoid changing unrelated reader code.

## Files to Create

| Path | Purpose |
| --- | --- |
| `writer/service/shard_routing.go` | Small pure helper for `fingerprint % shardCount`, validation of the ordered shard mapping, and reusable row-partition helpers (or place next to builder if that proves smaller during implementation). |
| `writer/service/shard_routing_test.go` | Unit tests for deterministic mapping, zero/invalid shard counts, uneven/empty partitions and row-field alignment. |
| `writer/controller/builder_sharding_test.go` | Unit tests for post-parse time-series/sample partitioning, tenant propagation, multiple shard promises, and failure handling. |

Prefer fewer new files if existing packages already provide clean homes; do not add a dependency just to implement modulo routing. For end-to-end verification, adapt a test fixture only if it truly provides multiple distinct ClickHouse shards: `test/federated-integration/clickhouse/config.d/cluster.xml` is **one shard/one replica** despite its cluster name.

## Implementation Steps

1. Define `CLICKHOUSE_LOCAL_SHARD_WRITES` as disabled by default and reject invalid Boolean values. Require `CLUSTER_NAME` when enabled and document that only writer modes use it.
2. Discover `system.clusters` rows for the named cluster through the existing ClickHouse connection. Order by numerical `shard_num`; validate a contiguous `1..N` set, a usable host/port for each shard, and one deterministic replica per shard (e.g. lowest `replica_num`). Build/authenticate connections with existing ClickHouse connection settings. Fail fast on incomplete/unreachable topology rather than falling back to Distributed writes. A rolling replica failure must surface a write error rather than reroute a fingerprint.
3. Construct an indexed shard service bundle from the resolved endpoints. Feed each bundle its own connection factory and local-table insert names; keep legacy services and selection untouched when the flag is off. Avoid separate cache/node state per shard when not needed to calculate an already-parsed fingerprint.
4. Implement a minimal, testable `shardIndex(fp, N) = int(fp % uint64(N))` helper. Never hash a label string again or use a random node for fingerprint-bearing rows in this mode.
5. Partition each parsed `TimeSeriesData` and `TimeSamplesData` by the corresponding `MFingerprint` element, copying **every** aligned column for that row and the request-level `MOid`/metadata as appropriate. Send one nonempty request per shard/service; keep each shard's time-series flush-before-samples behavior and wait on every queued write, including when one fails. Reject malformed parallel slices rather than dropping or misplacing rows.
6. Make the pattern path use the same shard index and local `patterns` table. Preserve non-fingerprint trace routing through its current Distributed tables and the existing `profiles_input` materialized-view path; no trace-local claim in documentation.
7. Add focused unit tests and a genuine two-shard ClickHouse integration test/fixture with distinguishable shard-local row counts. Exercise mixed fingerprints, all affected row types, multi-tenant requests, legacy mode, and one unavailable shard. Verify reads through existing distributed read tables.
8. Update configuration documentation and rollout notes: enable on one writer only after all writers share topology and routing settings; monitor per-shard inserts and failures; changing the shard count changes modulo placement and requires an intentional migration/rebalancing plan.

## Dependencies

- No new Go dependencies; use the existing ClickHouse clients and configuration types.
- Opt-in requires the writer to reach **one writable ClickHouse endpoint per shard** and to query `system.clusters`; credentials and transport settings must work on those endpoints. If replica selection or access differs by shard, an explicit endpoint mapping may be necessary instead of discovery.
- A real two-shard ClickHouse environment is required for meaningful routing validation; the current federated fixture is only one shard.

## Testing & Validation

1. `go test ./writer/... ./cmd/gigapipe/...` for routing, partition, config and service tests; then `go test ./...` (the `.github/workflows/go.yml` unit-test command).
2. `go build -o gigapipe cmd/gigapipe/main.go` (or CI's `CGO_ENABLED=0 go build -ldflags="-extldflags=-static" -o gigapipe cmd/gigapipe/main.go`). Run `gofmt` on modified Go files and check `git diff --check`.
3. Run the integration suite if ClickHouse/Docker is available: `./test/integration/run.sh` (builds a local stack and executes `go test -tags integration -count=1 -v ./test/integration/...`). This is a regression test, **not** by itself proof of multi-shard routing.
4. In a **two-shard** cluster, start with the flag off and verify inserts target `_dist`. Start with it on and check `SELECT shard_num, host_name FROM system.clusters WHERE cluster = '<cluster>'`, then inspect each shard's local `time_series`, `samples_v3`, and `patterns`: every inserted fingerprint must be on shard `(fingerprint % N)+1` only, with expected tenant and complete row count. Verify reader queries still see the records across shards.
5. Make one shard unavailable: opt-in startup/insert must fail visibly and must not place its rows on another shard. Re-enable it and check successful writes. Verify trace/profile ingest still works through their existing paths; repeat in legacy mode to guard default behavior.

## Rollback Plan

Disable `CLICKHOUSE_LOCAL_SHARD_WRITES` and restart writers to restore the existing Distributed-table path. Keep distributed tables and read topology intact. Already-written local rows remain where placed and should remain queryable via distributed reads; disabling the flag **does not move data** or restore the previous physical placement. If the feature code itself must be removed, revert the feature commit after restoring the default setting.

## Open Questions

1. **Scope:** This proposal routes fingerprint-bearing time-series, samples and patterns locally; should traces also bypass Distributed tables? They have trace IDs, not the same fingerprint, and require a separately specified deterministic key. Profiles already use a local `profiles_input` MV path.
2. **Topology:** Should `system.clusters` discovery (deterministic first replica, using inherited credentials/transport) be authoritative, or should operators explicitly configure one endpoint per shard? The current env path defines only one database node.
3. **Partial failure:** Proposed behavior is fail closed with an error and no fallback. Because a multi-shard batch can partially succeed before another shard fails, retrying a full batch may duplicate rows; is that acceptable for the existing ingestion semantics, or is a stronger replay/idempotency contract needed before rollout?
4. **Existing data:** This simple modulo router intentionally does not match ClickHouse's existing sharding expressions. Confirm that mixed placement across old/new writes and any future shard-count change will be handled operationally, not by migration logic in this feature.
