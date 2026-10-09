# Loki local-shard write plan

## Overview

Add an opt-in mode **only for Loki log ingestion at `POST /loki/api/v1/push`**. Partition parsed `time_series` and `samples_v3` rows by `shardIndex = fingerprint % shardCount` and insert into the selected shard's local tables. Keep distributed writes as the default. Other ingestion endpoints, log-pattern side effects, reads, and schemas retain their existing behavior. The zero-based index maps to ClickHouse `shard_num = shardIndex + 1`. The new mapping need not preserve historical data placement.

Discover a shard/replica mapping using a new `KEEPER_SERVER` connection; resolve all A/AAAA answers for `CLICKHOUSE_SERVER`, match their normalized IP addresses against replica endpoints recorded in that mapping, and watch for changes. **Prerequisite:** verify the actual production metadata first. This repo's ClickHouse cluster hosts are configured in `<remote_servers>` independently of Keeper (`test/federated-integration/clickhouse/config.d/cluster.xml:18-65`); it does not establish a Keeper znode tree that authoritatively records cluster membership. Keeper used for DDL/replication does not imply it holds distributed topology. If no such metadata/producer and ClickHouse synchronization contract exist, pause implementation and establish them; a Keeper watch cannot detect changes to unrelated ClickHouse XML by itself.

## Files to Change

| Path | Change |
| --- | --- |
| `cmd/gigapipe/main.go` | Parse disabled-by-default `CLICKHOUSE_LOCAL_SHARD_WRITES` and `KEEPER_SERVER`; require `CLUSTER_NAME`, `CLICKHOUSE_SERVER`, and Keeper metadata when enabled. Current env setup creates just one database node (`:84-115`). |
| `writer/plugin/qryn_writer.go` | Own Loki-only topology watcher/client lifecycle, startup readiness and shutdown; preserve existing services for non-Loki endpoints. |
| `writer/plugin/qryn_writer_db.go` | Create and retire indexed per-shard local-series and local-samples services with direct shard connections; preserve default service maps and per-shard series-before-samples flush (`:125-151`). |
| `writer/controller/insert.go` | Gate new routing at `PushStreamV2` only (`:48-59`) for JSON and protobuf Loki pushes. The route is registered at `writer/router/insert.go:8`. |
| `writer/controller/middleware.go` | Add a Loki-specific marker/selection bypass so only opted-in Loki requests avoid random pre-parse service selection (`:227-253`); leave shared middleware for all other endpoints unchanged. |
| `writer/controller/builder.go` | On the Loki opt-in branch of shared `doParse` (`:201-240`), split row-aligned series and sample requests after parsing; pin one snapshot for each request and await every shard result. Leave log-pattern side effects (`:197-199,230-232`) on their existing Distributed path. |
| `writer/service/insert/time_series.go` | Per-service option to insert Loki-routed rows into local `time_series` instead of `_dist` (`:59-75`); keep normal clustered behavior. |
| `writer/service/insert/samples.go` | Per-service option to insert Loki-routed rows into local `samples_v3` instead of `_dist` (`:61-78`); keep normal clustered behavior. |
| `writer/model/insert_request.go` | Extend `InsertServiceOpts` for the local-table choice if necessary; keep current fingerprint-bearing row models (`:158-190`). |
| `docs/configuration.md` | Document flags, Keeper topology contract, DNS/IP cross-check, watch recovery, scaling cutover, mixed placement and rollback. |

Do not change `writer/service/insert/{metrics,pattern,tempo,profile}.go`, read paths, SQL schemas, or non-Loki handlers. `writer/service/registry/static.go` currently converts maps to an unordered/random selection (`:32-103`); build a **separate indexed Loki shard router** rather than altering its global behavior. `writer/plugin/utils.go:136-153` counts shards from `system.clusters`; query it independently to validate the Keeper mapping, not as a substitute for an authoritative Keeper metadata contract.

## Files to Create

| Path | Purpose |
| --- | --- |
| `writer/plugin/keeper_topology.go` | Keeper client/watch, topology decoding, DNS reconciliation, ClickHouse `system.clusters` cross-check, immutable snapshots and atomic swap. Path/schema must follow verified production metadata, not a guessed znode. |
| `writer/plugin/keeper_topology_test.go` | Fake Keeper and DNS tests for watches, reconnect, mapping ambiguity, readiness, and snapshot swaps. |
| `writer/controller/loki_sharding.go` | Pure modulo helper and post-parse partitioner for aligned Loki series/sample arrays. |
| `writer/controller/loki_sharding_test.go` | JSON/protobuf Loki routing, tenant/metadata preservation, errors, and concurrent-swap tests. |

Add a two-shard integration fixture only after checking whether `scripts/test/e2e/clickhouse/config.xml:707-724` supports the verified Keeper metadata schema; the federated integration fixture is one shard.

## Implementation Steps

1. Inspect real production Keeper metadata with an operator. Record exact cluster-specific znode root, child and payload schema (shard ID, replica ID, host/IP and port), write ownership, ACL/auth/TLS, consistency/version rules, and how `<remote_servers>` and `system.clusters` synchronize with it. **Block implementation if absent or independent**; choose an explicit metadata publisher/config reload contract or revise the discovery design with the user.
2. Parse and validate opt-in configuration, and start a topology watcher only for Loki local writes. A missing/invalid initial snapshot means Loki local mode does not become ready; never silently fall back to Distributed writes under the opt-in flag.
3. Read a complete version-consistent Keeper snapshot. Watch relevant child/data znodes; re-arm one-shot watches after changes and session reconnects, with bounded retry/backoff and clean shutdown. Never publish a partial snapshot.
4. Resolve every A/AAAA answer of `CLICKHOUSE_SERVER` and Keeper replica hostnames; normalize and unambiguously match DNS IPs to replica IPs/shards. DNS is an eligibility/cross-check, **not proof of the complete shard set**. Reject unknown, duplicate or ambiguous IP mappings and missing shards; determine a stable eligible replica per shard. If DNS returns a load balancer VIP instead of replica IPs, require an explicit mapping contract rather than pretending they match.
5. Compare the candidate's complete shard/replica set with ClickHouse `system.clusters` for `CLUSTER_NAME`, verify direct endpoint reachability and local tables, construct per-shard local services, then atomically swap the immutable indexed snapshot. Preserve healthy last-known-good routing for transient watch/DNS errors; if it becomes unsafe, reject Loki pushes rather than choose another shard.
6. Make `PushStreamV2` alone pin one snapshot for the entire request. Reuse already-calculated `MFingerprint` values (`writer/model/insert_request.go:158-190`; calculated in `writer/utils/unmarshal/builder.go:309-389`), partition **every aligned field** in `TimeSeriesData` and `TimeSamplesData` and copy tenant `MOid`/metadata. Send each nonempty partition to index `fp % uint64(N)`, preserve same-shard time-series flush before samples, and wait on all promises even if one fails. Leave async patterns written via `patterns_dist` and all other routes unchanged.
7. Test a two-shard deployment, Keeper child/data changes, replica addition, DNS changes, session reconnect and concurrent snapshot updates. **Do not automatically activate a changed shard count** on a watch event: modulo changes remap fingerprints. Require a coordinated operator cutover/migration after every writer and ClickHouse distributed reader is ready; replica-only changes may activate after validation.
8. Document enablement, monitoring, shard-count migration and rollback. Avoid adding an unrelated generalized sharding layer.

## Dependencies

- A pinned ZooKeeper/ClickHouse Keeper Go client supporting session management and child/data watches; no direct Keeper client exists in `go.mod` today. Choose it after confirming actual protocol and authentication requirements.
- Authoritative Keeper topology znodes, a metadata producer, a mechanism that updates ClickHouse `<remote_servers>`/`system.clusters`, and read access to both sources. The repo does **not** establish these prerequisites.
- Direct ClickHouse connectivity/credentials for at least one eligible replica per shard; `CLICKHOUSE_SERVER` DNS answers must identify replica IPs (or an explicit mapping must be agreed).
- A genuine two-shard ClickHouse/Keeper test environment; the federated integration fixture alone cannot prove placement.

## Testing & Validation

1. Unit: `go test ./writer/... ./cmd/gigapipe/...` then `go test ./...` (CI `.github/workflows/go.yml`). Test invalid topology, IPv4/IPv6 DNS sets, ambiguous/missing IPs, watch re-arm and reconnect, replica/shard additions, malformed row arrays, tenants, all promises, and request-level snapshot consistency.
2. Build/format: `gofmt` modified Go files; `git diff --check`; `go build -o gigapipe cmd/gigapipe/main.go` (CI also uses `CGO_ENABLED=0 go build -ldflags="-extldflags=-static" -o gigapipe cmd/gigapipe/main.go`).
3. Existing regression stack: `./test/integration/run.sh` if Docker is available; it executes `go test -tags integration -count=1 -v ./test/integration/...`, but does **not** prove multi-shard routing.
4. Two-shard scenario: send JSON and protobuf Loki pushes with fingerprints of different remainders; inspect each shard's local `time_series` and `samples_v3` for exact `(fp % N)+1` placement, row/tenant/metadata counts, and distributed-read visibility. Verify flag-off Loki uses `_dist` and flag-on other endpoints (OTLP, Influx, Datadog, metrics, traces, profiles) plus Loki patterns retain their existing write targets.
5. Fault/scale tests: update Keeper replica metadata and DNS, drop a shard, expire a session, and introduce a new shard. Assert atomic replica update, no incorrect-shard writes or silent fallback, and no shard-count cutover before explicit all-writer/reader convergence. Observe mixed-batch partial successes and errors.

## Rollback Plan

Disable `CLICKHOUSE_LOCAL_SHARD_WRITES` across all writers and restart to restore the existing Loki Distributed-table path. Other endpoints have not changed. Keep historical local rows and distributed/read tables intact; rollback does **not** move data. After a shard-count cutover, coordinate rollback with the same migration/convergence procedure: simply reverting `N` can scatter one fingerprint across shards. A multi-shard request can partially succeed before returning an error, so retries can duplicate rows. Revert the code commit after disabling the feature if necessary.

## Open Questions

1. What is the exact authoritative Keeper znode path/payload/producer for ClickHouse shard and replica membership? If none exists, should operators publish it or should we replace Keeper watching with another approved source of truth?
2. What format, authentication and TLS settings should `KEEPER_SERVER` use? Does `CLICKHOUSE_SERVER` resolve to individual replica IPs, service IPs or a load-balancer VIP? The latter cannot be cross-referenced directly without another mapping.
3. How is `<remote_servers>` reloaded and how can a writer prove **all** distributed readers and writers have converged before using a new shard count? What rebalancing/migration procedure is expected for old fingerprints?
4. Are Loki-derived async patterns allowed to remain on `patterns_dist` as proposed, since only primary Loki log writes (`time_series`/`samples_v3`) are being rerouted?
5. Is partial multi-shard success followed by possible duplicate rows on whole-request retry acceptable under the current ingestion contract?
