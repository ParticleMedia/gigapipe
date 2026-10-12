# Loki local-shard write integration stack

A self-contained stack that exercises **`CLICKHOUSE_LOCAL_SHARD_WRITES`** against
a real sharded + replicated ClickHouse.

## Topology

- **ClickHouse**: 3 shards × 2 replicas = 6 nodes (`ch-s1r1`, `ch-s1r2`, `ch-s2r1`,
  `ch-s2r2`, `ch-s3r1`, `ch-s3r2`) in a cluster named `default`. Keeper is
  embedded in `ch-s1r1` (every node points its `<zookeeper>` at `ch-s1r1:9181`),
  so no separate keeper image is needed.
- **`ch-write` alias**: `CLICKHOUSE_SERVER=ch-write` is a Docker network alias
  shared by **only the write replica of each shard** — `ch-s1r1`, `ch-s2r1`,
  `ch-s3r1`. So `ch-write` resolves to exactly 3 IPs, one per shard. This is the
  "write subset" the feature restricts writes to.
- **gigapipe-writer**: `MODE=writer`, `CLICKHOUSE_LOCAL_SHARD_WRITES=true`. Writes
  each Loki row directly into the **local** `time_series` / `samples_v3` tables of
  shard `fingerprint % 3`, and only ever to a node in the `ch-write` set.
- **gigapipe-reader**: `MODE=reader`, **unchanged** — queries the `_dist`
  Distributed tables, which scatter to every shard and merge. It points at the
  `ch-read` alias (all 6 nodes), not the write subset: the Distributed engine can
  use any cluster node as its initiator, so reads spread across every node.
- **Exporters** (same shape as `federated-integration`, no tenant headers): two
  dummy log apps + Alloy tailers, and an nginx log generator with three traffic
  loops + an Alloy tailer.
- **Grafana** + **Prometheus** for querying.

```
         dummy apps / nginx ──Alloy──► gigapipe-writer ──local shard writes──►  ch-s1r1 (shard1)  ──repl──► ch-s1r2
                                       (fingerprint % 3)                         ch-s2r1 (shard2)  ──repl──► ch-s2r2
                                                                                 ch-s3r1 (shard3)  ──repl──► ch-s3r2
  Grafana ──► gigapipe-reader ──reads via *_dist (scatter/merge over all shards)
```

Writes land only on the `rN=1` nodes (the `ch-write` set); the `rN=2` nodes get
their copy through ReplicatedMergeTree replication, not through direct writes.

## Run

```bash
cd test/local-write-integration
docker compose up --build
```

Ports on the host:

| Service | URL / port |
| --- | --- |
| Grafana | http://localhost:3000 (anonymous admin) |
| Writer (push API) | http://localhost:3100 |
| Reader (query API) | http://localhost:3101 |
| Prometheus | http://localhost:9090 |
| ClickHouse native | 9001…9006 (s1r1,s1r2,s2r1,s2r2,s3r1,s3r2) |
| ClickHouse HTTP | 8201…8206 (same order) |

Tear down (removes all volumes): `docker compose down -v`.

## Query from Grafana

Open http://localhost:3000, pick the **gigapipe** Loki datasource, and try:

```logql
{service="dummy-app-platform"}
{service="dummy-app-data", level="error"}
{job="dummy-nginx", source="api"}
sum(rate({service="dummy-app-platform"}[1m]))
```

Everything is served through the reader's `_dist` path, so results are identical
to a non-sharded deployment — that is the point.

## Verify sharded writes

A short settle (~30s of ingest) gives each shard some streams. Substitute the
database name `gigapipe` as configured.

### 1. The write set is exactly one node per shard

```bash
# ch-write resolves to exactly 3 IPs (the r1 nodes). Run from a CH container
# (Ubuntu-based, has getent):
docker compose exec ch-s1r2 getent hosts ch-write

# system.clusters lists all 6 nodes; the writer only writes to the 3 above.
docker compose exec ch-s1r1 clickhouse-client -q \
  "SELECT shard_num, replica_num, host_name, host_address FROM system.clusters WHERE cluster='default' ORDER BY shard_num, replica_num"

# The writer logs confirm it enabled shard routing:
docker compose logs gigapipe-writer | grep -i "local-shard"
```

### 2. Each shard holds only its own fingerprints (local tables, no `_dist`)

Shard index = `shard_num - 1`, so shard 1 keeps `fingerprint % 3 = 0`, shard 2
`= 1`, shard 3 `= 2`. Each of these must return **0**:

```bash
docker compose exec ch-s1r1 clickhouse-client -q \
  "SELECT count() FROM gigapipe.time_series WHERE fingerprint % 3 != 0"
docker compose exec ch-s2r1 clickhouse-client -q \
  "SELECT count() FROM gigapipe.time_series WHERE fingerprint % 3 != 1"
docker compose exec ch-s3r1 clickhouse-client -q \
  "SELECT count() FROM gigapipe.time_series WHERE fingerprint % 3 != 2"
```

`time_series_gin` and `metrics_15s` co-shard automatically (local MVs); the same
check holds for them, e.g. on shard 2:

```bash
docker compose exec ch-s2r1 clickhouse-client -q \
  "SELECT count() FROM gigapipe.time_series_gin WHERE fingerprint % 3 != 1"
```

### 3. Direct writes hit only the `ch-write` replicas (r1), not r2

Replication copies data to r2, so both replicas end up with the same rows — the
proof that **writes** only went to r1 is in each node's `system.query_log`: r1
has direct `INSERT`s from gigapipe, r2 has none (it only received replicated
parts).

```bash
# r1 of shard 1: direct INSERTs into local samples_v3 (> 0 expected)
docker compose exec ch-s1r1 clickhouse-client -q \
  "SELECT count() FROM system.query_log WHERE type='QueryFinish' AND query ILIKE 'INSERT INTO samples_v3 %'"

# r2 of shard 1: NO direct INSERTs (0 expected) — data arrived via replication
docker compose exec ch-s1r2 clickhouse-client -q \
  "SELECT count() FROM system.query_log WHERE type='QueryFinish' AND query ILIKE 'INSERT INTO samples_v3 %'"
```

(Both replicas still return the same row count for `SELECT count() FROM
gigapipe.samples_v3`, confirming replication worked.)

### 4. Reads see every shard via `_dist`

```bash
# Per-shard local count (one shard's rows):
docker compose exec ch-s1r1 clickhouse-client -q "SELECT count() FROM gigapipe.samples_v3"
# Cluster-wide count via the Distributed table the reader uses (all shards):
docker compose exec ch-s1r1 clickhouse-client -q "SELECT count() FROM gigapipe.samples_v3_dist"
```

The `_dist` count equals the sum across the three shards, which is what the
reader (and Grafana) returns.

## Notes

- Federation is **off** here; this stack is only about shard placement. For the
  multi-tenant variant see `../federated-integration`.
- The writer refuses to start if `CLICKHOUSE_LOCAL_SHARD_WRITES=true` without a
  `CLUSTER_NAME`/`CLICKHOUSE_SERVER`, or if any shard has no replica in the
  `ch-write` set — it never silently falls back to `_dist`.
- A single-node Keeper is used for simplicity (not HA).
