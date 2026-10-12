# Configuration

## Docker Quickstart

Pull and run the latest image:

```bash
docker run -d \
  --name gigapipe \
  -p 3100:3100 \
  -e CLICKHOUSE_SERVER=clickhouse.example.com \
  -e CLICKHOUSE_AUTH=admin:password \
  ghcr.io/metrico/gigapipe:latest
```

The container image is published to `ghcr.io/metrico/gigapipe:latest` with multi-arch support (linux/amd64, linux/arm64). Gigapipe exposes port `3100` by default.

## ClickHouse Connection

- **`CLICKHOUSE_SERVER`** - ClickHouse server hostname or IP address (default: `localhost`)
- **`CLICKHOUSE_PORT`** - ClickHouse TCP port (default: `9000`)
- **`CLICKHOUSE_DB`** - Database name (default: `cloki`)
- **`CLICKHOUSE_AUTH`** - Authentication credentials in format `username:password`
- **`CLICKHOUSE_PROTO`** - Protocol to use: `http`, `https`, or `tls` (default: `http`)
- **`SELF_SIGNED_CERT`** - Skip TLS certificate verification for self-signed certificates (`true`, `false`)

## Cluster Configuration

- **`CLUSTER_NAME`** - Enables clustered mode and sets the cluster name. When set, gigapipe operates in distributed mode.
- **`CLICKHOUSE_READ_DIST_SUFFIX`** - Suffix for read-path distributed tables (default: `_dist`). Used for cross-cluster reads in multi-cluster deployments. See [Cross-Cluster Deployment](#cross-cluster-deployment) below.

## Loki Local-Shard Writes

- **`CLICKHOUSE_LOCAL_SHARD_WRITES`** - Opt-in (default off). When enabled, the **writer** stops handing Loki log rows to the ClickHouse `_dist` Distributed engine and instead writes each row directly to the **local** `time_series` / `samples_v3` tables of shard `fingerprint % shardCount`. Requires `CLUSTER_NAME` (the cluster to shard across) and `CLICKHOUSE_SERVER` (the write endpoint); the process refuses to start without them rather than silently falling back to `_dist`.
  - **Writes only ever go to the `CLICKHOUSE_SERVER` IPs.** `system.clusters` is used only to learn which `shard_num` each node belongs to; the per-shard write target is then restricted to replicas whose address is in the DNS resolution of `CLICKHOUSE_SERVER`. Cluster nodes that appear in the topology but are **not** in that DNS set never receive writes. If any shard has no replica in the `CLICKHOUSE_SERVER` set, the writer refuses to start (it will not route that shard's writes to a non-write node). The write set is re-resolved on each topology refresh.
- **`CLICKHOUSE_SHARD_REFRESH_INTERVAL`** - Bounded interval for re-polling `system.clusters` (Go duration, default `1m`; `0` disables refresh).
- **`CLICKHOUSE_SHARD_DNS_SUFFIX`** - DNS suffix appended to each replica's `system.clusters` `host_name` to form the per-shard write connection target: the writer dials `<host_name>.<suffix>` instead of the discovered `host_address`. Defaults to `clickhouse-cluster.svc.cluster.local` (a Kubernetes headless-service FQDN) because pod IPs (`host_address`) are not stable across restarts, whereas the StatefulSet DNS name is. Set it to an empty string (`""`) to dial the raw `host_address` instead — required for the local docker stack, whose short hostnames (`ch-s1r1`, …) don't resolve under a cluster suffix. This only changes the write connection target; the write-eligibility allow-list is still matched against the `CLICKHOUSE_SERVER` DNS resolution.

Notes:

- **Reads are unchanged.** Queries keep using the `_dist` tables, which scatter to every shard's local table and merge — so locally-placed rows are read back identically, and `CLICKHOUSE_READ_DIST_SUFFIX` still applies to all reads. There is no read-side collision with the cross-cluster suffix.
- `time_series_gin` and `metrics_15s` are **not** written directly; they are produced by local materialized views from `time_series` / `samples_v3`, so they co-shard automatically. Loki `patterns` stay on `patterns_dist`.
- **Shard addition** is picked up automatically on the next topology refresh (new writes route under the new count). A **shard-count change is held**, not applied live, because `fingerprint % N` remapping requires an operator-coordinated cutover; migration/rebalancing of historical data on shard removal is an operator task.

## Authentication

- **`QRYN_LOGIN`** - Username for HTTP basic authentication
- **`QRYN_PASSWORD`** - Password for HTTP basic authentication
- **`CLOKI_LOGIN`** - Legacy username (alias for `QRYN_LOGIN`)
- **`CLOKI_PASSWORD`** - Legacy password (alias for `QRYN_PASSWORD`)

## HTTP Settings

- **`PORT`** - HTTP server port (default: `3100`)
- **`HOST`** - HTTP server bind address (default: `0.0.0.0`)
- **`CORS_ALLOW_ORIGIN`** - Enable CORS and set allowed origin (e.g., `https://example.com`)

## Write Settings

- **`BULK_MAX_SIZE_BYTES`** - Maximum batch size in bytes before flushing to ClickHouse
- **`BULK_MAX_AGE_MS`** - Maximum age in milliseconds before flushing batch (default: `100`)

## Advanced Settings

- **`ADVANCED_SAMPLES_ORDERING`** - Custom ordering for samples table (ClickHouse ORDER BY clause)
- **`ADVANCED_PROMETHEUS_MAX_SAMPLES`** - Maximum number of samples returned in Prometheus queries
- **`ADVANCED_OMIT_EMPTY_VALUES`** - Omit empty values in query results (`true`, `false`)
- **`OMIT_CREATE_TABLES`** - Skip table creation on startup (`true`, `false`)
- **`COMPAT_4_0_19`** - Enable compatibility mode for v4.0.19 behavior (`true`, `false`)

## Storage and Retention

- **`SAMPLES_DAYS`** - TTL in days for stored samples (default: `7`)
- **`STORAGE_POLICY`** - ClickHouse storage policy name for data placement

## Mode

- **`MODE`** - Operating mode:
  - `all` - Run both reader and writer (default)
  - `reader` - Run query/read endpoints only
  - `writer` - Run ingestion/write endpoints only
  - `init_only` - Initialize database and exit
- **`READONLY`** - Set to `true` to force reader mode (equivalent to `MODE=reader`)

## Logging

- **`LOG_LEVEL`** - Log level: `debug`, `info`, `warn`, `error`

## Log Drilldown

- **`LOG_DRILLDOWN`** - Enable log pattern detection and drilldown features (`true`, `false`)
- **`LOG_PATTERN_SIMILARITY`** - Similarity threshold for pattern grouping, range 0-1 (default: `0.7`). Higher values require more similarity.
- **`LOG_PATTERN_READ_LIMIT`** - Maximum number of log patterns to read per request (default: `300`)

## Recording Rules (Ruler)

The ruler evaluates LogQL and PromQL recording rules on a schedule and writes
the results back as new series. It is single-tenant and recording-only
(alerting rules may be stored but are never evaluated). It runs only in modes
`all`/`""`, after the writer and reader initialize.

- **`QRYN_RULER_ENABLED`** - Enable the ruler (`1`, `true`, `yes`, `on`; default: disabled). When disabled, the rule endpoints (`/api/v1/rules`, `/loki/api/v1/rules`, `/api/prom/rules`) are **not** served and return `404`.
- **`QRYN_RULER_POLL_INTERVAL`** - How often rule groups are reloaded from storage and rescheduled, as a Go duration (e.g. `15s`, `1m`; default: `30s`).
- **`QRYN_RULER_MAX_LOGQL_RESULT_BYTES`** - Maximum size, in bytes, of a single LogQL recording-rule result buffered before parsing; a rule exceeding it fails that evaluation (default: `10485760`, i.e. 10 MiB).
- **`QRYN_RULER_REMOTE_WRITE_URL`** - When set, recording-rule results are shipped to this external **Mimir-compatible** remote-write endpoint (Prometheus remote-write v1: protobuf + snappy), e.g. `http://mimir:9009/api/v1/push`, instead of being written back into gigapipe's in-process metrics pipeline. Unset (default): in-process write-back. There is no retry/queue; a failed push is logged and dropped.
- **`QRYN_RULER_REMOTE_WRITE_TIMEOUT`** - Per-request HTTP timeout for remote-write, as a Go duration (e.g. `10s`, `1m`; default: `30s`). Only applies when `QRYN_RULER_REMOTE_WRITE_URL` is set.
- **`QRYN_RULER_REMOTE_WRITE_TENANT`** - Static `X-Scope-OrgID` for remote-write. When set, it is an **explicit override**: every remote-write request is sent under this tenant, regardless of federation. This decouples the destination (Mimir) tenant from the source (Loki) tenant — reads stay multi-tenant (each rule is evaluated scoped to its own `oid`) while all recorded series land under this single Mimir tenant (e.g. set to `anonymous` to collapse all rule output into one tenant). When **unset**: under federation the rule's own `oid` is used as `X-Scope-OrgID`; with federation off the header is omitted.

## Self-Profiling

- **`PYROSCOPE_SERVER_ADDRESS`** - Pyroscope server URL (e.g., `http://pyroscope:4040`)
- **`PYROSCOPE_APPLICATION_NAME`** - Application name in Pyroscope (default: `gigapipe`)

## Cross-Cluster Deployment

For multi-region or multi-cluster deployments, you can separate write and read paths using different distributed table configurations.

### Use Case

Use cross-cluster reads when you need to:
- Query data across multiple ClickHouse clusters
- Implement multi-region deployments with centralized querying
- Separate read and write workloads

### Architecture

Writes use local distributed tables (default `_dist` suffix) that target the local cluster. Reads can use tables with a custom suffix (configured via `CLICKHOUSE_READ_DIST_SUFFIX`) that target multiple clusters.

### Configuration Example

```bash
# Writer instances (local cluster writes)
MODE=writer
CLUSTER_NAME=local_cluster
CLICKHOUSE_SERVER=clickhouse-local.example.com

# Reader instances (cross-cluster reads)
MODE=reader
CLUSTER_NAME=local_cluster
CLICKHOUSE_SERVER=clickhouse-local.example.com
CLICKHOUSE_READ_DIST_SUFFIX=_dist_cross_cluster
```

In this setup:
- Write operations use `table_name_dist` (local cluster only)
- Read operations use `table_name_dist_cross_cluster` (can span multiple clusters)

### ClickHouse Setup

```sql
CREATE TABLE IF NOT EXISTS samples_v3_dist_cross_cluster ON CLUSTER '{cluster}'
AS samples_v3
ENGINE = Distributed(
  'cross_cluster_name',
  currentDatabase(),
  'samples_v3',
  rand()
)
SETTINGS skip_unavailable_shards = 1;
```

`skip_unavailable_shards=1` ensures queries continue even if some shards are temporarily unavailable.

### Backward Compatibility

Without setting `CLICKHOUSE_READ_DIST_SUFFIX`, gigapipe uses the default `_dist` suffix for both reads and writes, maintaining backward compatibility with existing deployments.

## Example Configuration

Minimal single-node setup:

```bash
CLICKHOUSE_SERVER=clickhouse.example.com
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=gigapipe
CLICKHOUSE_AUTH=admin:password
PORT=3100
```

Production clustered setup:

```bash
CLICKHOUSE_SERVER=clickhouse.example.com
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=gigapipe
CLICKHOUSE_AUTH=admin:password
CLUSTER_NAME=production_cluster
STORAGE_POLICY=tiered_storage
SAMPLES_DAYS=30
BULK_MAX_AGE_MS=100
PORT=3100
QRYN_LOGIN=admin
QRYN_PASSWORD=secure_password
LOG_LEVEL=info
```
