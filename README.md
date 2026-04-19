# backtest-engine

Worker for **Stage 3** orchestration: consumes `bt.run.requested` from NATS JetStream, marks the experiment run as `running` in control-plane, runs a **deterministic placeholder simulator** that synthesises trades / equity curve / aggregate metrics from the `run_id` seed, writes all rows into the canonical ClickHouse tables, PATCHes `experiment_runs.status` to `completed` with a flat summary, then publishes `bt.run.completed` (or `bt.run.failed` on any failure).

The simulator is placeholder only — it does *not* read feature parquet and does *not* interpret the DSL yet. It exists so that downstream (`results-api`, `control-desktop` results screens) can be built against the real canonical ClickHouse schema while the interpreter lands. Same `run_id` always yields byte-identical rows, so rerunning is safe (detail tables get an `ALTER DELETE` on the existing `run_id` before insert; metrics use `ReplacingMergeTree(version)`).

## Prerequisites

- **NATS** with JetStream and stream `ORCHESTRATION` (subjects `md.>`, `fb.>`, `bt.>`, `cp.>`) — same as control-plane worker.
- **control-plane** HTTP API reachable at `BT_CONTROL_PLANE_URL`.
- **ClickHouse** with schema applied from:
  - `migrations/clickhouse/001_init.sql` — `backtest_run_summaries` (one-row-per-run marker; still written for existing dashboards).
  - `migrations/clickhouse/002_backtest_results.up.sql` — canonical result tables `backtest_trades`, `backtest_equity_curve`, `backtest_run_metrics`. **Populated** on every successful run.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `BT_CONTROL_PLANE_URL` | `http://localhost:8080` | Base URL of control-plane (no trailing slash required). |
| `BT_NATS_URL` | `nats://localhost:4222` | NATS server URL. |
| `BT_CLICKHOUSE_DSN` | `clickhouse://default:clickhouse@localhost:9009/default` | Native ClickHouse protocol DSN (host port `9009` matches `ops/full-stack` compose mapping `9009:9000`). |
| `BT_HTTP_PORT` | `8090` | Minimal HTTP listener: `/healthz`, `/readyz` (see `openapi/openapi.yaml`). |

## HTTP (probes only)

Orchestration remains NATS-first. Operators use `curl http://localhost:${BT_HTTP_PORT:-8090}/readyz` — checks NATS connectivity and ClickHouse ping.

## Packaging

Docker: `Dockerfile`, `docker-compose.yml`, `.env.example`, `deploy/README.md`. Targets: `make build`, `make openapi-lint`.

## Run

From this directory:

```bash
go run ./cmd/worker
```

Or build:

```bash
go build -o backtest-engine-worker ./cmd/worker
./backtest-engine-worker
```

Apply ClickHouse migrations (example with `clickhouse-client` against the mapped port):

```bash
clickhouse-client --host localhost --port 9009 --user default --password clickhouse --queries-file migrations/clickhouse/001_init.sql
clickhouse-client --host localhost --port 9009 --user default --password clickhouse --queries-file migrations/clickhouse/002_backtest_results.up.sql
```

Ensure control-plane API and worker are running so experiment runs transition `created` → `queued` → `running` → `completed` via outbox flush and this consumer.

## Testing

Unit tests (no external services needed):

```bash
go test ./cmd/worker/...
```

Live ClickHouse smoke — requires the dev stack to be up (docker compose in `ops/full-stack`):

```bash
go test -tags=chsmoke -v -run TestChSmoke_WriteReal ./cmd/worker/
```

Verifies that `simulateRun` + `writeSimulationResult` populate all three canonical tables and cleans up its rows afterwards.
