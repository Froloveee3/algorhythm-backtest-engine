# backtest-engine

Minimal worker for **Stage 3** orchestration: consumes `bt.run.requested` from NATS JetStream, marks the experiment run as `running` in control-plane, writes a summary row to ClickHouse, then publishes `bt.run.completed` or `bt.run.failed`.

## Prerequisites

- **NATS** with JetStream and stream `ORCHESTRATION` (subjects `md.>`, `fb.>`, `bt.>`, `cp.>`) — same as control-plane worker.
- **control-plane** HTTP API reachable at `BT_CONTROL_PLANE_URL`.
- **ClickHouse** with schema applied from `migrations/clickhouse/001_init.sql` (MVP summary) and `002_backtest_results.up.sql` (canonical result tables: `backtest_trades`, `backtest_equity_curve`, `backtest_run_metrics` — checked in as **skeleton**, engine does not yet populate them).

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

Ensure control-plane API and worker are running so experiment runs transition `created` → `queued` → `running` via outbox flush and this consumer.
