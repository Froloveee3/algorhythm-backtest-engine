# Deploying backtest-engine

The worker consumes JetStream events and exposes **HTTP health only** (`BT_HTTP_PORT`, default `8090`: `/healthz`, `/readyz`). Use `openapi/openapi.yaml` for the probe contract.

## Docker

From `services/backtest-engine`:

```bash
cp .env.example .env   # adjust DSNs / NATS for your network
docker compose up --build
curl -sf http://localhost:8090/readyz
```

### Kubernetes / systemd

Run the same binary as in the Dockerfile; point `BT_NATS_URL` and `BT_CLICKHOUSE_DSN` at your cluster services. Probes should hit `/readyz` on `BT_HTTP_PORT`.
