package main

import "testing"

func TestHealthPortsEnvDocumented(t *testing.T) {
	t.Helper()
	// BT_HTTP_PORT (default 8090), BT_NATS_URL, BT_CLICKHOUSE_DSN, BT_CONTROL_PLANE_URL —
	// BT_FEATURE_READ_FRAME (optional "true" → MinIO feature read; requires BT_MINIO_*).
	// see README / .env.example
}
