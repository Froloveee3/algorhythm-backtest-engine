package storage

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("BT_MINIO_ENDPOINT", "localhost:9000")
	t.Setenv("BT_MINIO_ACCESS_KEY", "ak")
	t.Setenv("BT_MINIO_SECRET_KEY", "sk")
	t.Setenv("BT_MINIO_BUCKET", "algorhythm-datasets")
	t.Setenv("BT_MINIO_USE_SSL", "TRUE")
	t.Setenv("BT_MINIO_REGION", "us-east-1")

	cfg := ConfigFromEnv()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if cfg.Bucket != "algorhythm-datasets" || cfg.Endpoint != "localhost:9000" {
		t.Fatalf("unexpected: %+v", cfg)
	}
	if !cfg.UseSSL {
		t.Fatalf("UseSSL should be true for 'TRUE'")
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"empty endpoint", Config{Bucket: "b"}, true},
		{"empty bucket", Config{Endpoint: "minio:9000"}, true},
		{"ok", Config{Endpoint: "minio:9000", Bucket: "b"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() err=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func TestStripScheme(t *testing.T) {
	cases := map[string]string{
		"http://minio:9000":   "minio:9000",
		"https://minio:9000/": "minio:9000",
		"minio:9000":          "minio:9000",
	}
	for in, want := range cases {
		if got := stripScheme(in); got != want {
			t.Errorf("stripScheme(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMemoryReader_StatGetList(t *testing.T) {
	m := NewMemoryReader()
	m.Put("datasets/binance/BTCUSDT/trade_klines/1m/2025/10/data.parquet", []byte("hello"))
	m.Put("datasets/binance/BTCUSDT/trade_klines/1m/2025/11/data.parquet", []byte("world"))
	m.Put("datasets/binance/ETHUSDT/other/1m/2025/10/data.parquet", []byte("x"))

	// Stat
	st, err := m.Stat(context.Background(), "datasets/binance/BTCUSDT/trade_klines/1m/2025/10/data.parquet")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Size != 5 {
		t.Fatalf("size = %d, want 5", st.Size)
	}

	// Get
	rc, err := m.Get(context.Background(), "datasets/binance/BTCUSDT/trade_klines/1m/2025/11/data.parquet")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if string(body) != "world" {
		t.Fatalf("body = %q, want 'world'", body)
	}

	// List with prefix narrows + result is sorted.
	keys, err := m.List(context.Background(), "datasets/binance/BTCUSDT/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("len(keys) = %d, want 2", len(keys))
	}
	if !strings.HasSuffix(keys[0], "2025/10/data.parquet") || !strings.HasSuffix(keys[1], "2025/11/data.parquet") {
		t.Fatalf("keys not sorted lexically: %v", keys)
	}
}

func TestMemoryReader_NotFound(t *testing.T) {
	m := NewMemoryReader()

	_, err := m.Stat(context.Background(), "missing")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("Stat err = %v, want ErrObjectNotFound", err)
	}

	_, err = m.Get(context.Background(), "missing")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("Get err = %v, want ErrObjectNotFound", err)
	}
}
