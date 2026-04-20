package cpclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newServerMux wires a test server with the given handlers. Helper exists so
// individual tests stay focused on behaviour, not plumbing.
func newServerMux(t *testing.T, handlers map[string]http.HandlerFunc) (*httptest.Server, *Client) {
	t.Helper()
	mux := http.NewServeMux()
	for path, h := range handlers {
		mux.HandleFunc(path, h)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, New(srv.URL)
}

func TestGetStrategyVersion_HappyPath(t *testing.T) {
	wantID := "00000000-0000-0000-0000-000000000001"
	wantDSL := json.RawMessage(`{"schema_version":"2.0.0"}`)
	srv, cli := newServerMux(t, map[string]http.HandlerFunc{
		"/api/v1/strategy-versions/" + wantID: func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Fatalf("method = %s, want GET", r.Method)
			}
			_ = json.NewEncoder(w).Encode(StrategyVersion{
				ID:                 wantID,
				StrategyTemplateID: "tpl-1",
				Version:            3,
				DSLJSON:            wantDSL,
				CreatedAt:          time.Now().UTC(),
			})
		},
	})
	_ = srv

	got, err := cli.GetStrategyVersion(context.Background(), wantID)
	if err != nil {
		t.Fatalf("GetStrategyVersion: %v", err)
	}
	if got.ID != wantID || got.Version != 3 || got.StrategyTemplateID != "tpl-1" {
		t.Fatalf("unexpected version: %+v", got)
	}
	if string(got.DSLJSON) != string(wantDSL) {
		t.Fatalf("DSLJSON = %s, want %s", got.DSLJSON, wantDSL)
	}
}

func TestGetStrategyVersion_NotFound_WrapsSentinel(t *testing.T) {
	_, cli := newServerMux(t, map[string]http.HandlerFunc{
		"/api/v1/strategy-versions/missing": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		},
	})
	_, err := cli.GetStrategyVersion(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error on 404")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err should wrap ErrNotFound, got: %v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 404 {
		t.Fatalf("expected *APIError{404}, got: %T %v", err, err)
	}
	if !strings.Contains(apiErr.Body, "not found") {
		t.Fatalf("body should be carried verbatim, got: %q", apiErr.Body)
	}
}

func TestGetStrategyVersion_EmptyID(t *testing.T) {
	// No server needed — validation happens before we hit the wire.
	cli := New("http://invalid.invalid")
	if _, err := cli.GetStrategyVersion(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty id")
	}
}

func TestGetExperimentRun_HappyPath(t *testing.T) {
	runID := "run-42"
	_, cli := newServerMux(t, map[string]http.HandlerFunc{
		"/api/v1/experiment-runs/" + runID: func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(ExperimentRun{
				ID:                runID,
				ExperimentBatchID: "batch-1",
				StrategyVersionID: "sv-1",
				Symbol:            "BTCUSDT",
				Status:            "queued",
			})
		},
	})
	got, err := cli.GetExperimentRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetExperimentRun: %v", err)
	}
	if got.Symbol != "BTCUSDT" || got.Status != "queued" || got.StrategyVersionID != "sv-1" {
		t.Fatalf("unexpected run: %+v", got)
	}
}

func TestGetDataset_HappyPath(t *testing.T) {
	dsID := "dataset-1"
	_, cli := newServerMux(t, map[string]http.HandlerFunc{
		"/api/v1/datasets/" + dsID: func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(Dataset{
				ID:          dsID,
				Exchange:    "binance",
				Symbol:      "BTCUSDT",
				DatasetType: "trade_klines",
				Interval:    "1m",
				S3Prefix:    "datasets/binance/BTCUSDT/trade_klines/1m/",
			})
		},
	})
	got, err := cli.GetDataset(context.Background(), dsID)
	if err != nil {
		t.Fatalf("GetDataset: %v", err)
	}
	if got.S3Prefix == "" || got.Symbol != "BTCUSDT" {
		t.Fatalf("unexpected dataset: %+v", got)
	}
}

func TestListDatasetPartitions_HappyPath(t *testing.T) {
	dsID := "dataset-1"
	_, cli := newServerMux(t, map[string]http.HandlerFunc{
		"/api/v1/datasets/" + dsID + "/partitions": func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode([]DatasetPartition{
				{ID: "p1", DatasetID: dsID, Year: 2025, Month: 10, S3Path: "p/2025/10/part.parquet"},
				{ID: "p2", DatasetID: dsID, Year: 2025, Month: 11, S3Path: "p/2025/11/part.parquet"},
			})
		},
	})
	got, err := cli.ListDatasetPartitions(context.Background(), dsID)
	if err != nil {
		t.Fatalf("ListDatasetPartitions: %v", err)
	}
	if len(got) != 2 || got[0].Year != 2025 || got[1].Month != 11 {
		t.Fatalf("unexpected partitions: %+v", got)
	}
}

func TestPatchRunRunning_SendsStatusOnly(t *testing.T) {
	runID := "run-42"
	var gotMethod, gotPath string
	var gotBody map[string]any

	_, cli := newServerMux(t, map[string]http.HandlerFunc{
		"/api/v1/experiment-runs/" + runID + "/status": func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotPath = r.URL.Path
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &gotBody)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
		},
	})

	if err := cli.PatchRunRunning(context.Background(), runID); err != nil {
		t.Fatalf("PatchRunRunning: %v", err)
	}
	if gotMethod != http.MethodPatch {
		t.Fatalf("method = %s, want PATCH", gotMethod)
	}
	if !strings.HasSuffix(gotPath, "/status") {
		t.Fatalf("path = %s, want .../status", gotPath)
	}
	if gotBody["status"] != "running" {
		t.Fatalf("body.status = %v, want running", gotBody["status"])
	}
	if _, hasResult := gotBody["result"]; hasResult {
		t.Fatalf("body.result must not be sent for running (engine does not own terminal state): %v", gotBody)
	}
}

func TestPatchRunRunning_SurfacesServerError(t *testing.T) {
	_, cli := newServerMux(t, map[string]http.HandlerFunc{
		"/api/v1/experiment-runs/run-x/status": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"db down"}`, http.StatusInternalServerError)
		},
	})
	err := cli.PatchRunRunning(context.Background(), "run-x")
	if err == nil {
		t.Fatal("expected error on 500")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 500 {
		t.Fatalf("expected APIError{500}, got %v", err)
	}
}

// Guard: the client MUST respect ctx cancellation. Without this, a stuck CP
// would keep a run-worker goroutine alive past its run's lifetime.
func TestGet_RespectsContextCancellation(t *testing.T) {
	_, cli := newServerMux(t, map[string]http.HandlerFunc{
		"/api/v1/strategy-versions/slow": func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := cli.GetStrategyVersion(ctx, "slow")
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}
