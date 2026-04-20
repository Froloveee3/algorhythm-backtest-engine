package runresolve

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/cpclient"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
)

// fakeCP is a stubbable in-process control-plane used by the resolver tests.
// Each endpoint returns the stored value or 404 when absent. We build it
// explicitly rather than with chi so the routes match cpclient's path scheme
// verbatim (trailing segments, no regex shenanigans).
type fakeCP struct {
	runs     map[string]cpclient.ExperimentRun
	batches  map[string]cpclient.ExperimentBatch
	fsvs     map[string]cpclient.FeatureSetVersion
	fss      map[string]cpclient.FeatureSet
	datasets []cpclient.Dataset               // served by ListDatasets (filtered by symbol+type)
	partsBy  map[string][]cpclient.DatasetPartition
}

func newFakeCP() *fakeCP {
	return &fakeCP{
		runs:    map[string]cpclient.ExperimentRun{},
		batches: map[string]cpclient.ExperimentBatch{},
		fsvs:    map[string]cpclient.FeatureSetVersion{},
		fss:     map[string]cpclient.FeatureSet{},
		partsBy: map[string][]cpclient.DatasetPartition{},
	}
}

func (f *fakeCP) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasPrefix(path, "/api/v1/experiment-runs/"):
			id := strings.TrimPrefix(path, "/api/v1/experiment-runs/")
			v, ok := f.runs[id]
			f.writeOr404(w, ok, v)
		case strings.HasPrefix(path, "/api/v1/experiment-batches/"):
			id := strings.TrimPrefix(path, "/api/v1/experiment-batches/")
			v, ok := f.batches[id]
			f.writeOr404(w, ok, v)
		case strings.HasPrefix(path, "/api/v1/feature-set-versions/"):
			id := strings.TrimPrefix(path, "/api/v1/feature-set-versions/")
			v, ok := f.fsvs[id]
			f.writeOr404(w, ok, v)
		case strings.HasPrefix(path, "/api/v1/feature-sets/"):
			id := strings.TrimPrefix(path, "/api/v1/feature-sets/")
			v, ok := f.fss[id]
			f.writeOr404(w, ok, v)
		case path == "/api/v1/datasets":
			sym := r.URL.Query().Get("symbol")
			typ := r.URL.Query().Get("type")
			out := make([]cpclient.Dataset, 0, len(f.datasets))
			for _, d := range f.datasets {
				if sym != "" && d.Symbol != sym {
					continue
				}
				if typ != "" && d.DatasetType != typ {
					continue
				}
				out = append(out, d)
			}
			writeJSON(w, http.StatusOK, out)
		case strings.HasSuffix(path, "/partitions") && strings.HasPrefix(path, "/api/v1/datasets/"):
			trim := strings.TrimPrefix(path, "/api/v1/datasets/")
			id := strings.TrimSuffix(trim, "/partitions")
			parts, ok := f.partsBy[id]
			if !ok {
				writeJSON(w, http.StatusOK, []cpclient.DatasetPartition{})
				return
			}
			writeJSON(w, http.StatusOK, parts)
		default:
			http.NotFound(w, r)
		}
	})
}

func (f *fakeCP) writeOr404(w http.ResponseWriter, ok bool, v any) {
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// seedHappyPath wires a full run → batch → fsv → fs → dataset → partitions
// chain onto the fake CP and returns the derived identifiers callers can use
// to point the resolver at a specific run.
func seedHappyPath(f *fakeCP) (runID string) {
	fs := cpclient.FeatureSet{ID: "fs-1", Code: "btcusdt_futures_mvp", CreatedAt: time.Unix(1000, 0)}
	fsv := cpclient.FeatureSetVersion{ID: "fsv-1", FeatureSetID: fs.ID, Version: 1, CreatedAt: time.Unix(1001, 0)}
	batch := cpclient.ExperimentBatch{ID: "batch-1", Name: "m5-batch", FeatureSetVersionID: fsv.ID, CreatedAt: time.Unix(1002, 0)}
	run := cpclient.ExperimentRun{
		ID:                "run-1",
		ExperimentBatchID: batch.ID,
		StrategyVersionID: "sv-1",
		Symbol:            "BTCUSDT",
		Status:            "created",
		CreatedAt:         time.Unix(1003, 0),
		UpdatedAt:         time.Unix(1003, 0),
	}
	ds := cpclient.Dataset{
		ID:          "ds-feat-1",
		Exchange:    "binance_usdm",
		Symbol:      "BTCUSDT",
		DatasetType: "feature_btcusdt_futures_mvp_1m",
		Interval:    "1m",
		S3Prefix:    "datasets/feature/btcusdt_futures_mvp/1m/BTCUSDT",
		CreatedAt:   time.Unix(2000, 0),
		UpdatedAt:   time.Unix(2000, 0),
	}

	f.fss[fs.ID] = fs
	f.fsvs[fsv.ID] = fsv
	f.batches[batch.ID] = batch
	f.runs[run.ID] = run
	f.datasets = []cpclient.Dataset{ds}
	// Intentionally out of (Year, Month) order to verify the resolver sorts.
	// feature-builder stores S3Path as the partition *directory*
	// (…/year=YYYY/month=MM) and the reader consumes full object keys, so the
	// resolver is expected to append "/data.parquet" to each path.
	f.partsBy[ds.ID] = []cpclient.DatasetPartition{
		{ID: "p2", DatasetID: ds.ID, Year: 2024, Month: 3, S3Path: "datasets/feature/btcusdt_futures_mvp/1m/BTCUSDT/year=2024/month=03"},
		{ID: "p1", DatasetID: ds.ID, Year: 2024, Month: 1, S3Path: "datasets/feature/btcusdt_futures_mvp/1m/BTCUSDT/year=2024/month=01"},
		{ID: "p3", DatasetID: ds.ID, Year: 2024, Month: 2, S3Path: "datasets/feature/btcusdt_futures_mvp/1m/BTCUSDT/year=2024/month=02"},
	}
	return run.ID
}

func newClient(t *testing.T, h http.Handler) *cpclient.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return cpclient.New(srv.URL)
}

func TestResolve_HappyPath_WalksChainAndSortsPartitions(t *testing.T) {
	f := newFakeCP()
	runID := seedHappyPath(f)
	cp := newClient(t, f.handler())

	r, err := Resolve(context.Background(), cp, runID, "1m")
	if err != nil {
		t.Fatalf("Resolve: unexpected error %v", err)
	}
	want := featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1}
	if r.Key != want {
		t.Fatalf("Key=%+v, want %+v", r.Key, want)
	}
	if r.Symbol != "BTCUSDT" || r.Interval != "1m" {
		t.Fatalf("Symbol=%q Interval=%q, want BTCUSDT / 1m", r.Symbol, r.Interval)
	}
	if r.Dataset.ID != "ds-feat-1" || r.Dataset.DatasetType != "feature_btcusdt_futures_mvp_1m" {
		t.Fatalf("Dataset=%+v, unexpected", r.Dataset)
	}
	if len(r.Partitions) != 3 {
		t.Fatalf("partitions=%d, want 3", len(r.Partitions))
	}
	for i := 1; i < len(r.Partitions); i++ {
		prev, cur := r.Partitions[i-1], r.Partitions[i]
		if prev.Year > cur.Year || (prev.Year == cur.Year && prev.Month >= cur.Month) {
			t.Fatalf("partitions not strictly chronological at %d: %+v then %+v", i, prev, cur)
		}
	}
	// The resolver must materialise the full parquet object key by appending
	// "/data.parquet" to feature-builder's directory-level S3Path so the
	// reader can pass it verbatim to storage.
	wantFirst := "datasets/feature/btcusdt_futures_mvp/1m/BTCUSDT/year=2024/month=01/data.parquet"
	if r.Partitions[0].ObjectKey != wantFirst {
		t.Fatalf("ObjectKey[0]=%q, want %q", r.Partitions[0].ObjectKey, wantFirst)
	}
}

func TestResolve_EmptyRunID_FailsEarly(t *testing.T) {
	// Catching malformed input before any HTTP call keeps the failure
	// message actionable ("empty run id") instead of leaking a 404 URL.
	cp := cpclient.New("http://unused.invalid")
	if _, err := Resolve(context.Background(), cp, "", "1m"); err == nil {
		t.Fatal("expected error for empty run id, got nil")
	}
}

func TestResolve_RunNotFound_Propagates404(t *testing.T) {
	// Run absent: resolver must surface cpclient.ErrNotFound unchanged so
	// the worker can tag the bt.run.failed event with "run not found" and
	// operators have a single sentinel to grep for in CP logs.
	f := newFakeCP()
	cp := newClient(t, f.handler())
	_, err := Resolve(context.Background(), cp, "ghost-run", "1m")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, cpclient.ErrNotFound) {
		t.Fatalf("expected wrapped cpclient.ErrNotFound, got %v", err)
	}
}

func TestResolve_BatchMissingFSVID_HardErrors(t *testing.T) {
	// CP schema allows feature_set_version_id to be NULL (e.g. legacy rows);
	// the resolver treats that as a hard error rather than silently binding
	// the engine to whatever arrives via ListDatasets.
	f := newFakeCP()
	f.runs["run-x"] = cpclient.ExperimentRun{ID: "run-x", ExperimentBatchID: "batch-x", Symbol: "BTCUSDT"}
	f.batches["batch-x"] = cpclient.ExperimentBatch{ID: "batch-x", Name: "x"} // no FSV id
	cp := newClient(t, f.handler())

	_, err := Resolve(context.Background(), cp, "run-x", "1m")
	if err == nil || !strings.Contains(err.Error(), "empty feature_set_version_id") {
		t.Fatalf("expected feature_set_version_id error, got %v", err)
	}
}

func TestResolve_DatasetNotFound_WrapsSentinel(t *testing.T) {
	// Resolver reaches the feature set successfully but the dataset listing
	// is empty — this is the "feature-builder hasn't run for this
	// (symbol, code, interval) yet" path. Must surface as ErrDatasetNotFound
	// so the worker can publish a specific reason.
	f := newFakeCP()
	runID := seedHappyPath(f)
	f.datasets = nil // drop all datasets
	cp := newClient(t, f.handler())

	_, err := Resolve(context.Background(), cp, runID, "1m")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrDatasetNotFound) {
		t.Fatalf("expected wrapped ErrDatasetNotFound, got %v", err)
	}
}

func TestResolve_AmbiguousDataset_Rejects(t *testing.T) {
	// Two datasets with identical (symbol, type, interval, period): CP's
	// schema permits this but the resolver refuses to pick one silently.
	f := newFakeCP()
	runID := seedHappyPath(f)
	dup := f.datasets[0]
	dup.ID = "ds-feat-dup"
	f.datasets = append([]cpclient.Dataset{dup}, f.datasets...) // both at the head, same period
	cp := newClient(t, f.handler())

	_, err := Resolve(context.Background(), cp, runID, "1m")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrAmbiguousDataset) {
		t.Fatalf("expected wrapped ErrAmbiguousDataset, got %v", err)
	}
}

func TestResolve_IntervalDrivesDatasetType(t *testing.T) {
	// Asking for an interval whose dataset does not exist must fail even if
	// a *different*-interval dataset for the same (symbol, code) is available.
	// Without this the engine would silently bind to the wrong cadence.
	f := newFakeCP()
	runID := seedHappyPath(f)
	cp := newClient(t, f.handler())

	_, err := Resolve(context.Background(), cp, runID, "5m")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrDatasetNotFound) {
		t.Fatalf("expected wrapped ErrDatasetNotFound for 5m, got %v", err)
	}
}

func TestResolve_PartitionPathEndingInParquet_PassesThrough(t *testing.T) {
	// Forward-compat: if a future producer registers a full object key
	// (ending in .parquet), the resolver must pass it through unchanged
	// instead of appending "/data.parquet" a second time.
	f := newFakeCP()
	runID := seedHappyPath(f)
	f.partsBy["ds-feat-1"] = []cpclient.DatasetPartition{
		{ID: "p1", DatasetID: "ds-feat-1", Year: 2024, Month: 1, S3Path: "fully/qualified/2024/01/part.parquet"},
	}
	cp := newClient(t, f.handler())

	r, err := Resolve(context.Background(), cp, runID, "1m")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := r.Partitions[0].ObjectKey; got != "fully/qualified/2024/01/part.parquet" {
		t.Fatalf("ObjectKey=%q; want bare parquet key passed through", got)
	}
}

func TestResolve_PartitionPathWithLeadingSlash_IsTrimmed(t *testing.T) {
	// Storage layer expects bare keys; any leading slash in s3_path must be
	// stripped before the key reaches the reader.
	f := newFakeCP()
	runID := seedHappyPath(f)
	f.partsBy["ds-feat-1"] = []cpclient.DatasetPartition{
		{ID: "p1", DatasetID: "ds-feat-1", Year: 2024, Month: 1, S3Path: "/ds/year=2024/month=01"},
	}
	cp := newClient(t, f.handler())

	r, err := Resolve(context.Background(), cp, runID, "1m")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := r.Partitions[0].ObjectKey; got != "ds/year=2024/month=01/data.parquet" {
		t.Fatalf("ObjectKey=%q; expected leading slash trimmed", got)
	}
}
