// Package cpclient is the typed HTTP client backtest-engine uses to talk to
// control-plane over `/api/v1`. Keep it small: only the endpoints the engine
// actually needs to drive a run (GET strategy_version / experiment_run /
// dataset + partitions, PATCH running status).
//
// Terminal state (completed / failed + result) is NOT PATCHed from here —
// ADR-004 v2 puts that concern on the control-plane worker which reacts to
// `bt.run.completed` / `bt.run.failed` on NATS. This package intentionally
// does not expose a helper for it so we don't accidentally grow two writers
// of the same state.
package cpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultTimeout is the default per-request timeout when a caller constructs
// the client without passing their own *http.Client. Long enough for a cold
// control-plane but short enough that a hung CP eventually fails the run.
const DefaultTimeout = 30 * time.Second

// Client wraps HTTP calls to control-plane /api/v1. Safe for concurrent use.
type Client struct {
	baseURL string
	http    *http.Client
}

// Option configures Client at construction. Add new options rather than new
// constructors so call-sites stay compact.
type Option func(*Client)

// WithHTTPClient lets callers plug in their own *http.Client (custom Transport
// for tests, different Timeout, etc).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// New builds a Client. baseURL is the control-plane root, e.g.
// "http://localhost:8080"; a trailing slash is tolerated.
func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimSuffix(strings.TrimSpace(baseURL), "/"),
		http:    &http.Client{Timeout: DefaultTimeout},
	}
	for _, o := range opts {
		o(c)
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: DefaultTimeout}
	}
	return c
}

// BaseURL returns the root URL the client was constructed with, mostly for
// logging / tests.
func (c *Client) BaseURL() string { return c.baseURL }

// ---------------------------------------------------------------------------
// Wire types. Field names match control-plane's Go-encoded JSON, which is the
// capitalised form of the underlying domain struct fields (no json tags on
// the server). The explicit tags here pin the contract and make the wire
// format obvious at the call site.
// ---------------------------------------------------------------------------

// StrategyVersion mirrors control-plane's domain.StrategyVersion. DSLJSON is
// kept as a raw message so the engine can feed it into dslv2.Validator and
// the compile step without a second decode.
type StrategyVersion struct {
	ID                 string          `json:"ID"`
	StrategyTemplateID string          `json:"StrategyTemplateID"`
	Version            int             `json:"Version"`
	DSLJSON            json.RawMessage `json:"DSLJSON"`
	CreatedAt          time.Time       `json:"CreatedAt"`
}

// ExperimentRun mirrors control-plane's domain.ExperimentRun. The engine reads
// Symbol + StrategyVersionID + ParametersJSON and nothing else; the other
// fields are kept so the type is reusable for smoke tests.
type ExperimentRun struct {
	ID                string          `json:"ID"`
	ExperimentBatchID string          `json:"ExperimentBatchID"`
	StrategyVersionID string          `json:"StrategyVersionID"`
	Symbol            string          `json:"Symbol"`
	Status            string          `json:"Status"`
	ParametersJSON    json.RawMessage `json:"ParametersJSON"`
	ResultJSON        json.RawMessage `json:"ResultJSON"`
	CreatedAt         time.Time       `json:"CreatedAt"`
	UpdatedAt         time.Time       `json:"UpdatedAt"`
}

// Dataset mirrors control-plane's domain.Dataset. S3Prefix is what the engine
// uses to discover raw / feature parquet in MinIO.
type Dataset struct {
	ID          string          `json:"ID"`
	Exchange    string          `json:"Exchange"`
	Symbol      string          `json:"Symbol"`
	DatasetType string          `json:"DatasetType"`
	Interval    string          `json:"Interval"`
	PeriodFrom  time.Time       `json:"PeriodFrom"`
	PeriodTo    time.Time       `json:"PeriodTo"`
	S3Prefix    string          `json:"S3Prefix"`
	Status      string          `json:"Status"`
	RowCount    *int64          `json:"RowCount,omitempty"`
	MinTS       *time.Time      `json:"MinTS,omitempty"`
	MaxTS       *time.Time      `json:"MaxTS,omitempty"`
	Checksum    string          `json:"Checksum,omitempty"`
	SourceType  string          `json:"SourceType,omitempty"`
	Metadata    json.RawMessage `json:"Metadata,omitempty"`
	CreatedAt   time.Time       `json:"CreatedAt"`
	UpdatedAt   time.Time       `json:"UpdatedAt"`
}

// DatasetPartition mirrors control-plane's domain.DatasetPartition. The
// canonical month parquet key lives at S3Path; ListDatasetPartitions already
// returns them in a stable order from the server, but callers should still
// sort by (Year, Month) before reading to guarantee determinism across CP
// releases.
type DatasetPartition struct {
	ID        string    `json:"ID"`
	DatasetID string    `json:"DatasetID"`
	Year      int       `json:"Year"`
	Month     int       `json:"Month"`
	S3Path    string    `json:"S3Path"`
	CreatedAt time.Time `json:"CreatedAt"`
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// APIError is returned when control-plane answers with a non-2xx status. The
// raw body is kept so error paths in the engine can forward it verbatim into
// `bt.run.failed` payloads.
type APIError struct {
	Method     string
	URL        string
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	if e == nil {
		return "<nil APIError>"
	}
	preview := e.Body
	if len(preview) > 256 {
		preview = preview[:256] + "..."
	}
	return fmt.Sprintf("control-plane %s %s: HTTP %d: %s", e.Method, e.URL, e.StatusCode, preview)
}

// ErrNotFound is a convenience sentinel: callers that want to branch on 404
// without unwrapping APIError can `errors.Is(err, cpclient.ErrNotFound)`.
var ErrNotFound = errors.New("control-plane: not found")

// Is implements errors.Is so APIError{404} matches ErrNotFound.
func (e *APIError) Is(target error) bool {
	return target == ErrNotFound && e != nil && e.StatusCode == http.StatusNotFound
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// GetStrategyVersion fetches `/api/v1/strategy-versions/{id}`. Returns
// ErrNotFound-wrapped APIError on 404.
func (c *Client) GetStrategyVersion(ctx context.Context, id string) (*StrategyVersion, error) {
	if id == "" {
		return nil, errors.New("cpclient: empty strategy_version id")
	}
	var out StrategyVersion
	if err := c.getJSON(ctx, "/api/v1/strategy-versions/"+url.PathEscape(id), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetExperimentRun fetches `/api/v1/experiment-runs/{id}`.
func (c *Client) GetExperimentRun(ctx context.Context, id string) (*ExperimentRun, error) {
	if id == "" {
		return nil, errors.New("cpclient: empty experiment_run id")
	}
	var out ExperimentRun
	if err := c.getJSON(ctx, "/api/v1/experiment-runs/"+url.PathEscape(id), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetDataset fetches `/api/v1/datasets/{id}`. Used by the engine to resolve
// the S3 prefix for the feature parquet it's about to read.
func (c *Client) GetDataset(ctx context.Context, id string) (*Dataset, error) {
	if id == "" {
		return nil, errors.New("cpclient: empty dataset id")
	}
	var out Dataset
	if err := c.getJSON(ctx, "/api/v1/datasets/"+url.PathEscape(id), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListDatasetPartitions fetches `/api/v1/datasets/{id}/partitions`. Engine
// sorts them by (Year, Month) on its end for deterministic iteration.
func (c *Client) ListDatasetPartitions(ctx context.Context, datasetID string) ([]DatasetPartition, error) {
	if datasetID == "" {
		return nil, errors.New("cpclient: empty dataset id")
	}
	var out []DatasetPartition
	if err := c.getJSON(ctx, "/api/v1/datasets/"+url.PathEscape(datasetID)+"/partitions", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// PatchRunRunning is the only status-patch helper the engine is allowed to
// use. It sets an experiment_run to "running" without a result body.
//
// ADR-004 v2: engine publishes bt.run.completed / bt.run.failed on NATS;
// control-plane's own worker owns terminal state. We intentionally don't
// expose a PatchRunCompleted / PatchRunFailed here so that ownership can't
// drift.
func (c *Client) PatchRunRunning(ctx context.Context, runID string) error {
	if runID == "" {
		return errors.New("cpclient: empty run id")
	}
	body := map[string]string{"status": "running"}
	return c.patchJSON(ctx, "/api/v1/experiment-runs/"+url.PathEscape(runID)+"/status", body, nil)
}

// ---------------------------------------------------------------------------
// internals
// ---------------------------------------------------------------------------

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}

func (c *Client) patchJSON(ctx context.Context, path string, in, out any) error {
	return c.do(ctx, http.MethodPatch, path, in, out)
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	full := c.baseURL + path

	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("cpclient: marshal %s %s: %w", method, path, err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, full, body)
	if err != nil {
		return fmt.Errorf("cpclient: build request %s %s: %w", method, path, err)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cpclient: %s %s: %w", method, full, err)
	}
	defer resp.Body.Close()

	raw, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("cpclient: read %s %s: %w", method, full, readErr)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{Method: method, URL: full, StatusCode: resp.StatusCode, Body: string(raw)}
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("cpclient: decode %s %s: %w", method, full, err)
	}
	return nil
}
