package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/cpclient"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/dslcompile"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featurecompat"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/runresolve"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/runtime"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/storage"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// defaultFeatureInterval is the fallback dataset interval when the compiled
// plan does not carry one (v1 strategies). MVP datasets are always 1m; the
// DSL v2 executor will replace this with plan.Interval once multi-interval
// support lands.
const defaultFeatureInterval = "1m"

const (
	eventRunRequested = "bt.run.requested"
	eventRunCompleted = "bt.run.completed"
	eventRunFailed    = "bt.run.failed"
	engineVersion     = "mvp"
)

type eventEnvelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	OccurredAtUTC time.Time       `json:"occurred_at_utc"`
	Producer      string          `json:"producer"`
	TraceID       string          `json:"trace_id"`
	Payload       json.RawMessage `json:"payload"`
}

type runRequestedPayload struct {
	RunID             string          `json:"run_id"`
	ExperimentBatchID string          `json:"experiment_batch_id"`
	StrategyVersionID string          `json:"strategy_version_id"`
	Symbol            string          `json:"symbol"`
	Parameters        json.RawMessage `json:"parameters,omitempty"`
}

type runCompletedPayload struct {
	RunID   string          `json:"run_id"`
	Summary json.RawMessage `json:"summary,omitempty"`
}

type runFailedPayload struct {
	RunID string `json:"run_id"`
	Error string `json:"error,omitempty"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cpURL := strings.TrimSuffix(os.Getenv("BT_CONTROL_PLANE_URL"), "/")
	if cpURL == "" {
		cpURL = "http://localhost:8080"
	}
	natsURL := os.Getenv("BT_NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	chDSN := os.Getenv("BT_CLICKHOUSE_DSN")
	if chDSN == "" {
		chDSN = "clickhouse://default:clickhouse@localhost:9009/default"
	}

	chConn, err := openClickHouse(chDSN)
	if err != nil {
		slog.Error("clickhouse connect failed", "err", err)
		os.Exit(1)
	}
	defer chConn.Close()

	nc, err := nats.Connect(natsURL, nats.Name("backtest-engine-worker"))
	if err != nil {
		slog.Error("nats connect failed", "err", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("jetstream init failed", "err", err)
		os.Exit(1)
	}
	if err := ensureStream(js); err != nil {
		slog.Error("jetstream stream setup failed", "err", err)
		os.Exit(1)
	}

	cp := cpclient.New(cpURL, cpclient.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}))

	// M6: optional MinIO-backed feature parquet read. Off by default so local
	// dev without object storage still runs the placeholder simulator. When
	// BT_FEATURE_READ_FRAME=true the worker fails fast if BT_MINIO_* is incomplete.
	var objStore storage.ObjectReader
	if strings.EqualFold(os.Getenv("BT_FEATURE_READ_FRAME"), "true") {
		r, err := storage.NewMinIOReader(storage.ConfigFromEnv())
		if err != nil {
			slog.Error("BT_FEATURE_READ_FRAME=true but MinIO reader init failed", "err", err)
			os.Exit(1)
		}
		objStore = r
		slog.Info("feature frame read enabled (MinIO)")
	}

	sub, err := js.QueueSubscribe(eventRunRequested, "backtest-engine", func(msg *nats.Msg) {
		if err := handleRunRequested(ctx, js, cp, chConn, objStore, msg); err != nil {
			slog.Error("handle bt.run.requested failed", "err", err)
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	}, nats.ManualAck(), nats.Durable("backtest-engine-bt-run-v1"), nats.AckExplicit(), nats.DeliverNew(), nats.BindStream("ORCHESTRATION"))
	if err != nil {
		slog.Error("subscribe bt.run.requested failed", "err", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	httpPort := os.Getenv("BT_HTTP_PORT")
	if httpPort == "" {
		httpPort = "8090"
	}
	startHealthHTTPServer(httpPort, nc, chConn)

	slog.Info("backtest-engine worker starting", "cp", cpURL, "nats", natsURL)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	cancel()
	slog.Info("backtest-engine worker stopped")
}

func ensureStream(js nats.JetStreamContext) error {
	cfg := &nats.StreamConfig{
		Name:      "ORCHESTRATION",
		Subjects:  []string{"md.>", "fb.>", "bt.>", "cp.>"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
	}
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      cfg.Name,
		Subjects:  cfg.Subjects,
		Storage:   cfg.Storage,
		Retention: cfg.Retention,
	})
	if err == nil {
		return nil
	}
	if err != nats.ErrStreamNameAlreadyInUse {
		return err
	}
	if _, updateErr := js.UpdateStream(cfg); updateErr != nil {
		return updateErr
	}
	return nil
}

func openClickHouse(dsn string) (driver.Conn, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	pass, _ := u.User.Password()
	user := u.User.Username()
	if user == "" {
		user = "default"
	}
	host := u.Host
	if host == "" {
		host = "localhost:9009"
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" {
		db = "default"
	}
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{host},
		Auth: clickhouse.Auth{
			Database: db,
			Username: user,
			Password: pass,
		},
	})
}

func handleRunRequested(
	ctx context.Context,
	js nats.JetStreamContext,
	cp *cpclient.Client,
	chConn driver.Conn,
	objStore storage.ObjectReader,
	msg *nats.Msg,
) error {
	var envelope eventEnvelope
	if err := json.Unmarshal(msg.Data, &envelope); err != nil {
		return err
	}
	var payload runRequestedPayload
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return err
	}
	if payload.RunID == "" {
		return fmt.Errorf("run_id missing in payload")
	}

	// Engine only ever PATCHes `running`. Terminal state (completed / failed
	// + result) is owned by the control-plane worker, which reacts to the
	// NATS events we publish below. See ADR-004 v2.
	if err := cp.PatchRunRunning(ctx, payload.RunID); err != nil {
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
		return nil
	}

	// M4: DSL compile boundary. Fetch the versioned strategy document from
	// control-plane, then run dispatch.Parse + typed normalisation exactly
	// once per run. The CompiledPlan carries RequiredColumns the feature
	// reader (M3) will bind against once dataset resolution is wired; the
	// legacy v1 simulator below still runs for now.
	//
	// Every failure mode here (missing strategy_version, schema violation,
	// v2 semantic hard error, unknown feature column) must surface as a
	// bt.run.failed event with a human-readable message; ADR-004 v2 pins
	// terminal state on the control-plane worker, so we do NOT PATCH
	// failed/completed from this process.
	plan, err := compileStrategy(ctx, cp, payload.StrategyVersionID)
	if err != nil {
		slog.Error("dsl compile failed", "err", err, "run_id", payload.RunID, "strategy_version_id", payload.StrategyVersionID)
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
		return nil
	}
	slog.Info("dsl compile ok",
		"run_id", payload.RunID,
		"strategy_code", plan.StrategyCode,
		"major", int(plan.Major),
		"required_columns", len(plan.RequiredColumns),
		"semantic_warnings", len(plan.SemanticWarnings),
	)

	// M5: resolve the feature-parquet dataset bound to this run and verify
	// the compiled plan's RequiredColumns are part of its contract. Both
	// steps live on the cold boundary — one CP round-trip per hop, no S3 —
	// so a contract mismatch fails the run BEFORE any object read.
	//
	// v1 strategies compile with RequiredColumns empty and their legacy
	// simulator does not touch feature parquet; we still resolve to keep a
	// single code path and surface CP/data issues early. If the resolved
	// interval differs from the plan's interval (future multi-interval
	// strategies), we consider it a hard error because the feature file the
	// engine would bind would be for a different cadence.
	interval := resolveInterval(plan)
	resolved, err := runresolve.Resolve(ctx, cp, payload.RunID, interval)
	if err != nil {
		slog.Error("feature dataset resolve failed", "err", err, "run_id", payload.RunID, "interval", interval)
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
		return nil
	}
	if err := featurecompat.Check(resolved.Key, plan.RequiredColumns, nil); err != nil {
		slog.Error("feature-set compat check failed", "err", err, "run_id", payload.RunID, "code", resolved.Key.Code, "version", resolved.Key.Version)
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
		return nil
	}
	slog.Info("feature dataset resolved",
		"run_id", payload.RunID,
		"symbol", resolved.Symbol,
		"interval", resolved.Interval,
		"feature_set_code", resolved.Key.Code,
		"feature_set_version", resolved.Key.Version,
		"dataset_id", resolved.Dataset.ID,
		"partitions", len(resolved.Partitions),
	)

	// M6/M7: read feature parquet through the M3 reader when objStore is wired
	// (BT_FEATURE_READ_FRAME=true). For the real v1 executor this frame is now
	// required: the old synthetic simulator remains only as a temporary fallback
	// for non-v1 paths while runtime support grows.
	frame, err := readFeatureData(ctx, objStore, plan, resolved)
	if err != nil {
		errMsg := formatFeatureReadFailure(err)
		slog.Error("feature frame read failed", "err", err, "run_id", payload.RunID)
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, errMsg)
		return nil
	}
	if frame != nil {
		slog.Info("feature frame read ok",
			"run_id", payload.RunID,
			"rows", frame.RowCount,
			"price_scale", frame.PriceScale,
		)
	}
	if plan.Major == dslcompile.MajorV1 {
		if frame == nil {
			_ = publishRunFailed(js, payload.RunID, envelope.TraceID, formatRunFailure(fmt.Errorf("%w: FeatureFrame required; enable BT_FEATURE_READ_FRAME and MinIO access", runtime.ErrInvalidData)))
			return nil
		}
		metrics, err := executeV1Run(ctx, chConn, payload.RunID, payload.StrategyVersionID, payload.Symbol, plan, frame)
		if err != nil {
			slog.Error("runtime v1 execute failed", "err", err, "run_id", payload.RunID)
			_ = publishRunFailed(js, payload.RunID, envelope.TraceID, formatRunFailure(err))
			return nil
		}

		// backtest_run_summaries (from 001_init) stays as the cheap "one row per
		// run" marker some dashboards still scan. Canonical detail tables are
		// populated from the runtime result above.
		if err := insertSummary(ctx, chConn, payload.RunID, payload.Symbol); err != nil {
			_ = publishRunFailed(js, payload.RunID, envelope.TraceID, formatRunFailure(fmt.Errorf("%w: insert backtest_run_summaries: %v", ErrClickHouseWriteFailed, err)))
			return nil
		}
		summary, err := buildCompletedSummary(metrics)
		if err != nil {
			slog.Error("build summary failed", "err", err, "run_id", payload.RunID)
			_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
			return nil
		}
		if err := publishRunCompleted(js, payload.RunID, envelope.TraceID, summary); err != nil {
			slog.Error("publish completed failed", "err", err, "run_id", payload.RunID)
			_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
		}
		return nil
	}

	// backtest_run_summaries (from 001_init) stays as the cheap "one row per
	// run" marker some dashboards still scan. New canonical tables from
	// migration 002 now carry the real payload.
	if err := insertSummary(ctx, chConn, payload.RunID, payload.Symbol); err != nil {
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, formatRunFailure(fmt.Errorf("%w: insert backtest_run_summaries: %v", ErrClickHouseWriteFailed, err)))
		return nil
	}

	// Placeholder simulator: deterministic synthetic trades/equity/metrics by
	// run_id. The DSL-driven executor that replaces it lands with the M3
	// reader wiring; M4 only guarantees the plan is compiled and the required
	// columns are known by the time we get here.
	sim := simulateRun(payload.RunID, payload.StrategyVersionID, payload.Symbol)
	if err := writeSimulationResult(ctx, chConn, sim); err != nil {
		slog.Error("write simulation result failed", "err", err, "run_id", payload.RunID)
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, formatRunFailure(fmt.Errorf("%w: %v", ErrClickHouseWriteFailed, err)))
		return nil
	}

	summary, err := buildCompletedSummary(sim.Metrics)
	if err != nil {
		slog.Error("build summary failed", "err", err, "run_id", payload.RunID)
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
		return nil
	}

	if err := publishRunCompleted(js, payload.RunID, envelope.TraceID, summary); err != nil {
		slog.Error("publish completed failed", "err", err, "run_id", payload.RunID)
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
	}
	return nil
}

// resolveInterval picks the dataset cadence the engine should bind. v2
// strategies carry the interval on the plan; v1 strategies (and any plan
// with an empty interval) fall back to the MVP default "1m" so a run
// without explicit cadence still produces a deterministic result.
func resolveInterval(plan *dslcompile.CompiledPlan) string {
	if plan != nil && plan.Interval != "" {
		return plan.Interval
	}
	return defaultFeatureInterval
}

// compileStrategy is the M4 cold-boundary: GET /strategy-versions/{id}, then
// dispatch.Parse + typed normalisation via dslcompile.Compile. It returns a
// CompiledPlan the worker can pass to downstream steps (M3 reader, future
// DSL executor) without any further JSON decoding.
//
// Errors from this function are stringified into bt.run.failed — they include:
//   - cpclient.APIError (non-2xx from control-plane, incl. ErrNotFound for 404);
//   - *dslcompile.ParseError wrapping a dispatch schema / semantic-hard error;
//   - *dslcompile.ColumnError for a FeatureSelector.Name unknown to
//     feature-parquet-v1;
//   - dslcompile.ErrUnsupportedVersion when schema_version is outside ^1./^2.
func compileStrategy(ctx context.Context, cp *cpclient.Client, strategyVersionID string) (*dslcompile.CompiledPlan, error) {
	sv, err := cp.GetStrategyVersion(ctx, strategyVersionID)
	if err != nil {
		return nil, fmt.Errorf("get strategy_version %s: %w", strategyVersionID, err)
	}
	if len(sv.DSLJSON) == 0 {
		return nil, fmt.Errorf("strategy_version %s: empty DSLJSON", strategyVersionID)
	}
	plan, err := dslcompile.Compile(sv.DSLJSON)
	if err != nil {
		return nil, fmt.Errorf("compile strategy_version %s (v%d): %w", strategyVersionID, sv.Version, err)
	}
	return plan, nil
}

// buildCompletedSummary marshals a stable, flat JSON object so control-plane
// can store it verbatim in experiment_runs.result_json and downstream readers
// can rely on fixed field names without joining ClickHouse.
func buildCompletedSummary(m runMetrics) (json.RawMessage, error) {
	out := map[string]any{
		"engine":           engineVersion,
		"run_id":           m.RunID,
		"symbol":           m.Symbol,
		"period_from":      m.PeriodFrom.Format(time.RFC3339),
		"period_to":        m.PeriodTo.Format(time.RFC3339),
		"trades_total":     m.TradesTotal,
		"trades_won":       m.TradesWon,
		"trades_lost":      m.TradesLost,
		"pnl_abs":          m.PnLAbs,
		"pnl_pct":          m.PnLPct,
		"sharpe_ratio":     m.SharpeRatio,
		"sortino_ratio":    m.SortinoRatio,
		"max_drawdown_abs": m.MaxDrawdownAbs,
		"max_drawdown_pct": m.MaxDrawdownPct,
		"profit_factor":    m.ProfitFactor,
		"expectancy":       m.Expectancy,
		"strategy_version": m.StrategyVersionID,
	}
	return json.Marshal(out)
}

func insertSummary(ctx context.Context, conn driver.Conn, runID, symbol string) error {
	return conn.Exec(ctx, `
		INSERT INTO backtest_run_summaries (run_id, symbol, engine_version, created_at)
		VALUES (?, ?, ?, ?)
	`, runID, symbol, engineVersion, time.Now().UTC())
}

func publishRunCompleted(js nats.JetStreamContext, runID, traceID string, summary json.RawMessage) error {
	p, err := json.Marshal(runCompletedPayload{RunID: runID, Summary: summary})
	if err != nil {
		return err
	}
	env := eventEnvelope{
		EventID:       uuid.NewString(),
		EventType:     eventRunCompleted,
		OccurredAtUTC: time.Now().UTC(),
		Producer:      "backtest-engine",
		TraceID:       traceID,
		Payload:       p,
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = js.Publish(eventRunCompleted, raw)
	return err
}

func publishRunFailed(js nats.JetStreamContext, runID, traceID, errMsg string) error {
	p, err := json.Marshal(runFailedPayload{RunID: runID, Error: errMsg})
	if err != nil {
		return err
	}
	env := eventEnvelope{
		EventID:       uuid.NewString(),
		EventType:     eventRunFailed,
		OccurredAtUTC: time.Now().UTC(),
		Producer:      "backtest-engine",
		TraceID:       traceID,
		Payload:       p,
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = js.Publish(eventRunFailed, raw)
	return err
}

func startHealthHTTPServer(port string, nc *nats.Conn, chConn driver.Conn) {
	mux := http.NewServeMux()
	registerPreflightRoutes(mux)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if nc == nil || !nc.IsConnected() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if err := chConn.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		slog.Info("backtest-engine health HTTP", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health http", "err", err)
		}
	}()
}
