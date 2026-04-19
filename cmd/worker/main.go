package main

import (
	"bytes"
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
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

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

	httpClient := &http.Client{Timeout: 60 * time.Second}

	sub, err := js.QueueSubscribe(eventRunRequested, "backtest-engine", func(msg *nats.Msg) {
		if err := handleRunRequested(ctx, js, httpClient, cpURL, chConn, msg); err != nil {
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
	httpClient *http.Client,
	cpURL string,
	chConn driver.Conn,
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

	if err := patchExperimentRunStatus(ctx, httpClient, cpURL, payload.RunID, "running", nil); err != nil {
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
		return nil
	}

	// backtest_run_summaries (from 001_init) stays as the cheap "one row per
	// run" marker some dashboards still scan. New canonical tables from
	// migration 002 now carry the real payload.
	if err := insertSummary(ctx, chConn, payload.RunID, payload.Symbol); err != nil {
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
		return nil
	}

	// Placeholder simulator: deterministic synthetic trades/equity/metrics by
	// run_id. Replace with DSL-interpreted run once feature parquet reader
	// lands (stage-3 DoD item). Contract for downstream consumers is already
	// stable because the canonical CH schema is final.
	sim := simulateRun(payload.RunID, payload.StrategyVersionID, payload.Symbol)
	if err := writeSimulationResult(ctx, chConn, sim); err != nil {
		slog.Error("write simulation result failed", "err", err, "run_id", payload.RunID)
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
		return nil
	}

	summary, err := buildCompletedSummary(sim.Metrics)
	if err != nil {
		slog.Error("build summary failed", "err", err, "run_id", payload.RunID)
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
		return nil
	}

	// Tell control-plane the run is done and hand back the metrics summary so
	// it lands in experiment_runs.result_json in one roundtrip.
	if err := patchExperimentRunStatus(ctx, httpClient, cpURL, payload.RunID, "completed", summary); err != nil {
		slog.Error("patch completed status failed", "err", err, "run_id", payload.RunID)
		// Still publish on NATS so the orchestrator has a signal.
	}

	if err := publishRunCompleted(js, payload.RunID, envelope.TraceID, summary); err != nil {
		slog.Error("publish completed failed", "err", err, "run_id", payload.RunID)
		_ = publishRunFailed(js, payload.RunID, envelope.TraceID, err.Error())
	}
	return nil
}

// buildCompletedSummary marshals a stable, flat JSON object so control-plane
// can store it verbatim in experiment_runs.result_json and downstream readers
// can rely on fixed field names without joining ClickHouse.
func buildCompletedSummary(m runMetrics) (json.RawMessage, error) {
	out := map[string]any{
		"engine":            engineVersion,
		"run_id":            m.RunID,
		"symbol":            m.Symbol,
		"period_from":       m.PeriodFrom.Format(time.RFC3339),
		"period_to":         m.PeriodTo.Format(time.RFC3339),
		"trades_total":      m.TradesTotal,
		"trades_won":        m.TradesWon,
		"trades_lost":       m.TradesLost,
		"pnl_abs":           m.PnLAbs,
		"pnl_pct":           m.PnLPct,
		"sharpe_ratio":      m.SharpeRatio,
		"sortino_ratio":     m.SortinoRatio,
		"max_drawdown_abs":  m.MaxDrawdownAbs,
		"max_drawdown_pct":  m.MaxDrawdownPct,
		"profit_factor":     m.ProfitFactor,
		"expectancy":        m.Expectancy,
		"strategy_version":  m.StrategyVersionID,
	}
	return json.Marshal(out)
}

func patchExperimentRunStatus(ctx context.Context, client *http.Client, baseURL, runID, status string, result json.RawMessage) error {
	body := map[string]any{"status": status}
	if len(result) > 0 {
		body["result"] = result
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, baseURL+"/api/v1/experiment-runs/"+url.PathEscape(runID)+"/status", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("control-plane PATCH status %d", resp.StatusCode)
	}
	return nil
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
