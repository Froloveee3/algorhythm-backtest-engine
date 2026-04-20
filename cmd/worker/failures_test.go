package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/dslcompile"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/runtime"
)

func TestFormatRunFailure_RuntimeUnsupported(t *testing.T) {
	msg := formatRunFailure(errors.Join(runtime.ErrUnsupportedRuntime, errors.New("fill model unsupported")))
	if !strings.Contains(msg, "runtime_unsupported") {
		t.Fatalf("expected runtime_unsupported, got %q", msg)
	}
}

func TestFormatRunFailure_RuntimeDataInvalid(t *testing.T) {
	msg := formatRunFailure(errors.Join(runtime.ErrInvalidData, errors.New("frame missing")))
	if !strings.Contains(msg, "runtime_data_invalid") {
		t.Fatalf("expected runtime_data_invalid, got %q", msg)
	}
}

func TestFormatRunFailure_RuntimeExecutionFailed(t *testing.T) {
	msg := formatRunFailure(errors.Join(runtime.ErrExecutionFailed, errors.New("fill failed")))
	if !strings.Contains(msg, "runtime_execution_failed") {
		t.Fatalf("expected runtime_execution_failed, got %q", msg)
	}
}

func TestFormatRunFailure_ClickHouseWriteFailed(t *testing.T) {
	msg := formatRunFailure(errors.Join(ErrClickHouseWriteFailed, errors.New("insert failed")))
	if !strings.Contains(msg, "clickhouse_write_failed") {
		t.Fatalf("expected clickhouse_write_failed, got %q", msg)
	}
}

func TestExecuteV1Run_MissingTypedPlan_IsUnsupported(t *testing.T) {
	_, err := executeV1Run(context.Background(), nil, "run-1", "sv-1", "BTCUSDT", &dslcompile.CompiledPlan{}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, runtime.ErrUnsupportedRuntime) {
		t.Fatalf("expected ErrUnsupportedRuntime, got %v", err)
	}
}

func TestExecuteV1Run_MissingFrame_IsInvalidData(t *testing.T) {
	plan, err := dslcompile.Compile([]byte(v1StrategyJSON))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	_, err = executeV1Run(context.Background(), nil, "run-1", "sv-1", "BTCUSDT", plan, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, runtime.ErrInvalidData) {
		t.Fatalf("expected ErrInvalidData, got %v", err)
	}
}
