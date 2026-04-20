package main

import (
	"errors"
	"fmt"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/runtime"
)

var ErrClickHouseWriteFailed = errors.New("worker: clickhouse write failed")

type failureReason string

const (
	reasonRuntimeUnsupported  failureReason = "runtime_unsupported"
	reasonRuntimeDataInvalid  failureReason = "runtime_data_invalid"
	reasonRuntimeExecution    failureReason = "runtime_execution_failed"
	reasonClickHouseWrite     failureReason = "clickhouse_write_failed"
)

func formatRunFailure(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, ErrClickHouseWriteFailed):
		return fmt.Sprintf("%s: %v", reasonClickHouseWrite, err)
	case errors.Is(err, runtime.ErrUnsupportedRuntime):
		return fmt.Sprintf("%s: %v", reasonRuntimeUnsupported, err)
	case errors.Is(err, runtime.ErrInvalidData):
		return fmt.Sprintf("%s: %v", reasonRuntimeDataInvalid, err)
	case errors.Is(err, runtime.ErrExecutionFailed):
		return fmt.Sprintf("%s: %v", reasonRuntimeExecution, err)
	default:
		return err.Error()
	}
}
