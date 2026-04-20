package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
)

func TestFormatFeatureReadFailure_ReaderError(t *testing.T) {
	err := &featuredata.ReaderError{
		Reason:  featuredata.ReasonFeaturePartitionMissing,
		Message: "object foo not found",
		Cause:   errors.New("underlying"),
	}
	s := formatFeatureReadFailure(err)
	if !strings.Contains(s, string(featuredata.ReasonFeaturePartitionMissing)) {
		t.Fatalf("expected reason in message: %q", s)
	}
	if !strings.Contains(s, "object foo not found") {
		t.Fatalf("expected detail in message: %q", s)
	}
}

func TestFormatFeatureReadFailure_Generic(t *testing.T) {
	s := formatFeatureReadFailure(errors.New("boom"))
	if s != "feature read: boom" {
		t.Fatalf("got %q", s)
	}
}

func TestReadFeatureData_StoreNil_Skips(t *testing.T) {
	// No MinIO client: must not panic and must not attempt a read.
	frame, err := readFeatureData(t.Context(), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if frame != nil {
		t.Fatalf("expected nil frame, got %+v", frame)
	}
}
