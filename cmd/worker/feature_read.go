package main

import (
	"context"
	"fmt"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/dslcompile"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/runresolve"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/storage"
)

// readFeatureData runs featuredata.ReadFeatureFrame when store is non-nil
// (BT_FEATURE_READ_FRAME=true and MinIO configured). When store is nil the
// worker skips object storage entirely and returns (nil, nil) so the legacy
// simulator path stays unchanged for local dev without MinIO.
func readFeatureData(
	ctx context.Context,
	store storage.ObjectReader,
	plan *dslcompile.CompiledPlan,
	resolved *runresolve.Resolved,
) (*featuredata.FeatureFrame, error) {
	if store == nil {
		return nil, nil
	}
	req := plan.NewReadRequest(resolved.Key, resolved.Symbol, resolved.Partitions)
	return featuredata.ReadFeatureFrame(ctx, store, req, nil)
}

// formatFeatureReadFailure turns reader errors into a compact message for
// bt.run.failed while keeping the stable Reason prefix when present.
func formatFeatureReadFailure(err error) string {
	if err == nil {
		return ""
	}
	if re, ok := featuredata.AsReaderError(err); ok {
		return fmt.Sprintf("feature read [%s]: %s", re.Reason, re.Message)
	}
	return fmt.Sprintf("feature read: %v", err)
}
