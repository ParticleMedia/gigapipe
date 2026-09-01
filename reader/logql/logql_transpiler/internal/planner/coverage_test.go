package planner

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

// Two 5m bins: bin 0 is fully covered, bin 1 covers only 195s of its 300s (the
// partial trailing bin an instant query reports). At a steady 1 event/s the true
// rate is 1.0 and the full-window count is 300 in both bins.
func newCoverageCtx() *shared.PlannerContext {
	from := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	return &shared.PlannerContext{
		From: from,
		To:   from.Add(5*time.Minute + 195*time.Second),
	}
}

func TestLRAFinalizeRateNormalizesPartialBin(t *testing.T) {
	ctx := newCoverageCtx()
	l := &LRAPlanner{Func: "rate", AggregatorPlanner: AggregatorPlanner{Duration: 5 * time.Minute}}
	s := &aggOpStream{values: []float64{300, 1, 195, 1}} // bin0: 300 events, bin1: 195 events
	l.finalize(ctx, s)

	if got := s.values[0]; got != 1.0 {
		t.Errorf("complete bin rate = %v, want 1.0", got)
	}
	if got := s.values[2]; got != 1.0 {
		t.Errorf("partial bin rate = %v, want 1.0 (normalized by 195s, not 300s)", got)
	}
}

// The reader aligns a live range query's window UP to the range-vector duration,
// so ctx.To can be a future bin boundary. coveredNs must clamp to now, otherwise
// the still-filling trailing bin looks fully covered and normalization no-ops
// (the residual-dropoff bug). With From = now-7.5m and a far-future To, bin 0 is
// complete (~5m) and bin 1 straddles now (~2.5m covered), regardless of To.
func TestCoveredNsClampsFutureTo(t *testing.T) {
	d := 5 * time.Minute
	ctx := &shared.PlannerContext{
		From: time.Now().Add(-7*time.Minute - 30*time.Second),
		To:   time.Now().Add(1 * time.Hour), // future, bin-aligned in the real path
	}
	const tol = int64(5 * time.Second)

	if got := coveredNs(ctx, 0, d); abs64(got-d.Nanoseconds()) > tol {
		t.Errorf("complete bin covered = %dns, want ~%dns", got, d.Nanoseconds())
	}
	if got, want := coveredNs(ctx, 1, d), (2*time.Minute + 30*time.Second).Nanoseconds(); abs64(got-want) > tol {
		t.Errorf("straddling bin covered = %dns, want ~%dns (clamped to now, not To)", got, want)
	}
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func TestLRAFinalizeCountExtrapolatesPartialBin(t *testing.T) {
	ctx := newCoverageCtx()
	l := &LRAPlanner{Func: "count_over_time", AggregatorPlanner: AggregatorPlanner{Duration: 5 * time.Minute}}
	s := &aggOpStream{values: []float64{300, 1, 195, 1}}
	l.finalize(ctx, s)

	if got := s.values[0]; got != 300 {
		t.Errorf("complete bin count = %v, want 300 (unchanged)", got)
	}
	if got := s.values[2]; got != 300 {
		t.Errorf("partial bin count = %v, want 300 (extrapolated from 195)", got)
	}
}
