package planner

import (
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

// coveredNs returns how many nanoseconds of the fixed bin at bucketIndex actually
// fall within the query range [From, To). Bins are aligned to From with width d
// (matching the `(ts-From)/d` bucketing in addValue), so leading/trailing bins are
// only partially covered. Normalizing by this span instead of the nominal width d
// removes the incomplete-bucket artifact (see plan.md). The result is clamped to a
// minimum of 1ns to avoid divide-by-zero.
func coveredNs(ctx *shared.PlannerContext, bucketIndex int, d time.Duration) int64 {
	bucketStart := ctx.From.UnixNano() + int64(bucketIndex)*d.Nanoseconds()
	// Clamp the upper bound to now: the reader aligns a range query's window up to
	// the range-vector duration, so ctx.To for a live query is a future bin-aligned
	// instant. Without this, the trailing still-filling bin computes as fully
	// covered and the normalization no-ops.
	toNs := ctx.To.UnixNano()
	if nowNs := time.Now().UnixNano(); toNs > nowNs {
		toNs = nowNs
	}
	covered := min(bucketStart+d.Nanoseconds(), toNs) - max(bucketStart, ctx.From.UnixNano())
	if covered <= 0 {
		covered = 1
	}
	return covered
}
