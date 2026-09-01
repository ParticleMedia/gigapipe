package clickhouse_planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

// coveredNsExpr renders a ClickHouse expression for how many nanoseconds of the
// epoch-aligned bin [B, B+d) actually fall within the scanned range [From, To),
// where B is the bucket-start expression (typically intDiv(ts, d)*d).
//
// Range-aggregation bins are fixed and epoch-aligned, so the leading and trailing
// bins of a query are only partially covered by the scan range. Normalizing by
// this covered span instead of the nominal window width d is what removes the
// incomplete-bucket artifact (see plan.md): interior bins are fully covered so the
// span equals d and their value is unchanged, while a partial trailing bin — the
// one an instant/recording-rule query reports — is normalized by the time actually
// elapsed rather than the full window.
//
// offset mirrors the offset the stream select applied to the scan bounds: an
// explicit modifier when present, otherwise the context offset.
func coveredNsExpr(ctx *shared.PlannerContext, bucketStartExpr string, d time.Duration, offset *time.Duration) string {
	fromNs := ctx.From.UnixNano()
	toNs := ctx.To.UnixNano()
	switch {
	case offset != nil:
		fromNs += offset.Nanoseconds()
		toNs += offset.Nanoseconds()
	case ctx.Offset.Nanoseconds() != 0:
		fromNs += ctx.Offset.Nanoseconds()
		toNs += ctx.Offset.Nanoseconds()
	}
	return coveredNsExprBounds(bucketStartExpr, d, fromNs, toNs)
}

// coveredNsExprBounds is the low-level form of coveredNsExpr for callers whose
// effective scan bounds differ from ctx.From/To (e.g. the metrics_15s shortcut,
// which floors bounds to 15s and works in offset-shifted timestamp space).
// fromNs and toNs must be expressed in the same coordinate space as
// bucketStartExpr.
//
// The upper bound is clamped to the current wall-clock time: the reader aligns a
// range query's window UP to the range-vector duration, so toNs for a live query
// is a future bin-aligned instant. Without this clamp the trailing (still-filling)
// bin would compute as fully covered and the normalization would no-op, leaving
// the incomplete-bucket dropoff in place. A future bin has no data anyway, so the
// result is floored at 1ns to keep the divisor positive.
func coveredNsExprBounds(bucketStartExpr string, d time.Duration, fromNs, toNs int64) string {
	if nowNs := time.Now().UnixNano(); toNs > nowNs {
		toNs = nowNs
	}
	return fmt.Sprintf("greatest(least(%s + %d, %d) - greatest(%s, %d), 1)",
		bucketStartExpr, d.Nanoseconds(), toNs, bucketStartExpr, fromNs)
}

// rateValueExpr renders `numerator / coveredSeconds` for rate-form aggregations
// (rate, bytes_rate): a per-second value normalized by the bin's actually-covered
// span rather than the fixed window width.
func rateValueExpr(numerator, coveredNs string) string {
	return fmt.Sprintf("%s / (%s / 1e9)", numerator, coveredNs)
}

// totalValueExpr renders `total * d / coveredNs` for total-form aggregations
// (count_over_time, sum_over_time, bytes_over_time): the partial bin's total is
// extrapolated to a full-window estimate. Interior bins have coveredNs == d, so
// the factor is 1 and the total is unchanged.
func totalValueExpr(total, coveredNs string, d time.Duration) string {
	return fmt.Sprintf("%s * (%d / %s)", total, d.Nanoseconds(), coveredNs)
}
