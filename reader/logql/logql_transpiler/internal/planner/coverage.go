package planner

import (
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

func coveredNs(ctx *shared.PlannerContext, bucketIndex int, d time.Duration) int64 {
	bucketStart := ctx.From.UnixNano() + int64(bucketIndex)*d.Nanoseconds()
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
