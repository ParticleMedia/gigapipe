package clickhouse_planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

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

func coveredNsExprBounds(bucketStartExpr string, d time.Duration, fromNs, toNs int64) string {
	if nowNs := time.Now().UnixNano(); toNs > nowNs {
		toNs = nowNs
	}
	return fmt.Sprintf("greatest(least(%s + %d, %d) - greatest(%s, %d), 1)",
		bucketStartExpr, d.Nanoseconds(), toNs, bucketStartExpr, fromNs)
}

func rateValueExpr(numerator, coveredNs string) string {
	return fmt.Sprintf("%s / (%s / 1e9)", numerator, coveredNs)
}

func totalValueExpr(total, coveredNs string, d time.Duration) string {
	return fmt.Sprintf("%s * (%d / %s)", total, d.Nanoseconds(), coveredNs)
}
