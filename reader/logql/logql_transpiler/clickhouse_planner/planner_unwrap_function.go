package clickhouse_planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type UnwrapFunctionPlanner struct {
	Main       shared.SQLRequestPlanner
	Func       string
	Duration   time.Duration
	WithLabels bool
	Offset     *time.Duration
}

func (u *UnwrapFunctionPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := u.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	withMain := sql.NewWith(main, "unwrap_1")

	// Normalize by the bin's actually-covered span rather than the fixed window
	// width (see coverage.go). Bucket-start matches the timestamp_ns group key.
	bucketStart := fmt.Sprintf("intDiv(timestamp_ns, %d) * %[1]d", u.Duration.Nanoseconds())
	covered := coveredNsExpr(ctx, bucketStart, u.Duration, u.Offset)

	var val sql.SQLObject
	switch u.Func {
	case "rate":
		val = sql.NewRawObject(rateValueExpr("sum(unwrap_1.value)", covered))
	case "sum_over_time":
		val = sql.NewRawObject(totalValueExpr("sum(unwrap_1.value)", covered, u.Duration))
	case "avg_over_time":
		val = sql.NewRawObject("avg(unwrap_1.value)")
	case "max_over_time":
		val = sql.NewRawObject("max(unwrap_1.value)")
	case "min_over_time":
		val = sql.NewRawObject("min(unwrap_1.value)")
	case "first_over_time":
		val = sql.NewRawObject("argMin(unwrap_1.value, unwrap_1.timestamp_ns)")
	case "last_over_time":
		val = sql.NewRawObject("argMax(unwrap_1.value, unwrap_1.timestamp_ns)")
	case "stdvar_over_time":
		val = sql.NewRawObject("varPop(unwrap_1.value)")
	case "stddev_over_time":
		val = sql.NewRawObject("stddevPop(unwrap_1.value)")
	}

	res := sql.NewSelect().With(withMain).Select(
		sql.NewSimpleCol(
			fmt.Sprintf("intDiv(timestamp_ns, %d) * %[1]d", u.Duration.Nanoseconds()),
			"timestamp_ns"),
		sql.NewRawObject("fingerprint"),
		sql.NewSimpleCol(`''`, "string"),
		sql.NewCol(val, "value"),
		sql.NewSimpleCol("any(labels)", "labels")).
		From(sql.NewWithRef(withMain)).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ns"))

	return res, nil
}
