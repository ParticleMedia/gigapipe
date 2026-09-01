package clickhouse_planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type LRAPlanner struct {
	Main       shared.SQLRequestPlanner
	Duration   time.Duration
	Func       string
	WithLabels bool
	Offset     *time.Duration
}

func (l *LRAPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := l.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	cols := main.GetSelect()
	for i, c := range cols {
		_c, ok := c.(sql.Aliased)
		if !ok {
			continue
		}
		if _c.GetAlias() == "string" {
			cols[i] = sql.NewCol(_c.GetExpr(), "_string")
		}
	}

	// Normalize by the bin's actually-covered span rather than the fixed window
	// width, so a partial leading/trailing bin is not under-reported (see
	// coverage.go). The bucket-start expression matches the timestamp_ns group key
	// below.
	bucketStart := fmt.Sprintf("intDiv(time_series.timestamp_ns, %d) * %[1]d", l.Duration.Nanoseconds())
	covered := coveredNsExpr(ctx, bucketStart, l.Duration, l.Offset)

	var col sql.SQLObject
	switch l.Func {
	case "rate":
		col = sql.NewRawObject(rateValueExpr("toFloat64(COUNT())", covered))
	case "count_over_time":
		col = sql.NewRawObject(totalValueExpr("toFloat64(COUNT())", covered, l.Duration))
	case "bytes_rate":
		col = sql.NewRawObject(rateValueExpr("toFloat64(sum(length(_string)))", covered))
	case "bytes_over_time":
		col = sql.NewRawObject(totalValueExpr("toFloat64(sum(length(_string)))", covered, l.Duration))
	}

	withAgg := sql.NewWith(main, "agg_a")
	res := sql.NewSelect().With(withAgg).
		Select(
			sql.NewSimpleCol(
				fmt.Sprintf("intDiv(time_series.timestamp_ns, %d) * %[1]d", l.Duration.Nanoseconds()),
				"timestamp_ns",
			),
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol(`''`, "string"),
			sql.NewCol(col, "value"),
		).
		From(sql.NewCol(sql.NewWithRef(withAgg), "time_series")).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ns"))
	if l.WithLabels {
		res.Select(append(res.GetSelect(), sql.NewSimpleCol("any(labels)", "labels"))...)
	}
	return res, nil
}
