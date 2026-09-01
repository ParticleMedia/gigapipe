package clickhouse_planner

import (
	"strings"
	"testing"
	"time"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/reader/config"
	logql_parser "github.com/metrico/qryn/v5/reader/logql/logql_parser"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// rangeAggSQL transpiles a LogQL range-aggregation expression the way the ruler's
// instant query does — [now-5m, now) with a 1s step — and returns the generated
// SQL.
func rangeAggSQL(t *testing.T, script string) string {
	t.Helper()
	if config.Cloki == nil {
		config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	}
	ast, err := logql_parser.Parse(script)
	if err != nil {
		t.Fatalf("parse %q: %v", script, err)
	}
	planner, err := Plan(ast, true)
	if err != nil {
		t.Fatalf("plan %q: %v", script, err)
	}
	now := time.Date(2026, 9, 1, 12, 3, 20, 0, time.UTC)
	ctx := &shared.PlannerContext{
		From:                    now.Add(-5 * time.Minute),
		To:                      now,
		Step:                    time.Second,
		SamplesTableName:        "samples_v3",
		SamplesDistTableName:    "samples_v3",
		TimeSeriesTableName:     "time_series",
		TimeSeriesDistTableName: "time_series",
		TimeSeriesGinTableName:  "time_series_gin",
		CHSqlCtx:                &sql.Ctx{Params: map[string]sql.SQLObject{}, Result: map[string]sql.SQLObject{}},
	}
	sel, err := planner.Process(ctx)
	if err != nil {
		t.Fatalf("process %q: %v", script, err)
	}
	got, err := sel.String(ctx.CHSqlCtx)
	if err != nil {
		t.Fatalf("string %q: %v", script, err)
	}
	return got
}

// rate must divide the count by the bin's actually-covered span (least/greatest
// bounded, /1e9), never by the fixed nominal window width. The fixed `/ 300...`
// divisor is exactly the incomplete-bucket bug.
func TestRateUsesCoveredDurationDivisor(t *testing.T) {
	got := rangeAggSQL(t, `sum(rate({service="dummy-app-platform"}[5m]))`)
	if !strings.Contains(got, "least(") || !strings.Contains(got, "greatest(") || !strings.Contains(got, "/ 1e9") {
		t.Fatalf("rate should normalize by covered span; got:\n%s", got)
	}
	if strings.Contains(got, "/ 300.000000") {
		t.Fatalf("rate still uses fixed-window divisor:\n%s", got)
	}
}

// count_over_time is total-form: it extrapolates the partial bin by multiplying by
// D/covered rather than emitting a raw (partial) count.
func TestCountOverTimeUsesCoverageExtrapolation(t *testing.T) {
	got := rangeAggSQL(t, `sum(count_over_time({service="dummy-app-platform"}[5m]))`)
	if !strings.Contains(got, "* (300000000000 /") {
		t.Fatalf("count_over_time should extrapolate by D/covered; got:\n%s", got)
	}
	if !strings.Contains(got, "least(") || !strings.Contains(got, "greatest(") {
		t.Fatalf("count_over_time should reference the covered span; got:\n%s", got)
	}
}
