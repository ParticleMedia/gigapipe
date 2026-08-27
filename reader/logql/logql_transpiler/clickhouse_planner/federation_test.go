package clickhouse_planner

import (
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func renderCond(t *testing.T, c sql.SQLCondition) string {
	t.Helper()
	if c == nil {
		return "<nil>"
	}
	s, err := c.String(&sql.Ctx{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestGetOidFilter_NotFederatedIsNil(t *testing.T) {
	if f := GetOidFilter(&shared.PlannerContext{Federated: false}, "samples"); f != nil {
		t.Fatalf("expected nil when not federated, got %q", renderCond(t, f))
	}
}

func TestGetOidFilter_DenyIsFalse(t *testing.T) {
	ctx := &shared.PlannerContext{Federated: true, OidFilter: shared.OidFilter{Deny: true}}
	if got := renderCond(t, GetOidFilter(ctx, "samples")); got != "(1) == (0)" {
		t.Errorf("deny filter = %q, want (1) == (0)", got)
	}
}

func TestGetOidFilter_NoTenantsIsFalse(t *testing.T) {
	ctx := &shared.PlannerContext{Federated: true, OidFilter: shared.OidFilter{}}
	if got := renderCond(t, GetOidFilter(ctx, "")); got != "(1) == (0)" {
		t.Errorf("no-tenants filter = %q, want (1) == (0)", got)
	}
}

func TestGetOidFilter_MultiTenantIn(t *testing.T) {
	ctx := &shared.PlannerContext{Federated: true, OidFilter: shared.OidFilter{Tenants: []string{"platform", "data"}}}
	got := renderCond(t, GetOidFilter(ctx, "samples"))
	want := `samples.oid IN ('platform','data')`
	if got != want {
		t.Errorf("multi-tenant filter = %q, want %q", got, want)
	}
}

func TestGetOidFilter_SingleTenantEq(t *testing.T) {
	ctx := &shared.PlannerContext{Federated: true, OidFilter: shared.OidFilter{Tenants: []string{"platform"}}}
	got := renderCond(t, GetOidFilter(ctx, ""))
	want := `(oid) == ('platform')`
	if got != want {
		t.Errorf("single-tenant filter = %q, want %q", got, want)
	}
}
