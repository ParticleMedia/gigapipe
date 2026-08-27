package shared

import (
	"context"
	"reflect"
	"testing"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func TestResolveTenants(t *testing.T) {
	long := make([]byte, maxTenantIDLength+1)
	for i := range long {
		long[i] = 'a'
	}
	cases := []struct {
		in   string
		want []string
		ok   bool
	}{
		// valid: alphanumerics, '-'/'_'.
		{"platform", []string{"platform"}, true},
		{"a|b|c", []string{"a", "b", "c"}, true},
		{"team-a_1", []string{"team-a_1"}, true},
		{"foo.bar", []string{"foo.bar"}, true},
		{"team*", []string{"team*"}, false},
		{"x!('y')", []string{"x!('y')"}, false},
		// dedupe + sort, and whitespace trimming around each tenant.
		{"c|a|a", []string{"a", "c"}, true},
		{" a | b ", []string{"a", "b"}, true},
		// invalid -> fail closed (ok=false): the caller sets Deny.
		{"", nil, false},
		{"a||b", nil, false},
		{"a/b", nil, false},
		{`a\b`, nil, false},
		{"a b", nil, false},
		{".", nil, false},
		{"..", nil, false},
		{string(long), nil, false},
	}
	for _, c := range cases {
		got, ok := ResolveTenants(c.in)
		if ok != c.ok || (ok && !reflect.DeepEqual(got, c.want)) {
			t.Errorf("ResolveTenants(%q) = (%v,%v), want (%v,%v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestOidFilterFor(t *testing.T) {
	if f := OidFilterFor("a|b"); f.Deny || !reflect.DeepEqual(f.Tenants, []string{"a", "b"}) {
		t.Errorf("OidFilterFor(valid) = %+v, want Tenants=[a b], Deny=false", f)
	}
	if f := OidFilterFor("a/b"); !f.Deny {
		t.Errorf("OidFilterFor(invalid) = %+v, want Deny=true", f)
	}
}

func TestBuildOidCondition(t *testing.T) {
	renderSqlToString := func(c sql.SQLCondition) string {
		s, err := c.String(&sql.Ctx{})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	cases := []struct {
		name string
		f    OidFilter
		want string
	}{
		{"single", OidFilter{Tenants: []string{"platform"}}, "(oid) == ('platform')"},
		{"many", OidFilter{Tenants: []string{"a", "b", "c"}}, "oid IN ('a','b','c')"},
		{"deny", OidFilter{Deny: true}, "(1) == (0)"},
		{"empty", OidFilter{}, "(1) == (0)"},
		{"escaped", OidFilter{Tenants: []string{"o'brien"}}, `(oid) == ('o\'brien')`},
	}
	for _, c := range cases {
		if got := renderSqlToString(BuildOidCondition("oid", c.f)); got != c.want {
			t.Errorf("%s: BuildOidCondition = %q, want %q", c.name, got, c.want)
		}
	}
	// qualified column
	if got := renderSqlToString(BuildOidCondition("samples.oid", OidFilter{Tenants: []string{"a", "b"}})); got != "samples.oid IN ('a','b')" {
		t.Errorf("qualified = %q, want samples.oid IN ('a','b')", got)
	}
}

func TestOidConditionFromContext_OffIsNil(t *testing.T) {
	// federation disabled by default in tests (FEDERATED unset).
	if c := OidConditionFromContext(context.Background(), "samples"); c != nil {
		t.Fatalf("expected nil condition when federation is off, got %v", c)
	}
}
