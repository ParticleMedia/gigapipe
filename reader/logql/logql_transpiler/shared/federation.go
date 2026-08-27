package shared

import (
	"context"
	"net/http"
	"sort"
	"strings"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/metrico/qryn/v5/shared/federation"
)

type oidCtxKey struct{}

// OidFilter captures the scoping decision PER-REQUEST
type OidFilter struct {
	Tenants []string
	Deny    bool
}

const maxTenantIDLength = 150

func isSupportedTenantRune(r rune) bool {
	if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
		return true
	}
	return strings.ContainsRune("-_.", r)
}

func validTenantID(s string) bool {
	if s == "" || len(s) > maxTenantIDLength {
		return false
	}
	if s == "." || s == ".." {
		return false
	}
	for _, r := range s {
		if !isSupportedTenantRune(r) {
			return false
		}
	}
	return true
}

func ResolveTenants(raw string) (tenants []string, ok bool) {
	if raw == "" {
		return nil, false
	}
	parts := strings.Split(raw, "|")
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if !validTenantID(t) {
			return nil, false
		}
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		tenants = append(tenants, t)
	}
	sort.Strings(tenants)
	return tenants, true
}

func OidFilterFor(raw string) OidFilter {
	tenants, ok := ResolveTenants(raw)
	if !ok {
		return OidFilter{Deny: true}
	}
	return OidFilter{Tenants: tenants}
}

func ReadOidPreRequestPlugin(ctx context.Context, req *http.Request) (context.Context, error) {
	if !federation.Enabled() {
		return ctx, nil
	}
	f := OidFilterFor(req.Header.Get("X-Scope-OrgID"))
	return context.WithValue(ctx, oidCtxKey{}, f), nil
}

func OidFilterFromContext(ctx context.Context) OidFilter {
	if ctx == nil {
		return OidFilter{}
	}
	if f, ok := ctx.Value(oidCtxKey{}).(OidFilter); ok {
		return f
	}
	return OidFilter{}
}

func WithOidFilter(ctx context.Context, f OidFilter) context.Context {
	return context.WithValue(ctx, oidCtxKey{}, f)
}

// BuildOidCondition constructs the tenant-scoped predicate for column col:
//
//   - Deny or no tenants -> 1=0   (fail closed to empty)
//   - exactly one tenant -> oid = 'a'
//   - many tenants       -> oid IN ('a','b',...)
func BuildOidCondition(col string, f OidFilter) sql.SQLCondition {
	if f.Deny || len(f.Tenants) == 0 {
		return sql.Eq(sql.NewIntVal(1), sql.NewIntVal(0))
	}
	left := sql.NewRawObject(col)
	if len(f.Tenants) == 1 {
		return sql.Eq(left, sql.NewStringVal(f.Tenants[0]))
	}
	vals := make([]sql.SQLObject, len(f.Tenants))
	for i, t := range f.Tenants {
		vals[i] = sql.NewStringVal(t)
	}
	return sql.NewIn(left, vals...)
}

func OidConditionFromContext(ctx context.Context, tableAlias string) sql.SQLCondition {
	if !federation.Enabled() {
		return nil
	}
	col := "oid"
	if tableAlias != "" {
		col = tableAlias + ".oid"
	}
	return BuildOidCondition(col, OidFilterFromContext(ctx))
}
