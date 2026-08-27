package clickhouse_planner

import (
	"strings"
	"testing"

	log_parser "github.com/metrico/qryn/v5/reader/logql/logql_parser"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

func lineFilterSQL(t *testing.T, fn, quotedVal string) string {
	t.Helper()
	planner := &LineFilterPlanner{
		Main: staticPlanner{mockMain()},
		Filter: &log_parser.LineFilter{
			Fn: fn,
			Exp: log_parser.LineFilterExp{
				Head: log_parser.LineFilterHead{
					Simple: &log_parser.LineFilterSimple{
						Val: log_parser.QuotedString{Str: quotedVal},
					},
				},
			},
		},
	}
	sel, err := planner.Process(&shared.PlannerContext{})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	got, err := sel.String(newCtx())
	if err != nil {
		t.Fatalf("String() error = %v", err)
	}
	return got
}

// |= is optimized into a token-boundary hasPhrase() match: the value is trimmed,
// split on non-alphanumeric characters, and rejoined with single spaces.
func TestLineFilterEqualsUsesHasPhrase(t *testing.T) {
	cases := []struct {
		name  string
		val   string // quoted, as it appears in LogQL
		want  string
	}{
		{"plain phrase", `"bar baz"`, `hasPhrase(samples.string, 'bar baz')`},
		{"non-alnum separator", `"bar=baz"`, `hasPhrase(samples.string, 'bar baz')`},
		{"surrounding whitespace", `"  bar baz  "`, `hasPhrase(samples.string, 'bar baz')`},
		{"collapsed separators", `"bar==baz"`, `hasPhrase(samples.string, 'bar baz')`},
		{"single token", `"bar"`, `hasPhrase(samples.string, 'bar')`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := lineFilterSQL(t, "|=", c.val)
			if !strings.Contains(got, c.want) {
				t.Fatalf("expected %q in:\n%s", c.want, got)
			}
		})
	}
}

// A |= value with no alphanumeric characters cannot form a phrase, so it falls
// back to the original unprocessed substring LIKE.
func TestLineFilterEqualsFallsBackToLike(t *testing.T) {
	got := lineFilterSQL(t, "|=", `"==="`)
	if !strings.Contains(got, `like(samples.string, '%===%')`) {
		t.Fatalf("expected LIKE fallback in:\n%s", got)
	}
	if strings.Contains(got, "hasPhrase") {
		t.Fatalf("did not expect hasPhrase for non-alphanumeric value:\n%s", got)
	}
}

// |~ (and the other operators) must keep their existing behavior: a literal
// regex collapses to LIKE and still matches substrings ("bar" matches "foobar").
func TestLineFilterRegexUnchanged(t *testing.T) {
	got := lineFilterSQL(t, "|~", `"bar"`)
	if !strings.Contains(got, `like(samples.string, '%bar%')`) {
		t.Fatalf("expected |~ literal to stay LIKE in:\n%s", got)
	}

	got = lineFilterSQL(t, "|~", `"ba.z"`)
	if !strings.Contains(got, `match(string, 'ba.z')`) {
		t.Fatalf("expected |~ regex to stay match() in:\n%s", got)
	}
}

// != is a substring negation and must stay a notLike.
func TestLineFilterNotEqualsUnchanged(t *testing.T) {
	got := lineFilterSQL(t, "!=", `"bar=baz"`)
	if !strings.Contains(got, `notLike(samples.string, '%bar=baz%')`) {
		t.Fatalf("expected != to stay notLike in:\n%s", got)
	}
}
