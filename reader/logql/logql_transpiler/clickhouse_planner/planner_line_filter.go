package clickhouse_planner

import (
	"fmt"
	"regexp/syntax"
	"strings"

	log_parser "github.com/metrico/qryn/v5/reader/logql/logql_parser"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type LineFilterPlanner struct {
	Filter *log_parser.LineFilter
	Main   shared.SQLRequestPlanner
}

func (l *LineFilterPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	req, err := l.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	clause, err := l.buildExpCondition(l.Filter.Fn, &l.Filter.Exp)
	if err != nil {
		return nil, err
	}
	return req.AndWhere(clause), nil
}

func (l *LineFilterPlanner) buildExpCondition(fn string, exp *log_parser.LineFilterExp) (sql.SQLCondition, error) {
	head, err := l.buildHeadCondition(fn, &exp.Head)
	if err != nil {
		return nil, err
	}
	if exp.Op == "" {
		return head, nil
	}
	tail, err := l.buildExpCondition(fn, exp.Tail)
	if err != nil {
		return nil, err
	}
	if exp.Op == "or" {
		return sql.Or(head, tail), nil
	}
	return sql.And(head, tail), nil
}

func (l *LineFilterPlanner) buildHeadCondition(fn string, head *log_parser.LineFilterHead) (sql.SQLCondition, error) {
	if head.Complex != nil {
		return l.buildExpCondition(fn, head.Complex)
	}
	return l.buildSimpleCondition(fn, head.Simple)
}

func (l *LineFilterPlanner) buildSimpleCondition(fn string, s *log_parser.LineFilterSimple) (sql.SQLCondition, error) {
	val, err := s.Val.Unquote()
	if err != nil {
		return nil, err
	}
	tmp := &lineFilterOps{val: val}
	switch fn {
	case "|=":
		return tmp.doHasPhrase()
	case "!=":
		return tmp.doLike("notLike")
	case "|~":
		likeStr, isInsensitive, isLike := tmp.re2Like()
		if isLike {
			tmp.val = likeStr
			like := "like"
			if isInsensitive {
				like = "ilike"
			}
			return tmp.doLike(like)
		}
		return sql.Eq(&SqlMatch{
			col:     sql.NewRawObject("string"),
			pattern: val,
		}, sql.NewIntVal(1)), nil
	case "!~":
		likeStr, isInsensitive, isLike := tmp.re2Like()
		if isLike {
			tmp.val = likeStr
			like := "notLike"
			if isInsensitive {
				like = "notILike"
			}
			return tmp.doLike(like)
		}
		return sql.Eq(&SqlMatch{
			col:     sql.NewRawObject("string"),
			pattern: val,
		}, sql.NewIntVal(1)), nil
	case "|>":
		likeOp := patternMatch(val)
		return sql.BinaryLogicalOp("LIKE", sql.NewRawObject("string"), sql.NewStringVal(likeOp)), nil
	}
	return nil, &shared.NotSupportedError{Msg: fmt.Sprintf("%s not supported", fn)}
}

// lineFilterOps holds per-value helpers (extracted from the old LineFilterPlanner methods).
type lineFilterOps struct {
	val string
}

func (o *lineFilterOps) doLike(likeOp string) (sql.SQLCondition, error) {
	enqVal, err := sql.NewStringVal(o.val).String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})
	if err != nil {
		return nil, err
	}
	enqVal = strings.Trim(enqVal, `'`)
	enqVal = strings.Replace(enqVal, "%", "\\%", -1)
	enqVal = strings.Replace(enqVal, "_", "\\_", -1)
	return sql.Eq(
		sql.NewRawObject(fmt.Sprintf("%s(samples.string, '%%%s%%')", likeOp, enqVal)), sql.NewIntVal(1),
	), nil
}

// doHasPhrase optimizes a |= substring filter into a hasPhrase() token match.
// The value is split on non-alphanumeric characters and rejoined with single
// spaces so it aligns with ClickHouse's hasPhrase tokenization; unlike LIKE
// '%val%', this matches on word boundaries (|= "bar" no longer matches "foobar").
// A value with no alphanumeric characters falls back to the original LIKE.
func (o *lineFilterOps) doHasPhrase() (sql.SQLCondition, error) {
	phrase := splitOnNonAlphanumeric(o.val)
	if phrase == "" {
		return o.doLike("like")
	}
	return sql.Eq(
		&SqlHasPhrase{col: sql.NewRawObject("samples.string"), phrase: phrase}, sql.NewIntVal(1),
	), nil
}

// splitOnNonAlphanumeric trims surrounding whitespace, splits on every
// non-alphanumeric rune, and rejoins the tokens with single spaces. Runs of
// separators collapse and empty tokens are dropped. ASCII-only, matching
// ClickHouse's default token splitter.
func splitOnNonAlphanumeric(s string) string {
	fields := strings.FieldsFunc(strings.TrimSpace(s), func(r rune) bool {
		return !isAlphanumericRune(r)
	})
	return strings.Join(fields, " ")
}

func isAlphanumericRune(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}

func (o *lineFilterOps) re2Like() (string, bool, bool) {
	exp, err := syntax.Parse(o.val, syntax.PerlX)
	if err != nil {
		return "", false, false
	}
	if exp.Op != syntax.OpLiteral || exp.Flags& ^(syntax.PerlX|syntax.FoldCase) != 0 {
		return "", false, false
	}
	return string(exp.Rune), exp.Flags&syntax.FoldCase != 0, true
}

func patternMatch(match string) string {
	match = strings.Replace(match, "%", "%%", -1)
	parts := strings.Split(match, "<_>")
	for i := len(parts) - 2; i >= 0; i-- {
		slashCnt := 0
		for j := len(parts[i]) - 1; j >= 0; j-- {
			if parts[i][j] != '\\' {
				break
			}
			slashCnt++
		}
		if slashCnt%2 == 1 {
			parts[i] = parts[i] + "<_>" + parts[i+1]
			copy(parts[i+1:], parts[i+2:])
			parts = parts[:len(parts)-1]
		}
	}
	return strings.Join(parts, "%")
}
