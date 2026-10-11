package streamio

import (
	"fmt"

	"github.com/AlexAslan/streamio/internal/expr"
	"github.com/AlexAslan/streamio/internal/options"
)

// ErrInvalidWhere is wrapped by the option error WithWhere reports for an expression that does not
// parse.
var ErrInvalidWhere = expr.ErrSyntax

// WithWhere keeps only records satisfying a predicate such as
//
//	age >= 18 and country in ('US', 'GR') and not email is empty
//
// Comparisons are =, !=, <, <=, >, >=; `in (...)`, `is [not] empty`, `contains` and `starts_with`
// work on text; `and`, `or`, `not` and parentheses combine them. A literal's type decides the
// comparison: a quoted string compares the value's text, a number compares numerically (text is
// parsed, so a CSV column works), true and false compare booleans. Dotted names reach into nested
// map entries, and `a.b` quoted in backticks names a column that contains a dot.
//
// A missing, null or empty value matches no comparison, so `not (x = 1)` holds for records with no
// x. A value of the wrong type for its literal does not match and is counted in
// Stats.PredicateMisses; WithWhereStrict fails the run instead.
//
// A filter makes a same-format conversion decode and re-encode instead of streaming raw bytes.
// Filters apply in the order the options are given, so any later filter sees only the records
// that pass.
func WithWhere(expression string) Option {
	return whereOption(expression, false)
}

// WithWhereStrict is WithWhere, except that a value of the wrong type for its literal fails the run
// with an error naming the column, instead of simply not matching.
func WithWhereStrict(expression string) Option {
	return whereOption(expression, true)
}

func whereOption(expression string, strict bool) Option {
	opt, err := expr.Option(expression, strict)
	if err != nil {
		return func(c *options.Config) {
			if c.OptionErr == nil {
				c.OptionErr = fmt.Errorf("streamio: invalid where expression %q: %w", expression, err)
			}
		}
	}
	return opt
}
