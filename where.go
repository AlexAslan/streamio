package streamio

import (
	"fmt"

	"github.com/AlexAslan/streamio/internal/expr"
	"github.com/AlexAslan/streamio/internal/options"
)

// ErrInvalidWhere is wrapped by the option error WithWhere reports for an expression that does not
// parse.
var ErrInvalidWhere = expr.ErrSyntax

// ErrUncomparable is wrapped by the error WithWhereStrict fails a run with when a value has the
// wrong type for its literal.
var ErrUncomparable = expr.ErrUncomparable

// WithWhere keeps only the records satisfying a predicate such as
// `age >= 18 and country in ('US', 'GR') and not email is empty`.
//
// The grammar, how a literal's type decides a comparison, and how empty and mistyped values are
// handled are in docs/where.md. A value of the wrong type for its literal does not match and is
// counted in Stats.PredicateMisses; WithWhereStrict fails the run instead.
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
