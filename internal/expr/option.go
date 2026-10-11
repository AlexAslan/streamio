package expr

import (
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/record"
)

// filter adapts an Evaluator to the pool's RowFilter seam.
type filter struct {
	ev *Evaluator
}

// Keep reports whether rec satisfies the predicate.
func (f filter) Keep(rec record.Record) (bool, error) { return f.ev.Match(rec) }

// Misses reports the evaluator's type-mismatch count.
func (f filter) Misses() int64 { return f.ev.Misses() }

// Option returns the pipeline option that keeps only records satisfying src, or the parse error.
func Option(src string, strict bool) (options.Option, error) {
	p, err := Compile(src, strict)
	if err != nil {
		return nil, err
	}
	return options.WithRowFilter(func() (options.RowFilter, error) {
		return filter{ev: p.NewEvaluator()}, nil
	}), nil
}
