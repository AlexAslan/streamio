// Package profile summarizes a stream of canonical records: how many there are, how often each
// attribute is non-empty, and which values occur most.
package profile

import (
	"cmp"
	"slices"
	"strconv"
	"sync"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/record"
)

// Defaults for the knobs Config leaves at zero.
const (
	DefaultTopN        = 10
	DefaultMaxDistinct = 100_000
	// MaxColumns bounds how many distinct attribute paths are tracked, so a map column with
	// unbounded keys can't grow the profile without limit.
	MaxColumns = 4096
)

// Number-formatting parameters for strconv.
const (
	decimal = 10
	bits32  = 32
	bits64  = 64
)

// listValue is the single value a non-empty list column is counted under.
const listValue = "[list]"

// Config tunes a profile run; zero fields take the defaults above.
type Config struct {
	// Columns, when non-empty, restricts the profile to these attribute paths.
	Columns []string
	// TopN is how many most-frequent values to keep per attribute.
	TopN int
	// MaxDistinct caps how many distinct values are counted per attribute.
	MaxDistinct int
}

// ValueCount is one value and how often it occurred.
type ValueCount struct {
	Value string
	Count int64
	// Pct is Count as a percentage (0–100) of all records.
	Pct float64
}

// Column summarizes one attribute path.
type Column struct {
	Name          string
	TopValues     []ValueCount
	NonEmpty      int64
	Pct           float64
	Distinct      int64
	DistinctExact bool
}

// Profile is a finished summary.
type Profile struct {
	Columns []Column
	Records int64
	// ColumnsTruncated is true when more than MaxColumns attribute paths were seen and the rest
	// were left out.
	ColumnsTruncated bool
}

// Set hands out one Collector per decode worker and merges them into a Profile once the run ends.
type Set struct {
	collectors []*Collector
	cfg        Config
	mu         sync.Mutex
}

// NewSet returns an empty Set using cfg, with zero fields defaulted.
func NewSet(cfg Config) *Set {
	if cfg.TopN <= 0 {
		cfg.TopN = DefaultTopN
	}
	if cfg.MaxDistinct <= 0 {
		cfg.MaxDistinct = DefaultMaxDistinct
	}
	return &Set{cfg: cfg}
}

// NewCollector builds a worker-local Collector and registers it for the final merge.
//
//nolint:ireturn // pool.RunRecords wants a formatio.RecordEncoder factory.
func (s *Set) NewCollector() (formatio.RecordEncoder, error) {
	c := newCollector(s.cfg)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.collectors = append(s.collectors, c)
	return c, nil
}

// column is one attribute path's running counts inside a single worker.
type column struct {
	// counts holds a pointer per value so a repeat sighting increments in place: a map assignment
	// with a converted key would allocate the key string every time.
	counts   map[string]*int64
	name     string
	nonEmpty int64
	overflow bool
}

// Collector aggregates the records one decode worker sees. It implements formatio.RecordEncoder so
// the pool drives it exactly as it would an encoder; it never produces a document.
type Collector struct {
	index     map[string]int
	only      map[string]struct{}
	cols      []*column
	path      []byte
	scratch   []byte
	maxDist   int
	records   int64
	truncated bool
}

func newCollector(cfg Config) *Collector {
	c := &Collector{index: make(map[string]int), maxDist: cfg.MaxDistinct}
	if len(cfg.Columns) > 0 {
		c.only = make(map[string]struct{}, len(cfg.Columns))
		for _, name := range cfg.Columns {
			c.only[name] = struct{}{}
		}
	}
	return c
}

// EncodeBatch folds batch into the collector's counts and returns no documents.
func (c *Collector) EncodeBatch(batch []record.Record) ([][]byte, error) {
	for _, rec := range batch {
		c.records++
		c.walk(rec, 0)
	}
	return nil, nil
}

// walk visits every field of rec, with c.path[:prefix] holding the dotted path of rec's parent.
func (c *Collector) walk(rec record.Record, prefix int) {
	for i := range rec {
		f := &rec[i]

		c.path = c.path[:prefix]
		if prefix > 0 {
			c.path = append(c.path, '.')
		}
		c.path = append(c.path, f.Name...)

		if f.Value.Kind == record.KindMap {
			c.walk(f.Value.Map, len(c.path))
			continue
		}
		c.observe(&f.Value)
	}
}

// observe counts one scalar or list value under the path currently in c.path.
func (c *Collector) observe(v *record.Value) {
	if c.only != nil {
		if _, ok := c.only[string(c.path)]; !ok {
			return
		}
	}

	// Non-bytes kinds are rendered into the reused scratch buffer; bytes are looked up in place.
	var key []byte
	switch v.Kind {
	case record.KindNull, record.KindMap:
		// A map is expanded by walk; reaching here with one means it was empty.
		return
	case record.KindList:
		if len(v.List) == 0 {
			return
		}
		c.scratch = append(c.scratch[:0], listValue...)
		key = c.scratch
	case record.KindBytes:
		if len(v.Str) == 0 {
			return
		}
		key = v.Str
	case record.KindBool:
		c.scratch = strconv.AppendBool(c.scratch[:0], v.Bool)
		key = c.scratch
	case record.KindInt64:
		if v.Semantic == record.SemanticUnsigned {
			//nolint:gosec // SemanticUnsigned says the int64's bits are a uint64.
			c.scratch = strconv.AppendUint(c.scratch[:0], uint64(v.I64), decimal)
		} else {
			c.scratch = strconv.AppendInt(c.scratch[:0], v.I64, decimal)
		}
		key = c.scratch
	case record.KindFloat64:
		bits := bits64
		if v.Semantic == record.SemanticFloat32 {
			bits = bits32
		}
		c.scratch = strconv.AppendFloat(c.scratch[:0], v.F64, 'g', -1, bits)
		key = c.scratch
	}

	col := c.column()
	if col == nil {
		return
	}
	col.nonEmpty++

	// m[string(b)] lookups don't allocate; only a first-seen value pays for its key.
	if n, ok := col.counts[string(key)]; ok {
		*n++
		return
	}
	if len(col.counts) >= c.maxDist {
		col.overflow = true
		return
	}
	n := int64(1)
	col.counts[string(key)] = &n
}

// column returns the column for the path in c.path, creating it, or nil once MaxColumns is hit.
func (c *Collector) column() *column {
	if i, ok := c.index[string(c.path)]; ok {
		return c.cols[i]
	}
	if len(c.cols) >= MaxColumns {
		c.truncated = true
		return nil
	}
	name := string(c.path)
	col := &column{name: name, counts: make(map[string]*int64)}
	c.index[name] = len(c.cols)
	c.cols = append(c.cols, col)
	return col
}

// Merge combines every registered collector into one Profile.
func (s *Set) Merge() Profile {
	s.mu.Lock()
	defer s.mu.Unlock()

	type merged struct {
		counts   map[string]int64
		name     string
		nonEmpty int64
		overflow bool
	}
	var (
		order     []*merged
		byName    = make(map[string]*merged)
		out       Profile
		truncated bool
	)
	for _, c := range s.collectors {
		out.Records += c.records
		truncated = truncated || c.truncated
		for _, col := range c.cols {
			m, ok := byName[col.name]
			if !ok {
				m = &merged{name: col.name, counts: make(map[string]int64, len(col.counts))}
				byName[col.name] = m
				order = append(order, m)
			}
			m.nonEmpty += col.nonEmpty
			m.overflow = m.overflow || col.overflow
			for k, n := range col.counts {
				m.counts[k] += *n
			}
		}
	}
	out.ColumnsTruncated = truncated

	out.Columns = make([]Column, 0, len(order))
	for _, m := range order {
		col := Column{
			Name:          m.name,
			NonEmpty:      m.nonEmpty,
			Pct:           pct(m.nonEmpty, out.Records),
			Distinct:      int64(len(m.counts)),
			DistinctExact: !m.overflow,
			TopValues:     top(m.counts, s.cfg.TopN, out.Records),
		}
		out.Columns = append(out.Columns, col)
	}
	return out
}

// top returns the n most frequent entries of counts, ties broken by value for determinism.
func top(counts map[string]int64, n int, total int64) []ValueCount {
	all := make([]ValueCount, 0, len(counts))
	for v, c := range counts {
		all = append(all, ValueCount{Value: v, Count: c})
	}
	slices.SortFunc(all, func(a, b ValueCount) int {
		if d := cmp.Compare(b.Count, a.Count); d != 0 {
			return d
		}
		return cmp.Compare(a.Value, b.Value)
	})
	if len(all) > n {
		all = all[:n]
	}
	for i := range all {
		all[i].Pct = pct(all[i].Count, total)
	}
	return all
}

func pct(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) * 100 / float64(total)
}
