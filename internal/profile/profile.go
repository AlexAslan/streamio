// Package profile summarizes a stream of canonical records: how many there are, how often each
// attribute is non-empty, and which values occur most.
package profile

import (
	"slices"
	"sync"

	"github.com/AlexAslan/streamio/record"
)

// Defaults for the knobs Config leaves at zero.
const (
	DefaultTopN        = 10
	DefaultMaxDistinct = 100_000
	// MaxColumns bounds how many distinct attribute paths a profile reports, so a map column with
	// unbounded keys can't grow it without limit.
	MaxColumns = 4096
)

// listValue is the single value a non-empty list column is counted under.
const listValue = "[list]"

// heapArity is the number of children of each node in the binary heaps below.
const heapArity = 2

// Config tunes a profile run; zero fields take the defaults above.
type Config struct {
	// Columns, when non-empty, restricts the profile to these attribute paths and everything
	// nested under them.
	Columns []string
	// TopN is how many most-frequent values to keep per attribute.
	TopN int
	// MaxDistinct is how many distinct values each decode worker tracks per attribute exactly,
	// before it falls back to approximate heavy-hitter counting.
	MaxDistinct int
}

// ValueCount is one value and how often it occurred.
type ValueCount struct {
	Value string
	Count int64
	// MaxError bounds how far Count may be from the true count; zero when the column is exact.
	MaxError int64
	// Pct is Count as a percentage (0–100) of all records.
	Pct float64
}

// Column summarizes one attribute path.
type Column struct {
	Name string
	// TopValues are the most frequent non-empty values, most frequent first.
	TopValues []ValueCount
	// NonEmpty counts the records where the attribute is present and neither null nor empty.
	NonEmpty int64
	// Pct is NonEmpty as a percentage (0–100) of all records.
	Pct float64
	// Distinct is how many distinct non-empty values were counted; a lower bound when
	// DistinctExact is false.
	Distinct int64
	// DistinctExact is false once any worker exceeded Config.MaxDistinct for this attribute:
	// TopValues counts are then estimates (see ValueCount.MaxError) and Distinct a lower bound.
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
func (s *Set) NewCollector() (*Collector, error) {
	c := newCollector(s.cfg)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.collectors = append(s.collectors, c)
	return c, nil
}

// entry is one distinct value's running count.
type entry struct {
	key string
	n   int64
	// over is how much n may exceed the true count, from the entry having taken over an evicted
	// value's count.
	over int64
	// seen is scratch Merge uses to total the eviction floors of the workers that tracked this
	// value.
	seen int64
	// pos is the entry's index in column.heap while the column is approximate.
	pos int
}

// column is one attribute path's running counts inside a single worker.
//
// It counts every distinct value exactly until it holds the configured maximum; after that it
// switches to Space-Saving: the least-counted value is evicted for each newcomer, which inherits
// that count, so frequent values always survive and no count is ever under-reported.
type column struct {
	counts map[string]*entry
	name   string
	// heap is a min-heap of every entry by n, built when the column turns approximate.
	heap     []*entry
	nonEmpty int64
	// stamp is the number of the last record that touched this column, so an attribute repeated
	// within one record is counted once.
	stamp int64
}

// floor is the most a value this column no longer tracks could have been seen: the count of the
// least-counted tracked value, or zero while the column is still exact.
func (col *column) floor() int64 {
	if col.heap == nil {
		return 0
	}
	return col.heap[0].n
}

// add counts one sighting of key, tracking at most limit distinct values exactly.
func (col *column) add(key []byte, limit int) {
	// m[string(b)] lookups don't allocate; only a first-seen value copies its key.
	if e, ok := col.counts[string(key)]; ok {
		e.n++
		if col.heap != nil {
			siftDown(col.heap, e.pos)
		}
		return
	}

	if col.heap == nil {
		if len(col.counts) < limit {
			k := string(key)
			col.counts[k] = &entry{key: k, n: 1}
			return
		}
		col.heap = make([]*entry, 0, len(col.counts))
		for _, e := range col.counts {
			e.pos = len(col.heap)
			col.heap = append(col.heap, e)
		}
		for i := len(col.heap)/heapArity - 1; i >= 0; i-- {
			siftDown(col.heap, i)
		}
	}

	victim := col.heap[0]
	delete(col.counts, victim.key)
	victim.over = victim.n
	victim.n++
	victim.key = string(key)
	col.counts[victim.key] = victim
	siftDown(col.heap, 0)
}

// siftDown restores the min-heap property below index i, keeping every entry's pos current.
func siftDown(h []*entry, i int) {
	for {
		smallest := i
		if l := heapArity*i + 1; l < len(h) && h[l].n < h[smallest].n {
			smallest = l
		}
		if r := heapArity*i + heapArity; r < len(h) && h[r].n < h[smallest].n {
			smallest = r
		}
		if smallest == i {
			h[i].pos = i
			return
		}
		h[i], h[smallest] = h[smallest], h[i]
		h[i].pos = i
		i = smallest
	}
}

// cachedColumn remembers which column a top-level field position resolved to.
type cachedColumn struct {
	col  *column
	name string
	ok   bool
}

// Collector aggregates the records one decode worker sees. It is a streamio.RecordConsumer, so the
// pool drives it exactly as it would an encoder; it never produces a document.
type Collector struct {
	// index maps an attribute path to its position in cols, or to -1 for a path that is excluded.
	index map[string]int
	only  map[string]struct{}
	cols  []*column
	// cache is indexed by a field's position in the record, so flat records skip hashing the path.
	cache     []cachedColumn
	path      []byte
	scratch   []byte
	maxDist   int
	records   int64
	negatives int
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

// Consume folds batch into the collector's counts.
func (c *Collector) Consume(batch []record.Record) error {
	for _, rec := range batch {
		c.records++
		c.walk(rec, 0, false)
	}
	return nil
}

// walk visits every field of rec. c.path[:prefix] holds the dotted path of rec's parent, and
// nested says rec is a map's entries rather than a top-level record (so a dot, even after an empty
// parent name, separates path segments).
func (c *Collector) walk(rec record.Record, prefix int, nested bool) {
	for i := range rec {
		f := &rec[i]

		if !nested && f.Value.Kind != record.KindMap {
			c.record(c.topLevel(i, f.Name), &f.Value)
			continue
		}

		c.path = c.path[:prefix]
		if nested {
			c.path = append(c.path, '.')
		}
		c.path = append(c.path, f.Name...)

		switch {
		case f.Value.Kind != record.KindMap:
			c.record(c.lookup(), &f.Value)
		case len(f.Value.Map) == 0:
			// An empty map still names an attribute, which is 0% filled rather than absent.
			c.record(c.lookup(), nil)
		default:
			c.walk(f.Value.Map, len(c.path), true)
		}
	}
}

// topLevel resolves the column for the top-level field at position i named name.
func (c *Collector) topLevel(i int, name string) *column {
	if i < len(c.cache) && c.cache[i].ok && c.cache[i].name == name {
		return c.cache[i].col
	}

	c.path = append(c.path[:0], name...)
	col := c.lookup()

	for len(c.cache) <= i {
		c.cache = append(c.cache, cachedColumn{})
	}
	c.cache[i] = cachedColumn{name: name, col: col, ok: true}
	return col
}

// lookup returns the column for the path in c.path, creating it. It returns nil for a path the
// profile excludes or can no longer track.
func (c *Collector) lookup() *column {
	if i, ok := c.index[string(c.path)]; ok {
		if i < 0 {
			return nil
		}
		return c.cols[i]
	}

	name := string(c.path)
	if !c.allowed(name) {
		// Remembered so the filter isn't re-checked per record, but only up to a bound: a map with
		// unbounded keys must not grow the index without limit.
		if c.negatives < MaxColumns {
			c.index[name] = -1
			c.negatives++
		}
		return nil
	}
	if len(c.cols) >= MaxColumns {
		c.truncated = true
		return nil
	}

	col := &column{name: name, counts: make(map[string]*entry)}
	c.index[name] = len(c.cols)
	c.cols = append(c.cols, col)
	return col
}

// allowed reports whether the column filter admits name, either by name or through an ancestor.
func (c *Collector) allowed(name string) bool {
	if c.only == nil {
		return true
	}
	if _, ok := c.only[name]; ok {
		return true
	}
	for i := range len(name) {
		if name[i] != '.' {
			continue
		}
		if _, ok := c.only[name[:i]]; ok {
			return true
		}
	}
	return false
}

// record counts one occurrence of an attribute in the current record. A nil v registers the
// attribute without a value.
func (c *Collector) record(col *column, v *record.Value) {
	// The first occurrence within a record wins: counting a repeated attribute twice could push
	// non-empty past the record count.
	if col == nil || col.stamp == c.records {
		return
	}
	col.stamp = c.records
	if v == nil {
		return
	}

	key, ok := c.render(v)
	if !ok {
		return
	}
	col.nonEmpty++
	col.add(key, c.maxDist)
}

// render returns v's value as text for counting, in c.scratch or aliasing v. ok is false for an
// empty value: null, a zero-length string, or an empty list.
//
// Rendering is record.Value.AppendText, the converters' own formatting, so one value is one key
// whichever format it arrived in. A non-empty list is the one value that has no text of its own and
// counts under listValue.
func (c *Collector) render(v *record.Value) ([]byte, bool) {
	switch v.Kind {
	case record.KindBytes:
		return v.Str, len(v.Str) > 0
	case record.KindList:
		if len(v.List) == 0 {
			return nil, false
		}
		c.scratch = append(c.scratch[:0], listValue...)
		return c.scratch, true
	case record.KindNull, record.KindMap, record.KindBool, record.KindInt64, record.KindFloat64:
		var ok bool
		// A non-empty map is expanded by walk and never reaches here; AppendText reports it empty.
		c.scratch, ok = v.AppendText(c.scratch[:0])
		return c.scratch, ok
	}
	return nil, false
}

// merged is one attribute's counts combined across every worker.
type merged struct {
	entries map[string]*entry
	name    string
	// floorTotal sums the eviction floors of the workers that went approximate.
	floorTotal int64
	nonEmpty   int64
	approx     bool
}

// Merge combines every registered collector into one Profile. It consumes the collectors, so it
// must be called once, after the run has finished.
func (s *Set) Merge() Profile {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		order  []*merged
		byName = make(map[string]*merged)
		out    Profile
	)
	for _, c := range s.collectors {
		out.Records += c.records
		out.ColumnsTruncated = out.ColumnsTruncated || c.truncated

		for _, col := range c.cols {
			m, ok := byName[col.name]
			if !ok {
				// The first worker's counts are adopted as they are rather than copied.
				m = &merged{name: col.name, entries: col.counts}
				byName[col.name] = m
				order = append(order, m)
				m.absorbMeta(col)
				for _, e := range m.entries {
					e.seen = col.floor()
				}
				continue
			}
			m.absorbMeta(col)
			floor := col.floor()
			for k, e := range col.counts {
				if have, found := m.entries[k]; found {
					have.n += e.n
					have.over += e.over
					have.seen += floor
					continue
				}
				e.seen = floor
				m.entries[k] = e
			}
		}
	}

	// The cap is applied here, to the merged set, so which attributes survive doesn't depend on
	// how work happened to be split across workers.
	if len(order) > MaxColumns {
		order = order[:MaxColumns]
		out.ColumnsTruncated = true
	}

	out.Columns = make([]Column, 0, len(order))
	for _, m := range order {
		out.Columns = append(out.Columns, m.column(s.cfg.TopN, out.Records))
	}
	return out
}

// absorbMeta adds one worker column's totals to m.
func (m *merged) absorbMeta(col *column) {
	m.nonEmpty += col.nonEmpty
	if col.heap != nil {
		m.approx = true
		m.floorTotal += col.floor()
	}
}

// column renders m as a report column keeping the topN most frequent values.
func (m *merged) column(topN int, total int64) Column {
	col := Column{
		Name:          m.name,
		NonEmpty:      m.nonEmpty,
		Pct:           pct(m.nonEmpty, total),
		Distinct:      int64(len(m.entries)),
		DistinctExact: !m.approx,
	}

	for _, e := range selectTop(m.entries, topN) {
		vc := ValueCount{Value: e.key, Count: e.n, Pct: pct(e.n, total)}
		if m.approx {
			// A value is over-counted by at most e.over, and under-counted by at most what the
			// workers that no longer tracked it could have seen.
			vc.MaxError = max(e.over, m.floorTotal-e.seen)
		}
		col.TopValues = append(col.TopValues, vc)
	}
	return col
}

// better orders entries most frequent first, ties by value so the order is deterministic.
func better(a, b *entry) bool {
	if a.n != b.n {
		return a.n > b.n
	}
	return a.key < b.key
}

// selectTop returns the n best entries, best first, in O(len · log n) using a bounded heap whose
// root is the worst entry kept so far.
func selectTop(entries map[string]*entry, n int) []*entry {
	h := make([]*entry, 0, min(n, len(entries)))
	for _, e := range entries {
		switch {
		case len(h) < n:
			h = append(h, e)
			for i := len(h) - 1; i > 0; {
				parent := (i - 1) / heapArity
				if !better(h[parent], h[i]) {
					break
				}
				h[parent], h[i] = h[i], h[parent]
				i = parent
			}
		case better(e, h[0]):
			h[0] = e
			siftDownWorst(h, 0)
		}
	}
	slices.SortFunc(h, func(a, b *entry) int {
		switch {
		case better(a, b):
			return -1
		case better(b, a):
			return 1
		default:
			return 0
		}
	})
	return h
}

// siftDownWorst restores the heap whose root is the worst entry.
func siftDownWorst(h []*entry, i int) {
	for {
		worst := i
		if l := heapArity*i + 1; l < len(h) && better(h[worst], h[l]) {
			worst = l
		}
		if r := heapArity*i + heapArity; r < len(h) && better(h[worst], h[r]) {
			worst = r
		}
		if worst == i {
			return
		}
		h[i], h[worst] = h[worst], h[i]
		i = worst
	}
}

func pct(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) * 100 / float64(total)
}
