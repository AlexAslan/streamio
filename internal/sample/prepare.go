package sample

import (
	"context"
	"math"
	"sync"

	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/record"
)

// preparer runs the first pass that exact-size and min-per-stratum sampling need.
type preparer struct {
	c *compiled
}

// Prepare resolves the compiled spec's thresholds from the data.
func (p *preparer) Prepare(_ context.Context, pass options.PassFunc) error {
	var err error
	switch p.c.mode {
	case modeCount:
		err = p.prepareCount(pass)
	case modeStrata:
		err = p.prepareStrata(pass)
	case modeRate, modeRange:
		// Decided from the spec alone; there is nothing to learn.
	}
	if err != nil {
		return err
	}
	p.c.prepared = true
	return nil
}

// boundAbove returns the threshold that keeps every hash <= t.
func boundAbove(t uint64) threshold {
	if t == math.MaxUint64 {
		return threshold{all: true}
	}
	return threshold{bound: t + 1}
}

// fraction is the share of the hash space a threshold keeps.
func (t threshold) fraction() float64 {
	if t.all {
		return 1
	}
	return math.Ldexp(float64(t.bound), -hashBits)
}

// maxThreshold returns the more permissive of two thresholds.
func maxThreshold(a, b threshold) threshold {
	switch {
	case a.all || b.all:
		return threshold{all: true}
	case a.bound >= b.bound:
		return a
	default:
		return b
	}
}

// countConsumer feeds one worker's records' hashes into a bottom-k.
type countConsumer struct {
	f  *filter
	bk *bottomK
}

func (c *countConsumer) Consume(batch []record.Record) error {
	for _, rec := range batch {
		if h, ok := c.f.candidate(rec); ok {
			c.bk.add(h)
		}
	}
	return nil
}

func (p *preparer) prepareCount(pass options.PassFunc) error {
	var (
		mu        sync.Mutex
		consumers []*countConsumer
	)
	err := pass(func() (options.RecordConsumer, error) {
		cc := &countConsumer{f: &filter{c: p.c}, bk: newBottomK(p.c.count)}
		mu.Lock()
		consumers = append(consumers, cc)
		mu.Unlock()
		return cc, nil
	})
	if err != nil {
		return err
	}

	merged := newBottomK(p.c.count)
	for _, cc := range consumers {
		merged.merge(cc.bk)
	}

	t, ok := merged.kth()
	if !ok {
		// Fewer distinct keys than asked for: every one of them is in the sample.
		p.c.countT = threshold{all: true}
		p.c.weight = 1
		return nil
	}
	p.c.countT = boundAbove(t)
	// Roughly count of D keys are kept, which is the fraction of the hash space below t.
	p.c.weight = weightOf(p.c.countT.fraction())
	return nil
}

// strataConsumer feeds one worker's records into a bottom-k per stratum.
type strataConsumer struct {
	f       *filter
	per     map[string]*bottomK
	scratch []byte
	minPer  int
	max     int
}

func (c *strataConsumer) Consume(batch []record.Record) error {
	for _, rec := range batch {
		h, ok := c.f.candidate(rec)
		if !ok {
			continue
		}
		stratum := c.f.c.stratumText(rec, &c.scratch)
		bk, ok := c.per[string(stratum)]
		if !ok {
			if len(c.per) >= c.max {
				continue // beyond the tracked strata: it falls back to the default rate
			}
			bk = newBottomK(c.minPer)
			c.per[string(stratum)] = bk
		}
		bk.add(h)
	}
	return nil
}

func (p *preparer) prepareStrata(pass options.PassFunc) error {
	var (
		mu        sync.Mutex
		consumers []*strataConsumer
	)
	err := pass(func() (options.RecordConsumer, error) {
		sc := &strataConsumer{
			f:      &filter{c: p.c},
			per:    make(map[string]*bottomK),
			minPer: p.c.minPer,
			max:    p.c.maxStrata,
		}
		mu.Lock()
		consumers = append(consumers, sc)
		mu.Unlock()
		return sc, nil
	})
	if err != nil {
		return err
	}

	merged := make(map[string]*bottomK)
	for _, sc := range consumers {
		for stratum, bk := range sc.per {
			have, ok := merged[stratum]
			if !ok {
				merged[stratum] = bk
				continue
			}
			have.merge(bk)
		}
	}

	// Listed rates are the floor; a stratum with fewer than minPer distinct keys keeps them all.
	for stratum, bk := range merged {
		base := p.c.defaultT
		if t, listed := p.c.strata[stratum]; listed {
			base = t
		}

		t, full := bk.kth()
		if !full {
			base = threshold{all: true}
		} else {
			base = maxThreshold(base, boundAbove(t))
		}
		p.c.strata[stratum] = base
		p.c.weights[stratum] = weightOf(base.fraction())
	}
	return nil
}

// stratumText returns the text of rec's stratum value, empty when it has none.
func (c *compiled) stratumText(rec record.Record, scratch *[]byte) []byte {
	v, ok := lookupPath(rec, c.stratifyPath)
	if !ok {
		return nil
	}
	text, ok := v.AppendText((*scratch)[:0])
	*scratch = text
	if !ok {
		return nil
	}
	return text
}

// candidate returns the hash a first pass should count for rec, and false when rec is not a
// candidate for the sample: a record with no key that the missing-key policy keeps, drops or fails
// on is decided outside the rate, so it must not use up a slot in an exact-size sample.
func (f *filter) candidate(rec record.Record) (uint64, bool) {
	h, missing := f.hashOf(rec)
	if !missing {
		return h, true
	}
	if f.c.onMissing == MissingKeyRow {
		return f.rowHash(rec), true
	}
	return 0, false
}
