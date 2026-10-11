package sample

import "slices"

// bottomK keeps the k smallest distinct hash values it has seen.
//
// Values are appended to a buffer and compacted (sorted, deduplicated, truncated to k) whenever it
// reaches twice k, so adding is amortized O(log k) and memory is about 16 bytes per kept value.
// Once full, a value at or above the current k-th smallest is ignored without touching the buffer.
type bottomK struct {
	buf  []uint64
	k    int
	max  uint64
	full bool
}

func newBottomK(k int) *bottomK {
	return &bottomK{k: k}
}

// add offers h.
func (b *bottomK) add(h uint64) {
	if b.full && h >= b.max {
		return
	}
	b.buf = append(b.buf, h)
	if len(b.buf) >= 2*b.k {
		b.compact()
	}
}

// compact sorts, deduplicates and truncates the buffer to the k smallest values.
func (b *bottomK) compact() {
	slices.Sort(b.buf)
	b.buf = slices.Compact(b.buf)
	if len(b.buf) >= b.k {
		b.buf = b.buf[:b.k]
		b.full = true
		b.max = b.buf[b.k-1]
	}
}

// merge folds other's values into b.
func (b *bottomK) merge(other *bottomK) {
	for _, h := range other.buf {
		b.add(h)
	}
}

// kth returns the k-th smallest distinct value, and false when fewer than k distinct values exist.
func (b *bottomK) kth() (uint64, bool) {
	b.compact()
	if !b.full {
		return 0, false
	}
	return b.max, true
}
