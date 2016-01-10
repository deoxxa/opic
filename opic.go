package opic

import (
	"hash/fnv"
	"sync"
	"time"
)

func fnvHash(s string) uint64 {
	h := fnv.New64()
	h.Write([]byte(s))
	return h.Sum64()
}

// OPIC holds all the state for running the Adaptive OPIC algorithm.
type OPIC struct {
	m sync.RWMutex

	dirty bool

	current map[uint64]float64
	cleared map[uint64]time.Time
	history map[uint64]float64
}

// New constructs a new OPIC object.
func New() *OPIC {
	return &OPIC{
		current: make(map[uint64]float64),
		cleared: make(map[uint64]time.Time),
		history: make(map[uint64]float64),
	}
}

// InitialiseN sets the total cash for the system, and distributes it evenly
// amongst a collection of URLs referenced by numeric hash.
func (o *OPIC) InitialiseN(cash float64, in []uint64) {
	o.m.Lock()
	defer o.m.Unlock()

	n := cash / float64(len(in))

	for _, u := range in {
		o.current[u] = n
	}

	o.dirty = true
}

// Initialise sets the total cash for the system, and distributes it evenly
// amongst a collection of URLs.
func (o *OPIC) Initialise(cash float64, in []string) {
	ids := make([]uint64, len(in))
	for i, s := range in {
		ids[i] = fnvHash(s)
	}

	o.InitialiseN(cash, ids)
}

// Distribute distributes the cash from the input to the outputs, and marks
// the input as having been fetched.
func (o *OPIC) Distribute(source string, out []string, t time.Time) float64 {
	o.m.Lock()
	defer o.m.Unlock()

	sourceH := fnvHash(source)

	c := o.current[sourceH]

	o.current[0] = o.current[0] + c/float64(len(out)+1)

	for _, s := range out {
		outH := fnvHash(s)
		o.current[outH] = o.current[outH] + c/float64(len(out)+1)
		if _, ok := o.cleared[outH]; !ok {
			o.cleared[outH] = time.Now()
		}
	}

	d := o.current[0] / float64(len(o.current)+1)
	o.current[0] = o.current[0] - d

	o.current[sourceH] = d
	o.cleared[sourceH] = t
	o.history[sourceH] = c

	o.dirty = true

	return c
}

// Finalise moves all the current values into the history for the inputs.
func (o *OPIC) Finalise(in []string) {
	o.m.Lock()
	defer o.m.Unlock()

	for _, s := range in {
		inH := fnvHash(s)
		o.history[inH] = o.current[inH]
		o.current[inH] = 0
	}

	o.dirty = true
}

// GetN gets the details for an entry, referenced by numeric hash.
func (o *OPIC) GetN(v uint64) (float64, float64, time.Time) {
	o.m.RLock()
	defer o.m.RUnlock()

	return o.history[v], o.current[v], o.cleared[v]
}

// EstimateN estimates the total for an entry, referenced by numeric hash.
func (o *OPIC) EstimateN(v uint64, interval time.Duration, t time.Time) float64 {
	h, c, vt := o.GetN(v)
	d := t.Sub(vt)

	var r float64
	if d < interval {
		r = h*(float64(interval)-float64(d))/float64(interval) + c
	} else {
		r = c * (float64(interval) / float64(d))
	}

	return r
}

// EstimateNV estimates the total for a list of entries, referenced by numeric
// hash.
func (o *OPIC) EstimateNV(v []uint64, interval time.Duration, t time.Time) []float64 {
	r := make([]float64, len(v))

	for i, n := range v {
		r[i] = o.EstimateN(n, interval, t)
	}

	return r
}

// Get gets the details for an entry.
func (o *OPIC) Get(s string) (float64, float64, time.Time) {
	return o.GetN(fnvHash(s))
}

// Estimate estimates the total for an entry.
func (o *OPIC) Estimate(s string, interval time.Duration, t time.Time) float64 {
	return o.EstimateN(fnvHash(s), interval, t)
}

// EstimateV estimates the totals for a list of entries.
func (o *OPIC) EstimateV(v []string, interval time.Duration, t time.Time) []float64 {
	r := make([]float64, len(v))

	for i, n := range v {
		r[i] = o.Estimate(n, interval, t)
	}

	return r
}

// Dirty returns true if there have been any changes since the last time the
// OPIC instance was loaded or saved. It's intended that this be used by the
// persistency layer to decide what to do.
func (o *OPIC) Dirty() bool {
	return o.dirty
}

// EnsureBalance "tops up" the cash in the system, allowing the user to
// correct for slight inaccuracies in floating point math.
func (o *OPIC) EnsureBalance(n float64) {
	r1, r2 := o.Sums()
	if (r1 + r2) < n {
		o.current[0] = o.current[0] + (n - (r1 + r2))
	}

	o.dirty = true
}

// Virtual gets the details for the "virtual" entry.
func (o *OPIC) Virtual() (float64, float64) {
	o.m.RLock()
	defer o.m.RUnlock()

	return o.history[0], o.current[0]
}

// Sums returns the total cash in the system. Ideally, these values would be
// the same. Unfortunately floating point math is a tiny bit inaccurate, so
// these diverge over time. See also EnsureBalance.
func (o *OPIC) Sums() (float64, float64) {
	o.m.RLock()
	defer o.m.RUnlock()

	var r1, r2 float64
	for _, v := range o.history {
		r1 += v
	}
	for _, v := range o.current {
		r2 += v
	}
	return r1, r2
}
