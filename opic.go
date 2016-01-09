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

	dirty   bool
	current map[uint64]float64
	fetched map[uint64]time.Time
	history map[uint64]float64
}

// New constructs a new OPIC object.
func New() *OPIC {
	return &OPIC{
		current: make(map[uint64]float64),
		fetched: make(map[uint64]time.Time),
		history: make(map[uint64]float64),
	}
}

// Initialise sets the total cash for the system, and distributes it evenly
// amongst a collection of URLs.
func (o *OPIC) Initialise(cash float64, in []string) {
	o.m.Lock()
	defer o.m.Unlock()

	n := cash / float64(len(in))

	for _, s := range in {
		o.current[fnvHash(s)] = n
	}
}

// Distribute distributes the cash from the input to the outputs,
// and marks the input as having been
func (o *OPIC) Distribute(source string, out []string, t time.Time) float64 {
	o.m.Lock()
	defer o.m.Unlock()

	sourceH := fnvHash(source)

	c := o.current[sourceH]

	o.current[0] = o.current[0] + c/float64(len(out)+1)

	for _, s := range out {
		outH := fnvHash(s)
		o.current[outH] = o.current[outH] + c/float64(len(out)+1)
	}

	d := o.current[0] / float64(len(o.current)+1)
	o.current[0] = o.current[0] - d

	o.current[sourceH] = d
	o.fetched[sourceH] = t
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
}

// GetN gets the details for an entry, referenced by numeric hash.
func (o *OPIC) GetN(v uint64) (float64, float64, time.Time) {
	o.m.RLock()
	defer o.m.RUnlock()

	return o.history[v], o.current[v], o.fetched[v]
}

// EstimateN estimates the total for an entry, referenced by numeric hash.
func (o *OPIC) EstimateN(v uint64) float64 {
	v1, v2, _ := o.GetN(v)
	return v1 + v2
}

// EstimateNV estimates the total for a list of entries, referenced by numeric
// hash.
func (o *OPIC) EstimateNV(v []uint64) []float64 {
	r := make([]float64, len(v))

	for i, n := range v {
		r[i] = o.EstimateN(n)
	}

	return r
}

// Get gets the details for an entry.
func (o *OPIC) Get(s string) (float64, float64, time.Time) {
	return o.GetN(fnvHash(s))
}

// Estimate estimates the total for an entry.
func (o *OPIC) Estimate(s string) float64 {
	v1, v2, _ := o.Get(s)
	return v1 + v2
}

// EstimateV estimates the totals for a list of entries.
func (o *OPIC) EstimateV(v []string) []float64 {
	r := make([]float64, len(v))

	for i, n := range v {
		r[i] = o.Estimate(n)
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
