// Package schemav1 holds the wire records (generated from records.proto) and
// the hybrid logical clock that timestamps them.
package schemav1

import (
	"sync"
	"time"
)

// Clock is a hybrid logical clock packed into a uint64 as
// unix milliseconds << 16 | logical counter, so values compare as integers.
// A logical overflow carries into the physical part, which keeps it monotonic.
type Clock struct {
	mu   sync.Mutex
	last uint64
	wall func() time.Time
}

// NewClock starts a clock that never returns a value <= last, even if the
// wall clock is behind it (edge nodes without an RTC boot with a stale time).
func NewClock(last uint64, wall func() time.Time) *Clock {
	return &Clock{last: last, wall: wall}
}

// Now returns the timestamp for a local event.
func (c *Clock) Now() uint64 { return c.Update(0) }

// Update merges a timestamp received from another node and returns the
// timestamp for the receive event.
// ponytail: no max-drift check on remote, add one before the clock-skew chaos fault.
func (c *Clock) Update(remote uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	pt := uint64(max(c.wall().UnixMilli(), 0)) << 16 // pre-1970 would wrap and poison the clock
	if m := max(c.last, remote); pt > m {
		c.last = pt
	} else {
		c.last = m + 1
	}
	return c.last
}

// HLCTime returns the physical part of an HLC timestamp.
func HLCTime(hlc uint64) time.Time { return time.UnixMilli(int64(hlc >> 16)) }
