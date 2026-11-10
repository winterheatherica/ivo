// Package clock is the only source of time for IVO's domain logic.
// Domain code takes a Clock, so tests can move time however they like
// (docs/detail/README.md#time).
package clock

import (
	"sync"
	"time"
)

// WIB is the Asia/Jakarta time zone. Indonesia doesn't use daylight saving,
// so a fixed +7 offset is correct and doesn't depend on tzdata.
var WIB = time.FixedZone("WIB", 7*60*60)

// BucketSize is the size of an engagement bucket.
const BucketSize = 15 * time.Minute

// Clock returns the current time in UTC.
type Clock interface {
	Now() time.Time
}

// Real uses the system clock.
type Real struct{}

// Now returns the system time in UTC.
func (Real) Now() time.Time { return time.Now().UTC() }

// Fake is used in tests; its time only changes through Set or Advance.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake creates a Fake stopped at t.
func NewFake(t time.Time) *Fake { return &Fake{now: t.UTC()} }

// Now returns the Fake's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Set moves the Fake's time to t.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	f.now = t.UTC()
	f.mu.Unlock()
}

// Advance moves the Fake's time forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

// DateWIB returns the WIB calendar date of t as midnight UTC on that date,
// ready to be stored in a date column.
func DateWIB(t time.Time) time.Time {
	y, m, d := t.In(WIB).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// BucketStart rounds t down to the start of its 15-minute bucket (minutes 00, 15, 30, 45 UTC).
func BucketStart(t time.Time) time.Time { return t.UTC().Truncate(BucketSize) }
