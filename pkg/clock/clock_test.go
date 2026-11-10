package clock

import (
	"testing"
	"time"
)

func TestDateWIBCrossesMidnight(t *testing.T) {
	// 17:30 UTC is already the next day in WIB (00:30).
	got := DateWIB(time.Date(2026, 11, 9, 17, 30, 0, 0, time.UTC))
	want := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("DateWIB = %v, want %v", got, want)
	}
}

func TestBucketStartAlignsToQuarterHour(t *testing.T) {
	got := BucketStart(time.Date(2026, 11, 9, 12, 44, 59, 999, time.UTC))
	want := time.Date(2026, 11, 9, 12, 30, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("BucketStart = %v, want %v", got, want)
	}
}

func TestFakeAdvance(t *testing.T) {
	start := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	f := NewFake(start)
	f.Advance(90 * time.Minute)
	if got := f.Now(); !got.Equal(start.Add(90 * time.Minute)) {
		t.Fatalf("Now after Advance = %v", got)
	}
}
