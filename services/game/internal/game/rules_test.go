package game

import (
	"testing"
	"time"
)

func TestNoveltyBP(t *testing.T) {
	at := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	day := func(d int) *time.Time { v := at.Add(-time.Duration(d) * 24 * time.Hour); return &v }
	cases := []struct {
		name string
		last *time.Time
		want int32
	}{
		{"never visited", nil, 10000},
		{"yesterday hits the floor", day(1), 2000},
		{"three and a half days", func() *time.Time { v := at.Add(-84 * time.Hour); return &v }(), 5000},
		{"a week ago is already full", day(7), 10000},
		{"a month ago stays full", day(30), 10000},
	}
	for _, c := range cases {
		if got := NoveltyBP(c.last, at); got != c.want {
			t.Errorf("%s: NoveltyBP = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestFrequencyPenaltyBP(t *testing.T) {
	for n, want := range map[int]int32{0: 10000, 1: 9000, 3: 7000, 5: 5000, 9: 5000} {
		if got := FrequencyPenaltyBP(n); got != want {
			t.Errorf("FrequencyPenaltyBP(%d) = %d, want %d", n, got, want)
		}
	}
}

func TestConvertIsIntegerAndMonotonic(t *testing.T) {
	f, s := Convert(40, 3)
	if f != 16 || s != 55 { // (40×3000 + 3×15000) / 10000 = 16.5, rounded down
		t.Fatalf("Convert(40, 3) = (%d, %d), want (16, 55)", f, s)
	}
	prevF, prevS := Convert(0, 0)
	for likes := int32(1); likes <= 200; likes++ {
		f, s := Convert(likes, 0)
		if f < prevF || s <= prevS {
			t.Fatalf("Convert isn't monotonic at likes=%d", likes)
		}
		prevF, prevS = f, s
	}
}
