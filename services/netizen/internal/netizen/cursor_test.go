package netizen

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorRoundTrip(t *testing.T) {
	at := time.Date(2026, 11, 9, 12, 34, 56, 789_000_000, time.UTC)
	id := uuid.New()
	gotT, gotID, err := decodeCursor(encodeCursor(at, id))
	if err != nil {
		t.Fatal(err)
	}
	if !gotT.Equal(at) || gotID != id {
		t.Fatalf("cursor differs: %v %s", gotT, gotID)
	}
	if _, _, err := decodeCursor("not-a-cursor"); err == nil {
		t.Fatal("a corrupt cursor must be rejected")
	}
}

func TestBinRoundTrip(t *testing.T) {
	id := uuid.New()
	if got := unbin(bin(id)); got != id {
		t.Fatalf("bin/unbin = %s, want %s", got, id)
	}
}
