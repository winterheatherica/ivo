package ids

import (
	"testing"

	"github.com/google/uuid"
)

func TestPostIDMatchesSpec(t *testing.T) {
	player := uuid.MustParse("11111111-1111-7111-8111-111111111111")
	key := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	// Computed separately in Python: uuid.uuid5(NS_POST, "player:key").
	want := uuid.MustParse("32399c2b-6208-5350-bc9b-9bb7c5586c3a")
	if got := PostID(player, key); got != want {
		t.Fatalf("PostID = %s, want %s", got, want)
	}
}

func TestDerivedIDsAreVersion5AndStable(t *testing.T) {
	post := New()
	a, b := JobID(post, 0), JobID(post, 0)
	if a != b {
		t.Fatal("JobID must be deterministic")
	}
	if a.Version() != 5 {
		t.Fatalf("JobID version = %d, want 5", a.Version())
	}
	if JobID(post, 1) == a {
		t.Fatal("a different variant must produce a different job_id")
	}
}

func TestNewIsVersion7(t *testing.T) {
	if v := New().Version(); v != 7 {
		t.Fatalf("New version = %d, want 7", v)
	}
}
