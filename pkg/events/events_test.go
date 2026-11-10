package events

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestHeadersRoundTrip(t *testing.T) {
	in := Meta{
		ID:     uuid.MustParse("0193a1f2-7c4e-7b2a-9d1e-5f6a7b8c9d0e"),
		Type:   Type(TopicPostSubmitted),
		Source: "ivo/game",
		Time:   time.Date(2026, 11, 9, 11, 0, 0, 123, time.UTC),
	}
	h := in.Headers()
	if h[HeaderType] != "ivo.post.submitted.v1" || h[HeaderSpecVersion] != "1.0" {
		t.Fatalf("unexpected headers: %v", h)
	}
	out, err := ParseMeta(h)
	if err != nil {
		t.Fatal(err)
	}
	if out.ID != in.ID || !out.Time.Equal(in.Time) || out.Source != in.Source {
		t.Fatalf("round trip differs: %+v vs %+v", out, in)
	}
}

func TestParseMetaRejectsMissingID(t *testing.T) {
	if _, err := ParseMeta(map[string]string{}); err == nil {
		t.Fatal("an empty ce_id must be rejected")
	}
}
