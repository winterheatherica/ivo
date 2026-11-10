package gateway

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/codes"
)

func TestShortEnum(t *testing.T) {
	if got := shortEnum("POST_STATUS_QUEUED", "POST_STATUS_"); got != "queued" {
		t.Fatalf("shortEnum = %q", got)
	}
}

func TestHTTPStatusMapping(t *testing.T) {
	cases := []struct {
		code   codes.Code
		reason string
		want   int
	}{
		{codes.InvalidArgument, "INVALID_CHOICE", 400},
		{codes.InvalidArgument, "IDEMPOTENCY_KEY_REUSED", 422},
		{codes.FailedPrecondition, "INSUFFICIENT_CREDIT", 409},
		{codes.NotFound, "NOT_FOUND", 404},
		{codes.ResourceExhausted, "RATE_LIMITED", 429},
		{codes.Unavailable, "", 503},
		{codes.Internal, "", 500},
	}
	for _, c := range cases {
		if got := httpStatus(c.code, c.reason); got != c.want {
			t.Errorf("httpStatus(%s, %q) = %d, want %d", c.code, c.reason, got, c.want)
		}
	}
}

func TestAuthRejectsMissingPlayerHeader(t *testing.T) {
	s := New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{AuthMode: "dev"})
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/posts", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	var p problem
	if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Code != "TOKEN_INVALID" || p.RequestID == "" || p.Type != "/errors/token-invalid" {
		t.Fatalf("unexpected problem: %+v", p)
	}
}
