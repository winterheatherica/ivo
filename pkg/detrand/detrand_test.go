package detrand

import "testing"

// The reference values were computed separately in Python, so the Go
// implementation and the Python evals are guaranteed to use the same formula.
func TestUMatchesPythonReference(t *testing.T) {
	cases := []struct {
		material string
		k        int
		want     float64
	}{
		{"a", 0, 0.05790979865099921},
		{"post:netizen:1", 0, 0.3141684919028461},
		{"post:netizen:1", 1, 0.9415794040266439},
	}
	for _, c := range cases {
		if got := U(c.material, c.k); got != c.want {
			t.Errorf("U(%q, %d) = %v, want %v", c.material, c.k, got, c.want)
		}
	}
}

func TestSeed63MatchesPythonReference(t *testing.T) {
	if got, want := Seed63("job:0"), int64(8856518206409549440); got != want {
		t.Fatalf("Seed63 = %d, want %d", got, want)
	}
}

func TestIntnStaysInRange(t *testing.T) {
	for k := 0; k < 1000; k++ {
		if v := Intn("range", k, 7); v < 0 || v >= 7 {
			t.Fatalf("Intn out of range: %d", v)
		}
	}
}
