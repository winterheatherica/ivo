// Package detrand provides deterministic randomness that doesn't depend on the
// Go version and is easy to rewrite in Python:
//
//	u(material, k) = uint64_big_endian(SHA-256(material + ":" + k)[0:8]) >> 11, divided by 2^53
//
// See docs/detail/README.md#deterministic-randomness.
package detrand

import (
	"crypto/sha256"
	"encoding/binary"
	"strconv"
)

// U returns a number in [0, 1) that depends only on material and k.
func U(material string, k int) float64 {
	sum := sha256.Sum256([]byte(material + ":" + strconv.Itoa(k)))
	return float64(binary.BigEndian.Uint64(sum[:8])>>11) / (1 << 53)
}

// Intn returns an integer in [0, n). n must be greater than 0.
func Intn(material string, k, n int) int {
	i := int(U(material, k) * float64(n))
	if i >= n {
		i = n - 1
	}
	return i
}

// Seed63 returns the first 63 bits of SHA-256(material); used for image seeds.
func Seed63(material string) int64 {
	sum := sha256.Sum256([]byte(material))
	return int64(binary.BigEndian.Uint64(sum[:8]) & 0x7FFF_FFFF_FFFF_FFFF)
}
