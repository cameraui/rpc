package rpc

import (
	"fmt"
	"math/rand"
	"time"
)

const base36Chars = "0123456789abcdefghijklmnopqrstuvwxyz"

// GenerateID produces a request ID in the format "{timestamp}-{random9}",
func GenerateID() string {
	ts := time.Now().UnixMilli()
	r := make([]byte, 9)
	for i := range r {
		r[i] = base36Chars[rand.Intn(len(base36Chars))]
	}
	return fmt.Sprintf("%d-%s", ts, string(r))
}
