package rpc

import (
	cryptorand "crypto/rand"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const base36Chars = "0123456789abcdefghijklmnopqrstuvwxyz"

// GenerateID produces a request ID in the format "{timestamp}-{random9}",
func GenerateID() string {
	// 13-digit ms timestamp + '-' + 9 random chars = 23 bytes; a single
	// allocation via append/AppendInt (no fmt.Sprintf reflection overhead).
	b := make([]byte, 0, 24)
	b = strconv.AppendInt(b, time.Now().UnixMilli(), 10)
	b = append(b, '-')
	for range 9 {
		b = append(b, base36Chars[rand.Intn(len(base36Chars))])
	}
	return string(b)
}

// GenerateReplyPrefix produces a client-local reply prefix: 10 base36 chars
// (48 random bits, crypto-backed). Used as the first dot-separated segment of
// every id a client generates when no ConnID is configured, so all reply
// subjects of a client share one wildcard-subscribable prefix
// (`rpc.reply.<prefix>.>`) — the muxed reply inbox.
func GenerateReplyPrefix() string {
	var buf [6]byte
	if _, err := cryptorand.Read(buf[:]); err != nil {
		// Extremely unlikely; fall back to math/rand.
		for i := range buf {
			buf[i] = byte(rand.Intn(256))
		}
	}
	var v uint64
	for _, b := range buf {
		v = v<<8 | uint64(b)
	}
	s := strconv.FormatUint(v, 36)
	if len(s) < 10 {
		s = strings.Repeat("0", 10-len(s)) + s
	}
	return s
}
