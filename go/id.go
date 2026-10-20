package rpc

import (
	cryptorand "crypto/rand"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const base36Chars = "0123456789abcdefghijklmnopqrstuvwxyz"

// GenerateID returns a request id in the form "{timestamp}-{random9}".
func GenerateID() string {
	b := make([]byte, 0, 24)
	b = strconv.AppendInt(b, time.Now().UnixMilli(), 10)
	b = append(b, '-')
	for range 9 {
		b = append(b, base36Chars[rand.Intn(len(base36Chars))])
	}
	return string(b)
}

// GenerateReplyPrefix returns a random 10-char base36 prefix that every reply subject of a client starts with.
func GenerateReplyPrefix() string {
	var buf [6]byte
	if _, err := cryptorand.Read(buf[:]); err != nil {
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
