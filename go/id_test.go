package rpc

import (
	"regexp"
	"testing"
)

func TestGenerateID(t *testing.T) {
	id := GenerateID()

	// Format: {timestamp}-{random9}
	pattern := `^\d+-[a-z0-9]{9}$`
	matched, err := regexp.MatchString(pattern, id)
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Errorf("ID %q does not match pattern %s", id, pattern)
	}
}

func TestGenerateIDUniqueness(t *testing.T) {
	seen := make(map[string]bool)
	for range 1000 {
		id := GenerateID()
		if seen[id] {
			t.Errorf("duplicate ID: %s", id)
		}
		seen[id] = true
	}
}
