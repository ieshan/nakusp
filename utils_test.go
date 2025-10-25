package main

import (
	"encoding/hex"
	"testing"
)

func TestRandomIDLengthAndHex(t *testing.T) {
	id := RandomID()
	if len(id) != 64 {
		t.Fatalf("expected length 64, got %d", len(id))
	}
	if _, err := hex.DecodeString(id); err != nil {
		t.Fatalf("RandomID returned non-hex string: %v", err)
	}
}

func TestRandomIDUniqueness(t *testing.T) {
	const sampleSize = 100
	seen := make(map[string]struct{}, sampleSize)
	for i := 0; i < sampleSize; i++ {
		id := RandomID()
		if _, exists := seen[id]; exists {
			t.Fatalf("RandomID produced duplicate value in %d samples", sampleSize)
		}
		seen[id] = struct{}{}
	}
}
