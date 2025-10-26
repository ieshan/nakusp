package nakusp

import (
	"crypto/rand"
	"encoding/hex"
)

// RandomID generates a cryptographically secure random ID as a 64-character hexadecimal string.
// It panics if the random number generator fails, which should never happen in practice.
func RandomID() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
