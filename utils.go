package nakusp

import (
	"crypto/rand"
	"encoding/hex"
)

func RandomID() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
