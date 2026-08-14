package diagnostics

import (
	"crypto/rand"
	"errors"
	"strings"
)

const (
	shortIDPrefix        = "SILO-"
	shortIDPayloadLength = 12
	shortIDChars         = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
)

var ErrInvalidShortID = errors.New("invalid diagnostics short id")

func NewShortID() (string, error) {
	var random [shortIDPayloadLength]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	var out [shortIDPayloadLength]byte
	for i, b := range random {
		out[i] = shortIDChars[int(b)&31]
	}
	return shortIDPrefix + string(out[:]), nil
}

func ParseShortID(raw string) (string, error) {
	id := strings.ToUpper(strings.TrimSpace(raw))
	payload := strings.TrimPrefix(id, shortIDPrefix)
	if len(payload) != shortIDPayloadLength {
		return "", ErrInvalidShortID
	}
	for i := 0; i < len(payload); i++ {
		if !isShortIDChar(payload[i]) {
			return "", ErrInvalidShortID
		}
	}
	return shortIDPrefix + payload, nil
}

func isShortIDChar(ch byte) bool {
	for i := 0; i < len(shortIDChars); i++ {
		if shortIDChars[i] == ch {
			return true
		}
	}
	return false
}
