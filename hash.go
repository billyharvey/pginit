package pginit

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
)

func Hash(a []byte) string {
	return hex.EncodeToString(hmac.New(sha1.New, a).Sum(nil))
}
