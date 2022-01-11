package jsonenc

import (
	"bytes"
)

const NULL = "null"

var nullbytes = []byte("null")

type Decodable interface {
	DecodeJSON([]byte, *Encoder) error
}

func isNil(b []byte) bool {
	i := bytes.TrimSpace(b)

	return len(i) < 1 || bytes.Equal(i, nullbytes)
}
