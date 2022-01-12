package util

import (
	"bytes"

	"github.com/bytedance/sonic"
	sonicencoder "github.com/bytedance/sonic/encoder"
)

var nullJSONBytes = []byte("null")

func MarshalJSON(v interface{}) ([]byte, error) {
	return sonic.Marshal(v)
}

func UnmarshalJSON(b []byte, v interface{}) error {
	if IsNilJSON(b) {
		return nil
	}

	return sonic.Unmarshal(b, v)
}

func MustMarshalJSON(i interface{}) []byte {
	b, err := MarshalJSON(i)
	if err != nil {
		panic(err)
	}

	return b
}

func MarshalJSONIndent(i interface{}) ([]byte, error) {
	return sonicencoder.EncodeIndented(i, "", "  ", 0)
}

func MustMarshalJSONIndent(i interface{}) []byte {
	b, err := MarshalJSONIndent(i)
	if err != nil {
		panic(err)
	}

	return b
}

func IsNilJSON(b []byte) bool {
	i := bytes.TrimSpace(b)

	return len(i) < 1 || bytes.Equal(i, nullJSONBytes)
}
