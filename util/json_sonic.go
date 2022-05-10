//go:build amd64
// +build amd64

package util

import (
	"github.com/bytedance/sonic"
	sonicencoder "github.com/bytedance/sonic/encoder"
)

func marshalJSON(v interface{}) ([]byte, error) {
	return sonic.Marshal(v) //nolint:wrapcheck //...
}

func unmarshalJSON(b []byte, v interface{}) error {
	return sonic.Unmarshal(b, v) //nolint:wrapcheck //...
}

func marshalJSONIndent(i interface{}) ([]byte, error) {
	return sonicencoder.EncodeIndented(i, "", "  ", 0) //nolint:wrapcheck //...
}
