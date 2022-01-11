package util

import (
	"github.com/bytedance/sonic"
	sonicencoder "github.com/bytedance/sonic/encoder"
)

func MarshalJSON(v interface{}) ([]byte, error) {
	return sonic.Marshal(v)
}

func UnmarshalJSON(b []byte, v interface{}) error {
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
