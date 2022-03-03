package util

import (
	"bytes"

	"github.com/bytedance/sonic"
	sonicencoder "github.com/bytedance/sonic/encoder"
)

var nullJSONBytes = []byte("null")

type JSONMarshaled interface {
	Marshaled() ([]byte, bool)
}

type JSONSetMarshaled interface {
	SetMarshaled([]byte)
}

type DefaultJSONMarshaled struct {
	ismarshaled bool
	marshaled   []byte
}

func (m DefaultJSONMarshaled) Marshaled() ([]byte, bool) {
	return m.marshaled, m.ismarshaled
}

func (m *DefaultJSONMarshaled) SetMarshaled(b []byte) {
	m.ismarshaled = true
	m.marshaled = b
}

func MarshalJSON(v interface{}) ([]byte, error) {
	var marshaled JSONSetMarshaled
	switch j, ok := v.(JSONMarshaled); {
	case !ok:
	default:
		if b, ok := j.Marshaled(); ok {
			return b, nil
		}

		if k, ok := v.(JSONSetMarshaled); ok {
			marshaled = k
		}
	}

	b, err := sonic.Marshal(v)

	if marshaled != nil {
		marshaled.SetMarshaled(b)
	}

	return b, err
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

func MustMarshalJSONIndentString(i interface{}) string {
	b, err := MarshalJSONIndent(i)
	if err != nil {
		panic(err)
	}

	return string(b)
}

func IsNilJSON(b []byte) bool {
	i := bytes.TrimSpace(b)

	return len(i) < 1 || bytes.Equal(i, nullJSONBytes)
}
