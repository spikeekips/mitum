package util

import (
	"bytes"

	"github.com/bytedance/sonic"
	sonicencoder "github.com/bytedance/sonic/encoder"
)

var nullJSONBytes = []byte("null")

type Marshaled interface {
	Marshaled() ([]byte, bool)
	SetMarshaled([]byte)
}

type JSONMarshaled struct {
	ismarshaled bool
	marshaled   []byte
}

func (m JSONMarshaled) Marshaled() ([]byte, bool) {
	return m.marshaled, m.ismarshaled
}

func (m *JSONMarshaled) SetMarshaled(b []byte) {
	m.ismarshaled = true
	m.marshaled = b
}

func MarshalJSONWithMarshaled(v interface{}) ([]byte, error) {
	var marshaled Marshaled
	switch j, ok := v.(Marshaled); {
	case !ok:
	default:
		if b, ok := j.Marshaled(); ok {
			return b, nil
		}

		marshaled = j
	}

	b, err := sonic.Marshal(v)

	if marshaled != nil {
		marshaled.SetMarshaled(b)
	}

	return b, err
}

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
