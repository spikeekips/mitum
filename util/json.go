package util

import "bytes"

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

func IsNilJSON(b []byte) bool {
	i := bytes.TrimSpace(b)

	return len(i) < 1 || bytes.Equal(i, nullJSONBytes)
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

	b, err := marshalJSON(v)

	if marshaled != nil {
		marshaled.SetMarshaled(b)
	}

	return b, err
}

func UnmarshalJSON(b []byte, v interface{}) error {
	if IsNilJSON(b) {
		return nil
	}

	return unmarshalJSON(b, v)
}

func MarshalJSONIndent(i interface{}) ([]byte, error) {
	return marshalJSONIndent(i)
}

func MustMarshalJSON(i interface{}) []byte {
	b, err := MarshalJSON(i)
	if err != nil {
		panic(err)
	}

	return b
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
