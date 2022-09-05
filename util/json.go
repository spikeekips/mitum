package util

import (
	"bytes"
	"io"
	"time"

	"github.com/pkg/errors"
)

var nullJSONBytes = []byte("null")

type JSONMarshaled interface {
	Marshaled() ([]byte, bool)
}

type JSONSetMarshaled interface {
	SetMarshaled([]byte)
}

type DefaultJSONMarshaled struct {
	marshaled   []byte
	ismarshaled bool
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

type StreamEncoder interface {
	Encode(interface{}) error
}

type StreamDecoder interface {
	Decode(interface{}) error
}

func NewJSONStreamEncoder(w io.Writer) StreamEncoder {
	return newJSONStreamEncoder(w)
}

func NewJSONStreamDecoder(r io.Reader) StreamDecoder {
	return newJSONStreamDecoder(r)
}

type ReadableJSONDuration time.Duration

func (d ReadableJSONDuration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *ReadableJSONDuration) UnmarshalJSON(b []byte) error {
	var i interface{}
	if err := UnmarshalJSON(b, &i); err != nil {
		return err
	}

	switch t := i.(type) {
	case int64:
		*d = ReadableJSONDuration(time.Duration(t))
	case string:
		j, err := time.ParseDuration(t)
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal ReadableJSONDuration")
		}

		*d = ReadableJSONDuration(j)
	default:
		return errors.Errorf("unknown duration format, %q", string(b))
	}

	return nil
}
