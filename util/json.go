package util

import (
	"bytes"
	"io"
	"time"

	"github.com/pkg/errors"
)

var nullJSONBytes = []byte("null")

func IsNilJSON(b []byte) bool {
	i := bytes.TrimSpace(b)

	return len(i) < 1 || bytes.Equal(i, nullJSONBytes)
}

func MarshalJSON(v interface{}) ([]byte, error) {
	return marshalJSON(v)
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
