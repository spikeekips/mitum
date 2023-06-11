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
	b, err := marshalJSON(v)
	if err == nil {
		if i, ok := v.(ExtensibleJSONSetter); ok {
			i.SetMarshaledJSON(b)
		}
	}

	return b, err
}

func UnmarshalJSON(b []byte, v interface{}) error {
	if IsNilJSON(b) {
		return nil
	}

	if i, ok := v.(ExtensibleJSONSetter); ok {
		i.SetMarshaledJSON(b)
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

func MustMarshalJSONString(i interface{}) string {
	b, err := MarshalJSON(i)
	if err != nil {
		panic(err)
	}

	return string(b)
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

type ReadableDuration time.Duration

func (d ReadableDuration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *ReadableDuration) UnmarshalJSON(b []byte) error {
	var i interface{}
	if err := UnmarshalJSON(b, &i); err != nil {
		return err
	}

	switch t := i.(type) {
	case int64:
		*d = ReadableDuration(time.Duration(t))
	case string:
		j, err := time.ParseDuration(t)
		if err != nil {
			return errors.Wrap(err, "unmarshal ReadableJSONDuration")
		}

		*d = ReadableDuration(j)
	default:
		return errors.Errorf("unknown duration format, %q", string(b))
	}

	return nil
}

func (d *ReadableDuration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var i interface{}
	if err := unmarshal(&i); err != nil {
		return err
	}

	switch t := i.(type) {
	case int64:
		*d = ReadableDuration(time.Duration(t))
	case string:
		j, err := time.ParseDuration(t)
		if err != nil {
			return errors.Wrap(err, "unmarshal ReadableJSONDuration")
		}

		*d = ReadableDuration(j)
	default:
		return errors.Errorf("unknown duration format, %v", i)
	}

	return nil
}

type ExtensibleJSON interface {
	MarshaledJSON() ([]byte, bool)
}

type ExtensibleJSONSetter interface {
	SetMarshaledJSON([]byte)
}

type DefaultExtensibleJSON struct {
	marshaled   []byte
	isMarshaled bool
}

func (e DefaultExtensibleJSON) MarshaledJSON() ([]byte, bool) {
	return e.marshaled, e.isMarshaled
}

func (e *DefaultExtensibleJSON) SetMarshaledJSON(b []byte) {
	if e.isMarshaled {
		return
	}

	e.marshaled = b
	e.isMarshaled = true
}
