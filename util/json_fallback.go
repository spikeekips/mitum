//go:build !(darwin || linux || windows) || !amd64
// +build !darwin,!linux,!windows !amd64

package util

import (
	"encoding/json"
	"io"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

var jsoniterconfiged = jsoniter.Config{
	EscapeHTML: false,
}.Froze()

func marshalJSON(v interface{}) ([]byte, error) {
	b, err := jsoniterconfiged.Marshal(v)

	return b, errors.WithStack(err)
}

func unmarshalJSON(b []byte, v interface{}) error {
	return errors.WithStack(jsoniterconfiged.Unmarshal(b, v))
}

func marshalJSONIndent(i interface{}) ([]byte, error) {
	// NOTE jsoniter.MarshalIndent, v1.1.12 does not work ;(
	b, err := json.MarshalIndent(i, "", "  ")

	return b, errors.WithStack(err)
}

func newJSONStreamEncoder(w io.Writer) StreamEncoder {
	return jsoniter.NewEncoder(w)
}

func newJSONStreamDecoder(r io.Reader) StreamDecoder {
	return jsoniter.NewDecoder(r)
}
