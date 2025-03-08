//go:build !(darwin || linux || windows) || !amd64
// +build !darwin,!linux,!windows !amd64

package util

import (
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

func marshalJSON(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)

	return b, errors.WithStack(err)
}

func unmarshalJSON(b []byte, v interface{}) error {
	return errors.WithStack(json.Unmarshal(b, v))
}

func marshalJSONIndent(i interface{}) ([]byte, error) {
	b, err := json.MarshalIndent(i, "", "  ")

	return b, errors.WithStack(err)
}

func newJSONStreamEncoder(w io.Writer) StreamEncoder {
	return json.NewEncoder(w)
}

func newJSONStreamDecoder(r io.Reader) StreamDecoder {
	return json.NewDecoder(r)
}
