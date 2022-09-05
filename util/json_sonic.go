//go:build (darwin || linux || windows) && amd64
// +build darwin linux windows
// +build amd64

package util

import (
	"github.com/bytedance/sonic"
	sonicencoder "github.com/bytedance/sonic/encoder"
	"github.com/pkg/errors"
)

func marshalJSON(v interface{}) ([]byte, error) {
	b, err := sonic.Marshal(v) //nolint:wrapcheck //...

	return b, errors.WithStack(err)
}

func unmarshalJSON(b []byte, v interface{}) error {
	err := sonic.Unmarshal(b, v) //nolint:wrapcheck //...

	return errors.WithStack(err)
}

func marshalJSONIndent(i interface{}) ([]byte, error) {
	b, err := sonicencoder.EncodeIndented(i, "", "  ", 0) //nolint:wrapcheck //...

	return b, errors.WithStack(err)
}

func newJSONStreamEncoder(w io.Writer) StreamEncoder {
	return sonicencoder.NewStreamEncoder(w)
}

func newJSONStreamDecoder(r io.Reader) StreamDecoder {
	return sonicencoder.NewStreamDecoder(r)
}
