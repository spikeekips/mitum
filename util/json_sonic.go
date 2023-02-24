//go:build (darwin || linux || windows) && amd64
// +build darwin linux windows
// +build amd64

package util

import (
	"io"

	"github.com/bytedance/sonic"
	sonicdecoder "github.com/bytedance/sonic/decoder"
	sonicencoder "github.com/bytedance/sonic/encoder"
	"github.com/pkg/errors"
)

func marshalJSON(v interface{}) ([]byte, error) {
	b, err := sonic.Marshal(v)

	return b, errors.WithStack(err)
}

func unmarshalJSON(b []byte, v interface{}) error {
	return errors.WithStack(sonic.Unmarshal(b, v))
}

func marshalJSONIndent(i interface{}) ([]byte, error) {
	b, err := sonicencoder.EncodeIndented(i, "", "  ", 0)

	return b, errors.WithStack(err)
}

func newJSONStreamEncoder(w io.Writer) StreamEncoder {
	return sonicencoder.NewStreamEncoder(w)
}

func newJSONStreamDecoder(r io.Reader) StreamDecoder {
	return sonicdecoder.NewStreamDecoder(r)
}
