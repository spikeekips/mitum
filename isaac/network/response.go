package isaacnetwork

import (
	"bytes"
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

func readHint(ctx context.Context, r io.Reader) (ht hint.Hint, _ error) {
	e := util.StringErrorFunc("failed to read hint")

	b := make([]byte, hint.MaxHintLength)
	if _, err := network.EnsureRead(ctx, r, b); err != nil {
		return ht, e(err, "")
	}

	ht, err := hint.ParseHint(string(bytes.TrimRight(b[:hint.MaxHintLength], "\x00")))
	if err != nil {
		return ht, e(err, "")
	}

	return ht, nil
}

func readHeader(ctx context.Context, r io.Reader) ([]byte, error) {
	l := make([]byte, 8)

	if _, err := network.EnsureRead(ctx, r, l); err != nil {
		return nil, errors.Wrap(err, "failed to read header length")
	}

	length, err := util.BytesToUint64(l)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse header length")
	}

	if length < 1 {
		return nil, nil
	}

	h := make([]byte, length)

	if _, err := network.EnsureRead(ctx, r, h); err != nil {
		return nil, errors.Wrap(err, "failed to read header")
	}

	return h, nil
}

func writeHint(w io.Writer, ht hint.Hint) error {
	h := make([]byte, hint.MaxHintLength)
	copy(h, ht.Bytes())

	if _, err := ensureWrite(w, h); err != nil {
		return errors.Wrap(err, "failed to write hint")
	}

	return nil
}

func writeHeader(w io.Writer, header []byte) error {
	e := util.StringErrorFunc("failed to write header")

	l := util.Uint64ToBytes(uint64(len(header)))

	if _, err := ensureWrite(w, l); err != nil {
		return e(err, "")
	}

	if len(header) > 0 {
		if _, err := ensureWrite(w, header); err != nil {
			return e(err, "")
		}
	}

	return nil
}

func ensureWrite(w io.Writer, b []byte) (int, error) {
	switch n, err := w.Write(b); {
	case err != nil:
		return n, errors.Wrap(err, "")
	case n != len(b):
		return n, errors.Errorf("failed to write")
	default:
		return n, nil
	}
}
