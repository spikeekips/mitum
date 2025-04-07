package main

import (
	"bytes"
	"io"
	"slices"

	"github.com/pkg/errors"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

func writeSMVBundle(w io.Writer, enc encoder.Encoder, opindex uint64, fact util.Hash, values []dop5.SMV) error {
	buf := bytes.NewBuffer(nil)
	defer buf.Reset()

	if err := dutil.WriteInt(buf, opindex); err != nil {
		return err
	}

	if err := frame.WriteFixed(buf, fact.Bytes()); err != nil {
		return err
	}

	seq := func(yield func([]byte, error) bool) {
		for i := range slices.Values(values) {
			b, err := enc.Marshal(i)
			if !yield(b, err) {
				return
			}
		}
	}

	if err := frame.WriteFixedSlice(buf, dutil.SafeIntToUint32(len(values)), seq); err != nil {
		return err
	}

	return frame.WriteFixed(w, buf.Bytes())
}

func decodeSMVBundle(b []byte, enc encoder.Encoder) (uint64, util.Hash, []dop5.SMV, error) {
	buf := bytes.NewBuffer(b)
	defer buf.Reset()

	var opindex uint64
	if err := dutil.ReadInt(buf, &opindex); err != nil {
		return 0, nil, nil, err
	}

	var fact util.Hash
	if _, err := readFixedFrameHash(buf, &fact); err != nil {
		return 0, nil, nil, err
	}

	n, seq, err := frame.ReadFixedSlice(buf)
	if err != nil {
		return 0, nil, nil, err
	}

	values := make([]dop5.SMV, n)
	var i int

end:
	for b, err := range seq {
		var iseof bool
		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			if len(b) < 1 {
				break end
			}

			iseof = true
		default:
			return 0, nil, nil, err
		}

		if err := encoder.Unmarshal(enc, b, &values[i]); err != nil {
			return 0, nil, nil, err
		}

		if iseof {
			break end
		}

		i++
	}

	return opindex, fact, values, nil
}
