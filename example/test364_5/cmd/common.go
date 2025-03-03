package main

import (
	"crypto/tls"
	"encoding/binary"
	"io"
	"slices"

	"github.com/madflojo/testcerts"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

func newTLSConfig(serverName string) (*testcerts.KeyPair, *tls.Config, error) {
	ca := testcerts.NewCA()

	kp, err := ca.NewKeyPair(serverName)
	if err != nil {
		return nil, nil, err
	}

	tlsConfig, err := kp.ConfigureTLSConfig(ca.GenerateTLSConfig())
	if err != nil {
		return nil, nil, err
	}

	tlsConfig.InsecureSkipVerify = true

	return kp, tlsConfig, nil
}

func readUint64(r io.Reader, i *uint64) (int, error) {
	switch p, n, err := frame.ReadSized(r, 8); {
	case err != nil && !errors.Is(err, io.EOF),
		n < 1 && errors.Is(err, io.EOF):
		return n, err
	default:
		_, err := binary.Decode(p, binary.BigEndian, i)

		return n, errors.WithStack(err)
	}
}

func writeUint64(w io.Writer, i uint64) error {
	return errors.WithStack(binary.Write(w, binary.BigEndian, i))
}

func readUint64s(r io.Reader, u ...*uint64) error {
	for i := range slices.Values(u) {
		if _, err := readUint64(r, i); err != nil {
			return err
		}
	}

	return nil
}

func writeUint64s(w io.Writer, u ...uint64) error {
	for i := range slices.Values(u) {
		if err := writeUint64(w, i); err != nil {
			return err
		}
	}

	return nil
}

func readFixedFrameString(r io.Reader, i *string) (int, error) {
	return frame.ReadFixedValue(r, i, func(b []byte) (string, error) {
		return string(b), nil
	})
}

func ensureReadFixedFrameString(r io.Reader, i *string) (int, error) {
	return frame.EnsureReadFixedValue(r, i, func(b []byte) (string, error) {
		return string(b), nil
	})
}

func readFixedFrameStringSlice(r io.Reader, i *[]string) error {
	return frame.ReadFixedSliceValue(r, i, func(b []byte) (string, error) {
		return string(b), nil
	})
}

func writeFixedFrameStringSlice(w io.Writer, i []string) error {
	return frame.WriteFixedSlice(w, uint32(len(i)), func(yield func([]byte) bool) {
		slices.Values(i)(func(s string) bool {
			return yield([]byte(s))
		})
	})
}

func readFixedFrameHash(r io.Reader, i *util.Hash) (int, error) {
	return frame.ReadFixedValue(r, i, func(b []byte) (util.Hash, error) {
		return valuehash.NewBytes(b), nil
	})
}

func ensureReadFixedFrameHash(r io.Reader, i *util.Hash) (int, error) {
	return frame.EnsureReadFixedValue(r, i, func(b []byte) (util.Hash, error) {
		return valuehash.NewBytes(b), nil
	})
}

func writeFixedFrameHashes(w io.Writer, u ...util.Hash) error {
	for i := range slices.Values(u) {
		if i == nil {
			continue
		}

		if err := frame.WriteFixed(w, i.Bytes()); err != nil {
			return err
		}
	}

	return nil
}
