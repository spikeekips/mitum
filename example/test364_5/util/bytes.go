package util

import (
	"encoding/binary"
	"io"
	"slices"

	"github.com/pkg/errors"
)

func WriteInt(w io.Writer, i any) error {
	return errors.WithStack(binary.Write(w, binary.BigEndian, i))
}

func ReadInt(r io.Reader, t any) error {
	return errors.WithStack(binary.Read(r, binary.BigEndian, t))
}

func EncodeInt(i any) ([]byte, error) {
	b, err := binary.Append(nil, binary.BigEndian, i)

	return b, errors.WithStack(err)
}

func DecodeInt(b []byte, t any) error {
	_, err := binary.Decode(b, binary.BigEndian, t)

	return errors.WithStack(err)
}

func ReadInts(r io.Reader, u ...any) error {
	for i := range slices.Values(u) {
		if err := ReadInt(r, i); err != nil {
			return err
		}
	}

	return nil
}

func WriteInts(w io.Writer, u ...any) error {
	for i := range slices.Values(u) {
		if err := WriteInt(w, i); err != nil {
			return err
		}
	}

	return nil
}

func EncodeBool(i bool) []byte {
	if i {
		return []byte{0x1}
	}

	return []byte{0x0}
}

func DecodeBool(i []byte) (bool, error) {
	switch {
	case len(i) < 1:
		return false, errors.Errorf("insufficient")
	case i[0] == 0x1:
		return true, nil
	case i[1] == 0x0:
		return false, nil
	default:
		return false, errors.Errorf("unknown byte")
	}
}
