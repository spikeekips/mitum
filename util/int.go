package util

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
)

func Int64ToBytes(i int64) []byte {
	b := new(bytes.Buffer)
	_ = binary.Write(b, binary.LittleEndian, i)

	return b.Bytes()
}

func BytesToInt64(b []byte) (int64, error) {
	var i int64
	buf := bytes.NewReader(b)

	if err := binary.Read(buf, binary.LittleEndian, &i); err != nil {
		return 0, errors.Wrap(err, "failed invalid int64 bytes")
	}

	return i, nil
}

func Uint64ToBytes(i uint64) []byte {
	b := new(bytes.Buffer)
	_ = binary.Write(b, binary.LittleEndian, i)

	return b.Bytes()
}

func BytesToUint64(b []byte) (uint64, error) {
	var i uint64
	buf := bytes.NewReader(b)

	if err := binary.Read(buf, binary.LittleEndian, &i); err != nil {
		return 0, errors.Wrap(err, "invalid uint64 bytes")
	}

	return i, nil
}

func Float64ToBytes(i float64) []byte {
	bt := math.Float64bits(i)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, bt)

	return b
}

func UintToBytes(i uint) []byte {
	b := new(bytes.Buffer)
	_ = binary.Write(b, binary.LittleEndian, uint64(i))

	return b.Bytes()
}

func Uint8ToBytes(i uint8) []byte {
	b := new(bytes.Buffer)
	_ = binary.Write(b, binary.LittleEndian, i)

	return b.Bytes()
}
