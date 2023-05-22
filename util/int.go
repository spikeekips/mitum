package util

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
)

func int64ToBytes(i int64, endian binary.ByteOrder) []byte {
	b := new(bytes.Buffer)

	_ = binary.Write(b, endian, i)

	return b.Bytes()
}

func Int64ToBytes(i int64) []byte {
	return int64ToBytes(i, binary.BigEndian)
}

func Int64ToBigBytes(i int64) []byte {
	return int64ToBytes(i, binary.BigEndian)
}

func bytesToInt64(b []byte, endian binary.ByteOrder) (int64, error) {
	var i int64
	buf := bytes.NewReader(b)

	if err := binary.Read(buf, endian, &i); err != nil {
		return 0, errors.Wrap(err, "invalid int64 bytes")
	}

	return i, nil
}

func BytesToInt64(b []byte) (int64, error) {
	return bytesToInt64(b, binary.BigEndian)
}

func BigBytesToInt64(b []byte) (int64, error) {
	return bytesToInt64(b, binary.BigEndian)
}

func Uint64ToBytes(i uint64) []byte {
	return uint64ToBytes(i, binary.BigEndian)
}

func Uint64ToBigBytes(i uint64) []byte {
	return uint64ToBytes(i, binary.BigEndian)
}

func uint64ToBytes(i uint64, endian binary.ByteOrder) []byte {
	b := new(bytes.Buffer)
	_ = binary.Write(b, endian, i)

	return b.Bytes()
}

func BytesToUint64(b []byte) (uint64, error) {
	var i uint64
	buf := bytes.NewReader(b)

	if err := binary.Read(buf, binary.BigEndian, &i); err != nil {
		return 0, errors.Wrap(err, "invalid uint64 bytes")
	}

	return i, nil
}

func Float64ToBytes(i float64) []byte {
	bt := math.Float64bits(i)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, bt)

	return b
}

func UintToBytes(i uint) []byte {
	b := new(bytes.Buffer)
	_ = binary.Write(b, binary.BigEndian, uint64(i))

	return b.Bytes()
}

func Uint8ToBytes(i uint8) []byte {
	b := new(bytes.Buffer)
	_ = binary.Write(b, binary.BigEndian, i)

	return b.Bytes()
}
