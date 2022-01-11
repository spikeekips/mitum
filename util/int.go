package util

import (
	"bytes"
	"encoding/binary"
)

func Int64ToBytes(i int64) []byte {
	b := new(bytes.Buffer)
	_ = binary.Write(b, binary.LittleEndian, i)

	return b.Bytes()
}

func BytesToInt64(b []byte) (int64, error) {
	var i int64
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.LittleEndian, &i)
	if err != nil {
		return 0, err
	}

	return i, nil
}
