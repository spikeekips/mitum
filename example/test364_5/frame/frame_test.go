package frame

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestReadWriteSize(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	buf := bytes.NewBuffer(nil)

	t.Run("Write", func() {
		t.NoError(writeSize(buf, 33))
		t.T().Logf("len=%d bytes=%v", buf.Len(), buf.Bytes())
	})

	t.Run("Read", func() {
		var i uint32
		n, err := readSize(buf, &i)
		t.NoError(err)
		t.Equal(uint32(33), i)
		t.Equal(4, n)
	})

	t.Run("Read: insufficient", func() {
		buf := bytes.NewBuffer([]byte{0, 9})

		var i uint32
		n, err := readSize(buf, &i)
		t.ErrorContains(err, "insufficient read")
		t.Equal(uint32(0), i, "%d", i)
		t.Equal(2, n)
	})
}
