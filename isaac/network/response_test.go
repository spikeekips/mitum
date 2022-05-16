package isaacnetwork

import (
	"bytes"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testResponse struct {
	suite.Suite
}

func (t *testResponse) TestWrite() {
	t.Run("ok", func() {
		buf := bytes.NewBuffer(nil)

		header := util.UUID().Bytes()

		t.NoError(writeHeader(buf, header))

		b, err := readHeader(buf)
		t.NoError(err)

		t.Equal(header, b)
	})

	t.Run("zero header", func() {
		buf := bytes.NewBuffer(nil)

		t.NoError(writeHeader(buf, nil))

		b, err := readHeader(buf)
		t.NoError(err)

		t.Equal(0, len(b))
	})
}

func (t *testResponse) TestRead() {
	buf := bytes.NewBuffer(nil)

	header := util.UUID().Bytes()

	t.NoError(writeHeader(buf, header))
	written := buf.Bytes()

	t.Run("ok", func() {
		buf := bytes.NewBuffer(written)

		b, err := readHeader(buf)
		t.NoError(err)

		t.Equal(header, b)
	})

	t.Run("wrong header length", func() {
		buf := bytes.NewBuffer([]byte{'0', '1'})

		_, err := readHeader(buf)
		t.Error(err)
		t.ErrorContains(err, "failed to read header length")
	})

	t.Run("failed to read header", func() {
		buf := bytes.NewBuffer(written[:10])

		_, err := readHeader(buf)
		t.Error(err)
		t.ErrorContains(err, "failed to read header")
	})
}

func TestResponse(t *testing.T) {
	suite.Run(t, new(testResponse))
}
