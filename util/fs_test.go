package util

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type testBufferedResetReader struct {
	suite.Suite
}

func (t *testBufferedResetReader) TestRead() {
	content := []byte(`
a
b
c
d
e
`)

	t.Run("readall", func() {
		s := bytes.NewBuffer(content)

		br := NewBufferedResetReader(s)

		b, err := io.ReadAll(br)
		t.NoError(err)
		t.Equal(content, b)
	})
}

func (t *testBufferedResetReader) TestReset() {
	content := []byte(`
a
b
c
d
e
`)

	t.Run("read nothing and reset", func() {
		s := bytes.NewBuffer(content)

		br := NewBufferedResetReader(s)

		br.Reset()

		b, err := io.ReadAll(br)
		t.NoError(err)
		t.Equal(content, b)
	})

	t.Run("read all and reset", func() {
		s := bytes.NewBuffer(content)

		br := NewBufferedResetReader(s)

		_, err := io.ReadAll(br)
		t.NoError(err)

		br.Reset()

		b, err := io.ReadAll(br)
		t.NoError(err)
		t.Equal(content, b)
	})

	t.Run("read part and reset", func() {
		s := bytes.NewBuffer(content)

		br := NewBufferedResetReader(s)

		p := make([]byte, 3)
		{
			n, err := br.Read(p)
			t.NoError(err)
			t.Equal(len(p), n)
			t.Equal(content[:len(p)], p)
		}

		br.Reset()

		b, err := io.ReadAll(br)
		t.NoError(err)
		t.Equal(content, b)
	})
}

func (t *testBufferedResetReader) TestClose() {
	content := []byte(`
a
b
c
d
e
`)

	t.Run("close", func() {
		s := bytes.NewBuffer(content)

		br := NewBufferedResetReader(s)
		t.NoError(br.Close())
	})

	t.Run("close and read", func() {
		s := bytes.NewBuffer(content)

		br := NewBufferedResetReader(s)
		t.NoError(br.Close())

		p := make([]byte, 3)
		n, err := br.Read(p)
		t.Error(err)
		t.Equal(0, n)
		t.ErrorIs(err, os.ErrClosed)
	})
}

func TestBufferedResetReader(t *testing.T) {
	suite.Run(t, new(testBufferedResetReader))
}
