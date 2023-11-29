package util

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/suite"
)

type testCompressedReader struct {
	suite.Suite
}

func (t *testCompressedReader) TestReadGzipReader() {
	content := UUID().Bytes()

	t.Run("Read()", func() {
		buf := bytes.NewBuffer(nil)

		gw, err := NewGzipWriter(buf, gzip.BestSpeed)
		t.NoError(err)
		n, err := gw.Write(content)
		t.NoError(err)
		t.Equal(len(content), n)
		t.NoError(gw.Close())

		compressed := buf.Bytes()

		cr, err := NewCompressedReader(io.NopCloser(buf), "gz", nil)
		t.NoError(err)

		b, err := io.ReadAll(cr)
		t.NoError(err)
		t.Equal(compressed, b)
	})

	t.Run("Decompress()", func() {
		buf := bytes.NewBuffer(nil)

		gw, err := NewGzipWriter(buf, gzip.BestSpeed)
		t.NoError(err)
		n, err := gw.Write(content)
		t.NoError(err)
		t.Equal(len(content), n)
		t.NoError(gw.Close())

		cr, err := NewCompressedReader(io.NopCloser(buf), "gz", nil)
		t.NoError(err)

		dr, err := cr.Decompress()
		t.NoError(err)

		b, err := io.ReadAll(dr)
		t.NoError(err)
		t.Equal(content, b)
	})

	t.Run("Decompress() again", func() {
		buf := bytes.NewBuffer(nil)

		gw, err := NewGzipWriter(buf, gzip.BestSpeed)
		t.NoError(err)
		n, err := gw.Write(content)
		t.NoError(err)
		t.Equal(len(content), n)
		t.NoError(gw.Close())

		cr, err := NewCompressedReader(io.NopCloser(buf), "gz", nil)
		t.NoError(err)

		rbuf := bytes.NewBuffer(nil)

		{
			dr, err := cr.Decompress()
			t.NoError(err)

			b := make([]byte, 2)
			_, err = dr.Read(b)
			t.NoError(err)

			rbuf.Write(b)
		}

		dr, err := cr.Decompress()
		t.NoError(err)

		io.Copy(rbuf, dr)

		t.Equal(content, rbuf.Bytes())
	})

	t.Run("Tee", func() {
		buf := bytes.NewBuffer(nil)

		gw, err := NewGzipWriter(buf, gzip.BestSpeed)
		t.NoError(err)
		n, err := gw.Write(content)
		t.NoError(err)
		t.Equal(len(content), n)
		t.NoError(gw.Close())

		compressed := buf.Bytes()

		cr, err := NewCompressedReader(io.NopCloser(buf), "gz", nil)
		t.NoError(err)

		wbuf := bytes.NewBuffer(nil)

		tr, err := cr.Tee(wbuf)
		t.NoError(err)

		b, err := io.ReadAll(tr)
		t.NoError(err)
		t.Equal(content, b)

		t.Equal(compressed, wbuf.Bytes())
	})

	t.Run("Tee, Decompress", func() {
		buf := bytes.NewBuffer(nil)

		gw, err := NewGzipWriter(buf, gzip.BestSpeed)
		t.NoError(err)
		n, err := gw.Write(content)
		t.NoError(err)
		t.Equal(len(content), n)
		t.NoError(gw.Close())

		compressed := buf.Bytes()

		cr, err := NewCompressedReader(io.NopCloser(buf), "gz", nil)
		t.NoError(err)

		wbuf := bytes.NewBuffer(nil)

		_, err = cr.Tee(wbuf)
		t.NoError(err)

		dr, err := cr.Decompress()
		t.NoError(err)

		b, err := io.ReadAll(dr)
		t.NoError(err)
		t.Equal(content, b)

		t.Equal(compressed, wbuf.Bytes())
	})
}

func (t *testCompressedReader) TestReadNotCompressedReader() {
	content := UUID().Bytes()

	t.Run("Read()", func() {
		buf := bytes.NewBuffer(content)

		cr, err := NewCompressedReader(io.NopCloser(buf), "", nil)
		t.NoError(err)

		b, err := io.ReadAll(cr)
		t.NoError(err)
		t.Equal(content, b)
	})

	t.Run("Decompress()", func() {
		buf := bytes.NewBuffer(content)

		cr, err := NewCompressedReader(io.NopCloser(buf), "", nil)
		t.NoError(err)

		dr, err := cr.Decompress()
		t.NoError(err)

		b, err := io.ReadAll(dr)
		t.NoError(err)
		t.Equal(content, b)
	})

	t.Run("with compress format, gz", func() {
		buf := bytes.NewBuffer(content)

		cr, err := NewCompressedReader(io.NopCloser(buf), "gz", nil)
		t.NoError(err)

		_, err = cr.Decompress()
		t.Error(err)
		t.ErrorContains(err, "gzip: invalid header")
	})

	t.Run("Decompress() again", func() {
		buf := bytes.NewBuffer(content)

		cr, err := NewCompressedReader(io.NopCloser(buf), "", nil)
		t.NoError(err)

		rbuf := bytes.NewBuffer(nil)

		{
			dr, err := cr.Decompress()
			t.NoError(err)

			b := make([]byte, 2)
			_, err = dr.Read(b)
			t.NoError(err)

			rbuf.Write(b)
		}

		dr, err := cr.Decompress()
		t.NoError(err)

		io.Copy(rbuf, dr)

		t.Equal(content, rbuf.Bytes())
	})

	t.Run("Tee", func() {
		buf := bytes.NewBuffer(content)

		cr, err := NewCompressedReader(io.NopCloser(buf), "", nil)
		t.NoError(err)

		wbuf := bytes.NewBuffer(nil)

		tr, err := cr.Tee(wbuf)
		t.NoError(err)

		b, err := io.ReadAll(tr)
		t.NoError(err)
		t.Equal(content, b)

		t.Equal(content, wbuf.Bytes())
	})

	t.Run("Tee, Decompress", func() {
		buf := bytes.NewBuffer(content)

		cr, err := NewCompressedReader(io.NopCloser(buf), "", nil)
		t.NoError(err)

		wbuf := bytes.NewBuffer(nil)

		_, err = cr.Tee(wbuf)
		t.NoError(err)

		dr, err := cr.Decompress()
		t.NoError(err)

		b, err := io.ReadAll(dr)
		t.NoError(err)
		t.Equal(content, b)

		t.Equal(content, wbuf.Bytes())
	})
}

func TestCompressedReader(t *testing.T) {
	suite.Run(t, new(testCompressedReader))
}
