package frame

import (
	"bytes"
	"io"
	"math"
	"slices"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

func TestFixed(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	checkRead := func(r io.Reader, expected []byte) {
		b, err := ReadFixed(r)
		t.NoError(err)
		t.Equal(expected, b)
	}

	t.Run("ok", func() {
		expected := util.UUID().Bytes()

		buf := bytes.NewBuffer(nil)

		t.NoError(WriteFixed(buf, expected))
		checkRead(buf, expected)
	})

	t.Run("over size", func() {
		expected := make([]byte, math.MaxUint32+1)
		clear(expected)

		buf := bytes.NewBuffer(nil)

		err := WriteFixed(buf, expected)
		t.ErrorContains(err, "size too large")
	})

	t.Run("empty", func() {
		buf := bytes.NewBuffer(nil)

		t.NoError(WriteFixed(buf, nil))
		checkRead(buf, nil)
	})
}

type eofBuffer struct {
	*bytes.Buffer
}

func (f *eofBuffer) Read(p []byte) (int, error) {
	b, err := f.Buffer.Read(p)
	if err != nil {
		return b, err
	}

	if f.Buffer.Len() < 1 {
		return b, io.EOF
	}

	return b, nil
}

func TestFixedSlice(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	checkRead := func(buf *bytes.Buffer, expected [][]byte) {
		r := &eofBuffer{Buffer: buf}

		size, seq, err := ReadFixedSlice(r)
		t.NoError(err)
		t.EqualValues(len(expected), size)

		var i int

		for b, err := range seq {
			t.Equal(expected[i], b)

			i++

			switch {
			case i == len(expected):
				t.ErrorIs(err, io.EOF)
			default:
				t.NoError(err)
			}
		}
	}

	t.Run("ok", func() {
		expected := [][]byte{
			util.UUID().Bytes(),
			util.UUID().Bytes(),
		}

		buf := bytes.NewBuffer(nil)

		t.NoError(WriteFixedSlice(buf, 2, slices.Values(expected)))

		checkRead(buf, expected)
	})

	t.Run("empty", func() {
		buf := bytes.NewBuffer(nil)

		t.NoError(WriteFixedSlice(buf, 0, slices.Values(([][]byte)(nil))))

		checkRead(buf, nil)
	})

	t.Run("read; less size", func() {
		expected := [][]byte{
			util.UUID().Bytes(),
			util.UUID().Bytes(),
		}

		buf := bytes.NewBuffer(nil)

		err := WriteFixedSlice(buf, 3, slices.Values(expected))
		t.ErrorContains(err, "less size")

		size, seq, err := ReadFixedSlice(&eofBuffer{Buffer: buf})
		t.EqualValues(3, size)

		var i int
		for b, err := range seq {
			switch {
			case i < 2:
				t.Equal(expected[i], b)
			default:
				t.Nil(b)
				t.ErrorContains(err, "insufficient read")
			}

			i++
		}
	})

	t.Run("write; over size", func() {
		bs := [][]byte{
			util.UUID().Bytes(),
			util.UUID().Bytes(),
		}

		buf := bytes.NewBuffer(nil)

		err := WriteFixedSlice(buf, 1, slices.Values(bs))
		t.ErrorContains(err, "over size")
	})

	t.Run("write; less size", func() {
		bs := [][]byte{
			util.UUID().Bytes(),
			util.UUID().Bytes(),
		}

		buf := bytes.NewBuffer(nil)

		err := WriteFixedSlice(buf, 3, slices.Values(bs))
		t.ErrorContains(err, "less size")
	})
}
