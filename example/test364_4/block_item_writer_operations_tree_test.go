package main

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testBlockItemOperationsTreeWriterNewWriteFunc struct {
	baseTestBaseBlockItemWriterNewWriteFunc
}

func (t *testBlockItemOperationsTreeWriterNewWriteFunc) TestWrite() {
	t.Run("empty create id", func() {
		createWriter, _ := t.newCreateWriter()

		c := newBlockItemOperationsTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		_, err := c("")
		t.ErrorContains(err, "empty create id")
	})

	t.Run("zero total", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemOperationsTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		_, d := f(context.Background(), 0)
		bm, err := d(nil)
		t.NoError(err)
		t.Nil(bm)

		written, err := buf()
		t.NoError(err)
		t.Empty(written)
	})

	t.Run("zero total, but trying to write", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemOperationsTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		a, d := f(context.Background(), 0)

		err = a(0, valuehash.RandomSHA256(), true)
		t.ErrorContains(err, "zero total")

		bm, err := d(nil)
		t.NoError(err)
		t.Nil(bm)

		written, err := buf()
		t.NoError(err)
		t.Empty(written)
	})

	t.Run("add error", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemOperationsTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		a, d := f(context.Background(), 2)

		t.NoError(a(0, valuehash.RandomSHA256(), true))
		err = a(2, valuehash.RandomSHA256(), true)
		t.ErrorContains(err, "out of range")

		bm, err := d(nil)
		t.ErrorContains(err, "out of range")
		t.Nil(bm)

		written, err := buf()
		t.NoError(err)
		t.Empty(written)
	})

	t.Run("single", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemOperationsTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		a, d := f(context.Background(), 1)

		t.NoError(a(0, valuehash.NewBytes([]byte("findme")), true))

		bm, err := d(nil)
		t.NoError(err)
		t.NotNil(bm)

		written, err := buf()
		t.NoError(err)

		t.T().Logf("written:\n%s", string(written))

		expected := `# {"tree":"operation-fixedtree-v0.0.1","writer":"block-localfs-writer-364-v0.0.1","encoder":"json-encoder-v0.0.1","total":1}
# index=0
{"reason":null,"hash":"687e5adef44035829fc5ed20171d322d11ae20659b24e0748ba659f544585a02","key":"66696e646d65"}
`
		t.Equal(expected, string(written))
	})

	t.Run("async", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemOperationsTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		n := 3333

		a, d := f(context.Background(), uint64(n))

		jw, err := NewBaseJobWorker(context.Background(), int64(n))
		t.NoError(err)

		for i := 0; i < n; i++ {
			i := i

			t.NoError(jw.NewJob(func(context.Context) error {
				return a(uint64(i), valuehash.RandomSHA256(), true)
			}))
		}

		jw.JobDone()
		t.NoError(jw.Wait())

		bm, err := d(nil)
		t.NoError(err)
		t.NotNil(bm)

		written, err := buf()
		t.NoError(err)

		t.T().Logf("written:\n%s", string(written[:999]))
	})
}

func TestBlockItemOperationsTreeWriterNewWriteFunc(t *testing.T) {
	suite.Run(t, new(testBlockItemOperationsTreeWriterNewWriteFunc))
}
