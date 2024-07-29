package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testBlockItemStatesTreeWriterNewWriteFunc struct {
	baseTestBaseBlockItemWriterNewWriteFunc
}

func (t *testBlockItemStatesTreeWriterNewWriteFunc) readHeader(b []byte, v interface{}) error {
	buf := bytes.NewBuffer(b)
	defer buf.Reset()

	_, err := readBlockItemFileHeader(buf, v)

	return err
}

func (t *testBlockItemStatesTreeWriterNewWriteFunc) TestWrite() {
	t.Run("empty create id", func() {
		createWriter, _ := t.newCreateWriter()

		c := newBlockItemStatesTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		_, err := c("")
		t.ErrorContains(err, "empty create id")
	})

	t.Run("zero max size", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemStatesTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		_, d, err := f(context.Background(), 0)
		t.NoError(err)

		bm, err := d(nil)
		t.NoError(err)
		t.Nil(bm)

		written, err := buf()
		t.NoError(err)
		t.Empty(written)
	})

	t.Run("zero max size, but trying to write", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemStatesTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		a, d, err := f(context.Background(), 0)
		t.NoError(err)

		err = a(util.UUID().String())
		t.ErrorContains(err, "zero size")

		bm, err := d(nil)
		t.ErrorContains(err, "zero size")
		t.Nil(bm)

		written, err := buf()
		t.NoError(err)
		t.Empty(written)
	})

	t.Run("add error", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemStatesTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		a, d, err := f(context.Background(), 1)
		t.NoError(err)

		t.NoError(a(util.UUID().String()))
		t.NoError(a(util.UUID().String()))

		bm, err := d(nil)
		t.NoError(err)
		t.NotNil(bm)

		written, err := buf()
		t.NoError(err)
		t.NotEmpty(written)
	})

	t.Run("single", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemStatesTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		a, d, err := f(context.Background(), 1)

		t.NoError(a("showme"))

		bm, err := d(nil)
		t.NoError(err)
		t.NotNil(bm)

		written, err := buf()
		t.NoError(err)

		t.T().Logf("written:\n%s", string(written))

		expected := `# {"tree":"state-fixedtree-v0.0.1","writer":"block-localfs-writer-364-v0.0.1","encoder":"json-encoder-v0.0.1","total":1}
# index=0
{"hash":"4bff5792ba96a36b04dfc48838746d5a92a9d76b7a9ed2d55bd64b4c44be0ef0","key":"showme"}
`
		t.Equal(expected, string(written))

		var header treeItemsHeader

		t.NoError(t.readHeader(written, &header))

		t.Equal(LocalFSWriterHint, header.Writer)
		t.Equal(t.enc.Hint(), header.Encoder)
		t.Equal(base.StateFixedtreeHint, header.Tree)
		t.Equal(uint64(1), header.Total)
	})

	t.Run("grow", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemStatesTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		a, d, err := f(context.Background(), 1, 1)

		t.NoError(a(util.UUID().String()))
		t.NoError(a(util.UUID().String()))
		t.NoError(a(util.UUID().String()))

		bm, err := d(nil)
		t.NoError(err)
		t.NotNil(bm)

		written, err := buf()
		t.NoError(err)
		t.NotEmpty(written)

		t.T().Logf("written:\n%s", string(written))

		var header treeItemsHeader

		t.NoError(t.readHeader(written, &header))

		t.Equal(uint64(3), header.Total)
	})

	t.Run("single; duplicated", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemStatesTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		a, d, err := f(context.Background(), 2)

		t.NoError(a("showme"))
		t.NoError(a("showme"))

		bm, err := d(nil)
		t.NoError(err)
		t.NotNil(bm)

		written, err := buf()
		t.NoError(err)

		t.T().Logf("written:\n%s", string(written))

		expected := `# {"tree":"state-fixedtree-v0.0.1","writer":"block-localfs-writer-364-v0.0.1","encoder":"json-encoder-v0.0.1","total":1}
# index=0
{"hash":"4bff5792ba96a36b04dfc48838746d5a92a9d76b7a9ed2d55bd64b4c44be0ef0","key":"showme"}
`
		t.Equal(expected, string(written))

		var header treeItemsHeader

		t.NoError(t.readHeader(written, &header))

		t.Equal(uint64(1), header.Total)
	})

	t.Run("async", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemStatesTreeWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		n := 3333

		a, d, err := f(context.Background(), uint64(n))
		t.NoError(err)

		jw, err := NewBaseJobWorker(context.Background(), int64(n))
		t.NoError(err)

		for i := 0; i < n; i++ {
			t.NoError(jw.NewJob(func(context.Context) error {
				return a(util.UUID().String())
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

		var header treeItemsHeader

		t.NoError(t.readHeader(written, &header))

		t.Equal(uint64(n), header.Total)
	})
}

func TestBlockItemStatesTreeWriterNewWriteFunc(t *testing.T) {
	suite.Run(t, new(testBlockItemStatesTreeWriterNewWriteFunc))
}
