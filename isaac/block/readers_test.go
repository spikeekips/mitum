package isaacblock

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

type dummyItemReader struct {
	t   base.BlockItemType
	ht  hint.Hint
	enc encoder.Encoder
	r   *util.CompressedReader
}

func (r *dummyItemReader) Type() base.BlockItemType {
	return r.t
}

func (r *dummyItemReader) Encoder() encoder.Encoder {
	return r.enc
}

func (r *dummyItemReader) Reader() *util.CompressedReader {
	return r.r
}

func (r *dummyItemReader) Decode() (interface{}, error) {
	switch i, err := r.r.Decompress(); {
	case err != nil:
		return nil, err
	default:
		_, err := io.ReadAll(i)

		return nil, err
	}
}

func (r *dummyItemReader) DecodeItems(f func(uint64, uint64, interface{}) error) (uint64, error) {
	_, err := r.Decode()

	return 0, err
}

func newItemReaderFunc(writerhint hint.Hint) isaac.NewBlockItemReaderFunc {
	return func(t base.BlockItemType, enc encoder.Encoder, r *util.CompressedReader) (isaac.BlockItemReader, error) {
		return &dummyItemReader{
			t:   t,
			ht:  writerhint,
			enc: enc,
			r:   r,
		}, nil
	}
}

type testReaders struct {
	BaseTestLocalBlockFS
}

func (t *testReaders) prepare(height base.Height) {
	fs, _, _, _, _, _, _ := t.PrepareFS(base.NewPoint(height, 0), nil, nil)

	_, err := fs.Save(context.Background())
	t.NoError(err)
}

func (t *testReaders) openFile(height base.Height, it base.BlockItemType) *os.File {
	fname, err := DefaultBlockFileName(it, t.Encs.JSON().Hint().Type())
	t.NoError(err)
	p := filepath.Join(t.Root, isaac.BlockHeightDirectory(height), fname)

	f, err := os.Open(p)
	t.NoError(err)

	return f
}

func (t *testReaders) updateItem(height base.Height, it base.BlockItemType, b []byte) {
	fname, err := DefaultBlockFileName(it, t.Encs.JSON().Hint().Type())
	t.NoError(err)
	p := filepath.Join(t.Root, isaac.BlockHeightDirectory(height), fname)

	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	t.NoError(err)
	defer f.Close()

	_, err = f.Write(b)
	t.NoError(err)
}

func (t *testReaders) updateItemHeader(height base.Height, writerhint hint.Hint, it base.BlockItemType) {
	f := t.openFile(height, it)

	// NOTE update writerhint
	buf := bytes.NewBuffer(nil)
	{
		switch br, _, _, err := isaac.LoadBlockItemFileBaseHeader(f); {
		case err != nil:
			t.NoError(err)
		default:
			io.Copy(buf, br)
		}

		f.Close()
	}

	t.NoError(writeBaseHeader(buf, isaac.BlockItemFileBaseItemsHeader{Writer: writerhint, Encoder: t.Encs.JSON().Hint()}))

	t.updateItem(height, it, buf.Bytes())
}

func (t *testReaders) TestItem() {
	height := base.Height(33)

	t.prepare(height)

	readers := isaac.NewBlockItemReaders(t.Root, t.Encs, nil)

	t.NoError(readers.Add(LocalFSWriterHint, newItemReaderFunc(LocalFSWriterHint)))

	t.Run("known", func() {
		called := make(chan isaac.BlockItemReader, 1)
		_, found, err := readers.Item(height, base.BlockItemMap, func(r isaac.BlockItemReader) error {
			called <- r

			return nil
		})
		t.True(found)
		t.NoError(err)

		r := <-called
		t.Equal(LocalFSWriterHint, r.(*dummyItemReader).ht)
	})

	t.Run("unknown", func() {
		unknown := hint.MustNewHint("w1-v0.0.1")
		t.updateItemHeader(height, unknown, base.BlockItemMap)
		defer func() {
			t.updateItemHeader(height, LocalFSWriterHint, base.BlockItemMap)
		}()

		_, found, err := readers.Item(height, base.BlockItemMap, func(isaac.BlockItemReader) error {
			return nil
		})
		t.False(found)
		t.Error(err)
		t.True(errors.Is(err, util.ErrNotFound))
		t.ErrorContains(err, "writer")
	})

	t.Run("compatible", func() {
		ht := hint.MustNewHint("block-localfs-writer-v0.1.1")

		t.updateItemHeader(height, ht, base.BlockItemMap)
		defer func() {
			t.updateItemHeader(height, LocalFSWriterHint, base.BlockItemMap)
		}()

		called := make(chan isaac.BlockItemReader, 1)
		_, found, err := readers.Item(height, base.BlockItemMap, func(r isaac.BlockItemReader) error {
			called <- r

			return nil
		})
		t.True(found)
		t.NoError(err)

		r := <-called
		t.Equal(LocalFSWriterHint, r.(*dummyItemReader).ht)
	})

	t.Run("not compatible", func() {
		ht := hint.MustNewHint("block-localfs-writer-v1.0.0")

		t.updateItemHeader(height, ht, base.BlockItemMap)
		defer func() {
			t.updateItemHeader(height, LocalFSWriterHint, base.BlockItemMap)
		}()

		_, found, err := readers.Item(height, base.BlockItemMap, func(isaac.BlockItemReader) error {
			return nil
		})
		t.False(found)
		t.Error(err)
		t.True(errors.Is(err, util.ErrNotFound))
		t.ErrorContains(err, "writer")
	})
}

func (t *testReaders) TestItemFromReader() {
	height := base.Height(33)

	t.prepare(height)

	readers := isaac.NewBlockItemReaders(t.Root, t.Encs, nil)

	t.NoError(readers.Add(LocalFSWriterHint, newItemReaderFunc(LocalFSWriterHint)))

	t.Run("ok", func() {
		f := t.openFile(height, base.BlockItemMap)
		defer f.Close()

		called := make(chan isaac.BlockItemReader, 1)
		t.NoError(readers.ItemFromReader(base.BlockItemMap, f, "", func(r isaac.BlockItemReader) error {
			called <- r

			return nil
		}))

		r := <-called
		t.Equal(LocalFSWriterHint, r.(*dummyItemReader).ht)
	})

	t.Run("decode", func() {
		ht := hint.MustNewHint("block-localfs-writer-v0.1.1")

		t.updateItemHeader(height, ht, base.BlockItemMap)
		defer func() {
			t.updateItemHeader(height, LocalFSWriterHint, base.BlockItemMap)
		}()

		f := t.openFile(height, base.BlockItemMap)
		defer f.Close()

		buf := bytes.NewBuffer(nil)
		_, err := io.Copy(buf, f)
		t.NoError(err)

		_, err = f.Seek(0, 0)
		t.NoError(err)

		t.NoError(readers.ItemFromReader(base.BlockItemMap, f, "", func(r isaac.BlockItemReader) error {
			rbuf := bytes.NewBuffer(nil)

			_, err := r.Reader().Tee(rbuf, nil)
			t.NoError(err)

			_, err = r.Decode()
			t.NoError(err)

			t.Equal(buf.Bytes(), rbuf.Bytes())

			return nil
		}))
	})

	t.Run("closed reader", func() {
		ht := hint.MustNewHint("local-block-fs-writer-v0.1.1")

		t.updateItemHeader(height, ht, base.BlockItemMap)
		defer func() {
			t.updateItemHeader(height, LocalFSWriterHint, base.BlockItemMap)
		}()

		f := t.openFile(height, base.BlockItemMap)
		f.Close() // close

		err := readers.ItemFromReader(base.BlockItemMap, f, "", func(r isaac.BlockItemReader) error {
			return nil
		})
		t.Error(err)
		t.True(errors.Is(err, os.ErrClosed))
	})
}

func (t *testReaders) TestBrokenItemFiles() {
	height := base.Height(33)

	t.prepare(height)

	readers := isaac.NewBlockItemReaders(t.Root, t.Encs, nil)
	t.NoError(readers.Add(LocalFSWriterHint, newItemReaderFunc(LocalFSWriterHint)))

	t.updateItem(height, base.BlockItemMap, []byte("hehehe"))

	_, found, err := readers.Item(height, base.BlockItemMap, func(isaac.BlockItemReader) error {
		return nil
	})
	t.False(found)
	t.Error(err)
	t.True(errors.Is(err, util.ErrNotFound))
	t.ErrorContains(err, "header")
}

func (t *testReaders) TestCompressedItemFile() {
	height := base.Height(33)

	t.prepare(height)

	readers := isaac.NewBlockItemReaders(t.Root, t.Encs, nil)
	t.NoError(readers.Add(LocalFSWriterHint, newItemReaderFunc(LocalFSWriterHint)))

	t.Run("not compressed", func() {
		_, found, err := readers.Item(height, base.BlockItemMap, func(r isaac.BlockItemReader) error {
			t.Empty(r.Reader().Format)

			return nil
		})
		t.True(found)
		t.NoError(err)
	})

	t.Run("compressed", func() {
		_, found, err := readers.Item(height, base.BlockItemOperations, func(r isaac.BlockItemReader) error {
			t.Equal("gz", r.Reader().Format)

			return nil
		})
		t.True(found)
		t.NoError(err)
	})
}

func TestReaders(t *testing.T) {
	suite.Run(t, new(testReaders))
}
