package isaacblock

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

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

func (r *dummyItemReader) Reader() *util.CompressedReader {
	return r.r
}

func (r *dummyItemReader) DecodeHeader(v interface{}) error {
	return util.ErrNotImplemented.Errorf("decode header")
}

func (r *dummyItemReader) Decode(f func(interface{}) error) error {
	return util.ErrNotImplemented.Errorf("decode")
}

func newItemReaderFunc(writerhint hint.Hint) NewItemReaderFunc {
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

func (t *testReaders) updateItem(height base.Height, it base.BlockItemType, b []byte) {
	fname, err := BlockFileName(it, t.Encs.JSON().Hint().Type().String())
	t.NoError(err)
	p := filepath.Join(t.Root, HeightDirectory(height), fname)

	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	t.NoError(err)
	defer f.Close()

	_, err = f.Write(b)
	t.NoError(err)
}

func (t *testReaders) updateItemHeader(height base.Height, writerhint hint.Hint, it base.BlockItemType) {
	fname, err := BlockFileName(it, t.Encs.JSON().Hint().Type().String())
	t.NoError(err)

	p := filepath.Join(t.Root, HeightDirectory(height), fname)

	// NOTE update writerhint
	buf := bytes.NewBuffer(nil)
	{
		f, err := os.Open(p)
		t.NoError(err)
		defer f.Close()

		switch br, _, _, err := loadBaseHeader(f); {
		case err != nil:
			t.NoError(err)
		default:
			io.Copy(buf, br)
		}
	}

	t.NoError(writeBaseHeader(buf, baseItemsHeader{Writer: writerhint, Encoder: t.Encs.JSON().Hint()}))

	t.updateItem(height, it, buf.Bytes())
}

func (t *testReaders) TestNew() {
	height := base.Height(33)

	t.prepare(height)

	readers := NewReaders(t.Root, t.Encs, nil)

	t.NoError(readers.Add(LocalFSWriterHint, newItemReaderFunc(LocalFSWriterHint)))

	t.Run("known", func() {
		called := make(chan isaac.BlockItemReader, 1)
		found, err := readers.Item(height, base.BlockItemMap, func(r isaac.BlockItemReader) error {
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

		found, err := readers.Item(height, base.BlockItemMap, func(isaac.BlockItemReader) error {
			return nil
		})
		t.False(found)
		t.NoError(err)
	})

	t.Run("compatible", func() {
		ht := hint.MustNewHint("local-block-fs-writer-v0.1.1")

		t.updateItemHeader(height, ht, base.BlockItemMap)
		defer func() {
			t.updateItemHeader(height, LocalFSWriterHint, base.BlockItemMap)
		}()

		called := make(chan isaac.BlockItemReader, 1)
		found, err := readers.Item(height, base.BlockItemMap, func(r isaac.BlockItemReader) error {
			called <- r

			return nil
		})
		t.True(found)
		t.NoError(err)

		r := <-called
		t.Equal(LocalFSWriterHint, r.(*dummyItemReader).ht)
	})

	t.Run("not compatible", func() {
		ht := hint.MustNewHint("local-block-fs-writer-v1.0.0")

		t.updateItemHeader(height, ht, base.BlockItemMap)
		defer func() {
			t.updateItemHeader(height, LocalFSWriterHint, base.BlockItemMap)
		}()

		found, err := readers.Item(height, base.BlockItemMap, func(isaac.BlockItemReader) error {
			return nil
		})
		t.False(found)
		t.NoError(err)
	})
}

func (t *testReaders) TestBrokenItemFiles() {
	height := base.Height(33)

	t.prepare(height)

	readers := NewReaders(t.Root, t.Encs, nil)
	t.NoError(readers.Add(LocalFSWriterHint, newItemReaderFunc(LocalFSWriterHint)))

	t.updateItem(height, base.BlockItemMap, []byte("hehehe"))

	found, err := readers.Item(height, base.BlockItemMap, func(isaac.BlockItemReader) error {
		return nil
	})
	t.False(found)
	t.NoError(err)
}

func (t *testReaders) TestCompressedItemFile() {
	height := base.Height(33)

	t.prepare(height)

	readers := NewReaders(t.Root, t.Encs, nil)
	t.NoError(readers.Add(LocalFSWriterHint, newItemReaderFunc(LocalFSWriterHint)))

	t.Run("not compressed", func() {
		found, err := readers.Item(height, base.BlockItemMap, func(r isaac.BlockItemReader) error {
			t.Empty(r.Reader().Format)

			return nil
		})
		t.True(found)
		t.NoError(err)
	})

	t.Run("compressed", func() {
		found, err := readers.Item(height, base.BlockItemOperations, func(r isaac.BlockItemReader) error {
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
