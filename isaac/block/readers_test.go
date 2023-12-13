package isaacblock

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
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

func (t *testReaders) newReaders(args *isaac.BlockItemReadersArgs) *isaac.BlockItemReaders {
	readers := isaac.NewBlockItemReaders(t.Root, t.Encs, args)
	t.NoError(readers.Add(LocalFSWriterHint, newItemReaderFunc(LocalFSWriterHint)))

	return readers
}

func (t *testReaders) TestItem() {
	height := base.Height(33)

	t.prepare(height)

	readers := t.newReaders(nil)

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

	readers := t.newReaders(nil)

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

	readers := t.newReaders(nil)

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

	readers := t.newReaders(nil)

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

func (t *testReaders) TestWriteItemFiles() {
	height := base.Height(33)

	t.prepare(height)
	t.PrintFS(t.Root)

	args := isaac.NewBlockItemReadersArgs()
	readers := t.newReaders(args)

	ech := make(chan int64, 1)
	args.AddEmptyHeightFunc = func(height base.Height) error {
		t.T().Logf("added, %d", height)

		ech <- height.Int64()

		return nil
	}

	args.CancelAddedEmptyHeightFunc = func(height base.Height) error {
		t.T().Logf("canceled, %d", height)

		ech <- height.Int64() * -1

		return nil
	}

	checkUpdatedBytes := func(height base.Height, b33 []byte) {
		found, err := readers.ItemFilesReader(height, func(r io.Reader) error {
			b, err := io.ReadAll(r)
			if err != nil {
				return err
			}

			t.Equal(b33, b)

			return nil
		})
		t.NoError(err)
		t.True(found)
	}

	t.Run("unknown height", func() {
		var b33 []byte

		found, err := readers.ItemFilesReader(33, func(r io.Reader) (err error) {
			b33, err = io.ReadAll(r)

			return err
		})
		t.NoError(err)
		t.True(found)

		updated, err := readers.WriteItemFiles(34, b33)
		t.NoError(err)
		t.False(updated)

		t.Equal(0, readers.EmptyHeightsLock().Len())

		select {
		case <-time.After(time.Millisecond * 100):
		case <-ech:
			t.NoError(errors.Errorf("aded or removed"))
		}
	})

	t.Run("known height, but in local fs", func() {
		var b33 []byte

		found, err := readers.ItemFilesReader(33, func(r io.Reader) (err error) {
			b33, err = io.ReadAll(r)

			return err
		})
		t.NoError(err)
		t.True(found)

		updated, err := readers.WriteItemFiles(33, b33)
		t.NoError(err)
		t.True(updated)

		t.Equal(0, readers.EmptyHeightsLock().Len())

		select {
		case <-time.After(time.Millisecond * 100):
			t.NoError(errors.Errorf("not canceled"))
		case height := <-ech:
			t.Equal(int64(-33), height)
		}
	})

	t.Run("known height, but some items not in local fs", func() {
		bfiles, found, err := readers.ItemFiles(33)
		t.NoError(err)
		t.True(found)

		newbfiles := isaac.NewBlockItemFilesMaker(t.Encs.JSON())

		olditems := bfiles.Items()
		for i := range olditems {
			var newitem base.BlockItemFile = isaac.NewFileBlockItemFile(util.UUID().String(), "gz")

			if i == base.BlockItemMap {
				newitem = olditems[i]
			}

			added, err := newbfiles.SetItem(i, newitem)
			t.NoError(err)
			t.True(added)
		}

		b33, err := newbfiles.Bytes()
		t.NoError(err)

		t.T().Log("new block item files:", string(b33))

		updated, err := readers.WriteItemFiles(33, b33)
		t.NoError(err)
		t.True(updated)

		t.Equal(0, readers.EmptyHeightsLock().Len())

		select {
		case <-time.After(time.Millisecond * 100):
			t.NoError(errors.Errorf("not canceled"))
		case height := <-ech:
			t.Equal(int64(-33), height)
		}

		checkUpdatedBytes(33, b33)
	})

	t.Run("known height, but not in local fs", func() {
		bfiles, found, err := readers.ItemFiles(33)
		t.NoError(err)
		t.True(found)

		newbfiles := isaac.NewBlockItemFilesMaker(t.Encs.JSON())

		for i := range bfiles.Items() {
			added, err := newbfiles.SetItem(i, isaac.NewFileBlockItemFile(util.UUID().String(), "gz"))
			t.NoError(err)
			t.True(added)
		}

		b33, err := newbfiles.Bytes()
		t.NoError(err)

		t.T().Log("new block item files:", string(b33))

		updated, err := readers.WriteItemFiles(33, b33)
		t.NoError(err)
		t.True(updated)

		t.Equal(1, readers.EmptyHeightsLock().Len())
		defer readers.EmptyHeightsLock().Empty()

		select {
		case <-time.After(time.Millisecond * 100):
			t.NoError(errors.Errorf("not canceled"))
		case height := <-ech:
			t.Equal(int64(33), height)
		}

		checkUpdatedBytes(33, b33)
	})

	t.Run("write no local items, but write local items", func() {
		// update block item files
		bfiles, found, err := readers.ItemFiles(height)
		t.NoError(err)
		t.True(found)

		newbfiles := isaac.NewBlockItemFilesMaker(t.Encs.JSON())

		for i := range bfiles.Items() {
			var newitem base.BlockItemFile = isaac.NewFileBlockItemFile(util.UUID().String(), "gz")
			if i == base.BlockItemMap {
				newitem = isaac.NewLocalFSBlockItemFile(util.UUID().String(), "gz")
			}

			added, err := newbfiles.SetItem(i, newitem)
			t.NoError(err)
			t.True(added)
		}

		b33, err := newbfiles.Bytes()
		t.NoError(err)
		t.T().Log("new block item files:", string(b33))

		updated, err := readers.WriteItemFiles(height, b33)
		t.NoError(err)
		t.True(updated)

		checkUpdatedBytes(33, b33)

		select {
		case <-time.After(time.Millisecond * 100):
			t.NoError(errors.Errorf("not canceled"))
		case height := <-ech:
			t.Equal(int64(33)*-1, height)
		}

		t.False(readers.EmptyHeightsLock().Exists(height))
	})

	t.Run("some items missing", func() {
		var oldbfilesbytes []byte
		_, _ = readers.ItemFilesReader(height, func(r io.Reader) error {
			b, err := io.ReadAll(r)
			if err != nil {
				return err
			}

			oldbfilesbytes = b

			return nil
		})
		t.NotNil(oldbfilesbytes)

		bfiles, found, err := readers.ItemFiles(33)
		t.NoError(err)
		t.True(found)

		newbfiles := isaac.NewBlockItemFilesMaker(t.Encs.JSON())

		olditems := bfiles.Items()
		for i := range olditems {
			if i == base.BlockItemOperations {
				continue
			}

			added, err := newbfiles.SetItem(i, isaac.NewFileBlockItemFile(util.UUID().String(), "gz"))
			t.NoError(err)
			t.True(added)
		}

		b33, err := newbfiles.Bytes()
		t.NoError(err)

		t.T().Log("new block item files:", string(b33))

		_, err = readers.WriteItemFiles(33, b33)
		t.Error(err)
		t.ErrorContains(err, `item, "operations" not found`)

		t.Equal(0, readers.EmptyHeightsLock().Len())

		select {
		case <-time.After(time.Millisecond * 100):
		case <-ech:
			t.NoError(errors.Errorf("aded or removed"))
		}

		checkUpdatedBytes(33, oldbfilesbytes)
	})

	t.Run("wrong bytes", func() {
		var oldbfilesbytes []byte
		_, _ = readers.ItemFilesReader(height, func(r io.Reader) error {
			b, err := io.ReadAll(r)
			if err != nil {
				return err
			}

			oldbfilesbytes = b

			return nil
		})
		t.NotNil(oldbfilesbytes)

		var b33 []byte

		found, err := readers.ItemFilesReader(33, func(r io.Reader) (err error) {
			b33, err = io.ReadAll(r)

			return err
		})
		t.NoError(err)
		t.True(found)

		b33 = b33[:333] // wrong json message

		t.T().Log("new block item files:", string(b33))

		_, err = readers.WriteItemFiles(33, b33)
		t.Error(err)
		t.ErrorContains(err, "decode")

		t.Equal(0, readers.EmptyHeightsLock().Len())

		select {
		case <-time.After(time.Millisecond * 100):
		case <-ech:
			t.NoError(errors.Errorf("aded or removed"))
		}

		checkUpdatedBytes(33, oldbfilesbytes)
	})
}

func (t *testReaders) fsfiles() (files []string) {
	_ = filepath.Walk(t.Root, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)

		return nil
	})

	return files
}

func (t *testReaders) TestLoadAndRemoveEmptyHeight() {
	height := base.Height(33)

	t.prepare(height)

	oldfiles := t.fsfiles()

	args := isaac.NewBlockItemReadersArgs()
	readers := t.newReaders(args)
	readers.SetLogging(logging.TestNilLogging)

	var loadedHeights []base.Height

	args.LoadEmptyHeightsFunc = func() ([]base.Height, error) {
		return loadedHeights, nil
	}

	t.Run("empty LoadEmptyHeightsFunc", func() {
		removed, err := readers.LoadAndRemoveEmptyHeightDirectories()
		t.NoError(err)
		t.Equal(uint64(0), removed)

		t.Equal(oldfiles, t.fsfiles())
	})

	t.Run("unknown height in LoadEmptyHeightsFunc", func() {
		loadedHeights = append(loadedHeights, 34)

		removed, err := readers.LoadAndRemoveEmptyHeightDirectories()
		t.NoError(err)
		t.Equal(uint64(1), removed)

		t.Equal(oldfiles, t.fsfiles())
	})

	t.Run("known height in LoadEmptyHeightsFunc", func() {
		ech := make(chan int64, 3)
		args.CancelAddedEmptyHeightFunc = func(height base.Height) error {
			t.T().Logf("canceled, %d", height)

			ech <- height.Int64() * -1

			return nil
		}

		loadedHeights = nil
		loadedHeights = append(loadedHeights, 33, 34, 35)

		removed, err := readers.LoadAndRemoveEmptyHeightDirectories()
		t.NoError(err)
		t.Equal(uint64(len(loadedHeights)), removed)

		t.Equal(oldfiles, t.fsfiles())

		var canceleds []base.Height

	end:
		for {
			select {
			case <-time.After(time.Millisecond * 100):
				t.NoError(errors.Errorf("not canceled"))
			case height := <-ech:
				canceleds = append(canceleds, base.Height(height*-1))
				if len(canceleds) == len(loadedHeights) {
					break end
				}
			}
		}

		sort.Slice(loadedHeights, func(i, j int) bool { return loadedHeights[i].Int64() < loadedHeights[j].Int64() })
		sort.Slice(canceleds, func(i, j int) bool { return canceleds[i].Int64() < canceleds[j].Int64() })
		t.Equal(loadedHeights, canceleds)
	})
}

func (t *testReaders) TestRemoveEmptyHeightLock() {
	args := isaac.NewBlockItemReadersArgs()
	readers := t.newReaders(args)

	clean := func() {
		t.NoError(os.RemoveAll(t.Root))
		t.NoError(os.MkdirAll(t.Root, 0o700))
	}

	t.Run("empty", func() {
		height := base.Height(33)
		t.prepare(height)
		defer clean()

		oldfiles := t.fsfiles()

		removed, err := readers.RemoveEmptyHeightsLock()
		t.NoError(err)
		t.Equal(uint64(0), removed)

		t.Equal(oldfiles, t.fsfiles())
	})

	t.Run("but has local items", func() {
		height := base.Height(33)
		t.prepare(height)
		defer clean()

		oldfiles := t.fsfiles()

		args.RemoveEmptyAfter = 1
		readers.EmptyHeightsLock().SetValue(height, time.Now().Add(time.Second*-1))

		removed, err := readers.RemoveEmptyHeightsLock()
		t.NoError(err)
		t.Equal(uint64(1), removed)
		t.Equal(0, readers.EmptyHeightsLock().Len())

		t.Equal(oldfiles, t.fsfiles())
	})

	t.Run("no local items, but not expired", func() {
		height := base.Height(33)
		t.prepare(height)
		defer clean()

		oldfiles := t.fsfiles()

		// update block item files
		bfiles, found, err := readers.ItemFiles(height)
		t.NoError(err)
		t.True(found)

		newbfiles := isaac.NewBlockItemFilesMaker(t.Encs.JSON())

		for i := range bfiles.Items() {
			added, err := newbfiles.SetItem(i, isaac.NewFileBlockItemFile(util.UUID().String(), "gz"))
			t.NoError(err)
			t.True(added)
		}

		{
			b33, err := newbfiles.Bytes()
			t.NoError(err)
			updated, err := readers.WriteItemFiles(height, b33)
			t.NoError(err)
			t.True(updated)
		}

		args.RemoveEmptyAfter = 1
		readers.EmptyHeightsLock().SetValue(height, time.Now().Add(time.Minute))

		removed, err := readers.RemoveEmptyHeightsLock()
		t.NoError(err)
		t.Equal(uint64(0), removed)
		t.Equal(1, readers.EmptyHeightsLock().Len())

		t.Equal(oldfiles, t.fsfiles())
	})

	t.Run("no local items", func() {
		height := base.Height(33)
		t.prepare(height)
		defer clean()

		oldfiles := t.fsfiles()

		// update block item files
		bfiles, found, err := readers.ItemFiles(height)
		t.NoError(err)
		t.True(found)

		newbfiles := isaac.NewBlockItemFilesMaker(t.Encs.JSON())

		for i := range bfiles.Items() {
			added, err := newbfiles.SetItem(i, isaac.NewFileBlockItemFile(util.UUID().String(), "gz"))
			t.NoError(err)
			t.True(added)
		}

		{
			b33, err := newbfiles.Bytes()
			t.NoError(err)
			updated, err := readers.WriteItemFiles(height, b33)
			t.NoError(err)
			t.True(updated)
		}

		args.RemoveEmptyAfter = 1
		readers.EmptyHeightsLock().SetValue(height, time.Now().Add(time.Second*-1))

		removed, err := readers.RemoveEmptyHeightsLock()
		t.NoError(err)
		t.Equal(uint64(1), removed)

		t.NotEqual(oldfiles, t.fsfiles())
		t.Equal(0, readers.EmptyHeightsLock().Len())

		h33found := slices.Index(t.fsfiles(), filepath.Join(t.Root, isaac.BlockHeightDirectory(height)))
		b33found := slices.Index(t.fsfiles(), isaac.BlockItemFilesPath(t.Root, height))

		t.Equal(-1, h33found, "height directory removed")
		t.True(b33found >= 0, "block item files not removed")
	})
}

func (t *testReaders) TestStartToRemove() {
	args := isaac.NewBlockItemReadersArgs()
	readers := t.newReaders(args)

	removeheight := base.Height(33)
	notremoveheight := base.Height(removeheight + 1)
	t.prepare(removeheight)
	t.prepare(notremoveheight)

	// update block item files; no local item files
	for _, height := range []base.Height{removeheight, notremoveheight} {
		bfiles, found, err := readers.ItemFiles(height)
		t.NoError(err)
		t.True(found)

		newbfiles := isaac.NewBlockItemFilesMaker(t.Encs.JSON())

		for i := range bfiles.Items() {
			added, err := newbfiles.SetItem(i, isaac.NewFileBlockItemFile(util.UUID().String(), "gz"))
			t.NoError(err)
			t.True(added)
		}

		b33, err := newbfiles.Bytes()
		t.NoError(err)
		updated, err := readers.WriteItemFiles(height, b33)
		t.NoError(err)
		t.True(updated)
	}

	oldfiles := t.fsfiles()

	args.RemoveEmptyAfter = 1
	args.RemoveEmptyInterval = time.Millisecond * 33
	// 2 heights are expired
	readers.EmptyHeightsLock().SetValue(removeheight, time.Now().Add(time.Minute*-1))
	readers.EmptyHeightsLock().SetValue(notremoveheight, time.Now().Add(time.Minute))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.NoError(readers.Start(ctx))

	okch := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(time.Millisecond * 33)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				switch {
				case readers.EmptyHeightsLock().Len() != 1:
				case readers.EmptyHeightsLock().Exists(removeheight):
				case !readers.EmptyHeightsLock().Exists(notremoveheight):
				default:
					okch <- struct{}{}

					return
				}
			}
		}
	}()

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("something wrong"))
	case <-okch:
	}

	t.NotEqual(oldfiles, t.fsfiles())
	t.Equal(1, readers.EmptyHeightsLock().Len())

	h33found := slices.Index(t.fsfiles(), filepath.Join(t.Root, isaac.BlockHeightDirectory(removeheight)))
	b33found := slices.Index(t.fsfiles(), isaac.BlockItemFilesPath(t.Root, removeheight))
	h34found := slices.Index(t.fsfiles(), filepath.Join(t.Root, isaac.BlockHeightDirectory(notremoveheight)))
	b34found := slices.Index(t.fsfiles(), isaac.BlockItemFilesPath(t.Root, notremoveheight))

	t.Equal(-1, h33found, "remove height directory removed")
	t.True(b33found >= 0, "remove block item files not removed")

	t.True(h34found >= 0, "not remove height directory not removed")
	t.True(b34found >= 0, "not remove block item files not removed")
}

func TestReaders(t *testing.T) {
	suite.Run(t, new(testReaders))
}
