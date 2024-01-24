package leveldbstorage

import (
	"context"
	"fmt"
	"os"
	"sort"
	"syscall"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/goleak"
)

type testStorage struct {
	suite.Suite
	root string
}

func (t *testStorage) SetupTest() {
	d, err := os.MkdirTemp("", "leveldb")
	t.NoError(err)

	t.root = d
}

func (t *testStorage) TearDownTest() {
	defer os.RemoveAll(t.root)
}

func (t *testStorage) TestNew() {
	st := NewFSStorage(t.root)
	defer st.Close()

	b, found, err := st.Get(util.UUID().Bytes())
	t.Nil(b)
	t.False(found)
	t.NoError(err)
}

func (t *testStorage) TestCloseAgain() {
	st := NewFSStorage(t.root)

	t.NoError(st.Close())
	t.NoError(st.Close())
}

func (t *testStorage) TestPut() {
	st := NewMemStorage()
	defer st.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		t.NoError(st.Put([]byte(b.String()), b.Bytes(), nil))
	}

	for k := range bs {
		v, found, err := st.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}
}

func (t *testStorage) TestDelete() {
	st := NewMemStorage()
	defer st.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		t.NoError(st.Put([]byte(b.String()), b.Bytes(), nil))
	}

	deleted := map[string]struct{}{}

	var i int
	for k := range bs {
		if i == 5 {
			break
		}

		t.NoError(st.Delete([]byte(k), nil))
		deleted[k] = struct{}{}

		i++
	}

	for k := range bs {
		v, found, err := st.Get([]byte(k))

		if _, dfound := deleted[k]; dfound {
			t.False(found)
		} else {
			t.NoError(err)
			t.True(found)

			t.Equal(bs[k], v)
		}
	}
}

func (t *testStorage) TestPutBatch() {
	st := NewMemStorage()
	defer st.Close()

	var batch leveldb.Batch

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		batch.Put([]byte(b.String()), b.Bytes())
	}

	t.NoError(st.Batch(&batch, nil))

	for k := range bs {
		v, found, err := st.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}
}

func (t *testStorage) TestDeleteBatch() {
	st := NewMemStorage()
	defer st.Close()

	var batch leveldb.Batch

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		batch.Put([]byte(b.String()), b.Bytes())
	}

	t.NoError(st.Batch(&batch, nil))

	deleted := map[string]struct{}{}

	batch.Reset()

	var i int
	for k := range bs {
		if i == 5 {
			break
		}

		batch.Delete([]byte(k))
		deleted[k] = struct{}{}

		i++
	}

	t.NoError(st.Batch(&batch, nil))

	for k := range bs {
		v, found, err := st.Get([]byte(k))

		if _, dfound := deleted[k]; dfound {
			t.False(found)
		} else {
			t.NoError(err)
			t.True(found)

			t.Equal(bs[k], v)
		}
	}
}

func (t *testStorage) TestCompaction() {
	st := NewMemStorage()
	defer st.Close()

	var batch leveldb.Batch

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		batch.Put([]byte(b.String()), b.Bytes())
	}

	t.NoError(st.Batch(&batch, nil))

	batch.Reset()

	for k := range bs {
		batch.Put([]byte(k), bs[k])
	}
	t.NoError(st.Batch(&batch, nil))

	for k := range bs {
		v, found, err := st.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}

	var count int
	t.NoError(st.Iter(nil, func([]byte, []byte) (bool, error) {
		count++

		return true, nil
	}, true))

	t.Equal(33, count)
}

func (t *testStorage) TestOpenLocked() {
	t.Run("open locked; panic", func() {
		st := NewFSStorage(t.root)
		defer st.Close()

		_, err := leveldbStorage.OpenFile(t.root, false)
		t.Error(err)

		var serr syscall.Errno
		t.ErrorAs(err, &serr)
		t.True(serr.Temporary())
	})

	t.Run("open locked; readonly", func() {
		_ = NewFSStorage(t.root)

		_, err := leveldbStorage.OpenFile(t.root, true)
		t.Error(err)

		var serr syscall.Errno
		t.ErrorAs(err, &serr)
		t.True(serr.Temporary())
	})
}

func (t *testStorage) TestIter() {
	st := NewMemStorage()
	defer st.Close()

	var batch leveldb.Batch

	bs := make([]string, 33)
	for i := range make([]int, 33) {
		b := util.ULID()
		bs[i] = b.String()

		batch.Put([]byte(b.String()), b.Bytes())
	}

	t.NoError(st.Batch(&batch, nil))

	t.Run("nil range", func() {
		var rbs []string

		t.NoError(st.Iter(nil, func(key []byte, _ []byte) (bool, error) {
			rbs = append(rbs, string(key))

			return true, nil
		}, true))

		t.Equal(33, len(rbs))
		t.Equal(bs, rbs)
	})

	t.Run("range start", func() {
		r := &leveldbutil.Range{Start: []byte(bs[9])}

		var rbs []string

		t.NoError(st.Iter(r, func(key []byte, _ []byte) (bool, error) {
			rbs = append(rbs, string(key))

			return true, nil
		}, true))

		t.Equal(24, len(rbs))
		t.Equal(bs[9:], rbs)
	})

	t.Run("range limit", func() {
		r := &leveldbutil.Range{Limit: []byte(bs[9])}

		var rbs []string

		t.NoError(st.Iter(r, func(key []byte, _ []byte) (bool, error) {
			rbs = append(rbs, string(key))

			return true, nil
		}, true))

		t.Equal(9, len(rbs))
		t.Equal(bs[:9], rbs)
	})

	t.Run("range start + limit", func() {
		r := &leveldbutil.Range{Start: []byte(bs[3]), Limit: []byte(bs[9])}

		var rbs []string

		t.NoError(st.Iter(r, func(key []byte, _ []byte) (bool, error) {
			rbs = append(rbs, string(key))

			return true, nil
		}, true))

		t.Equal(6, len(rbs))
		t.Equal(bs[3:9], rbs)
	})

	t.Run("sort; range start < limit", func() {
		r := &leveldbutil.Range{
			Start: []byte(bs[9]),
			Limit: []byte(bs[3]),
		}

		var rbs []string

		t.NoError(st.Iter(r, func(key []byte, _ []byte) (bool, error) {
			rbs = append(rbs, string(key))

			return true, nil
		}, true))

		t.Equal(0, len(rbs))
	})

	t.Run("reverse sort; range start < limit", func() {
		r := &leveldbutil.Range{
			Start: []byte(bs[9]),
			Limit: []byte(bs[3]),
		}

		var rbs []string

		t.NoError(st.Iter(r, func(key []byte, _ []byte) (bool, error) {
			rbs = append(rbs, string(key))

			return true, nil
		}, false))

		t.Equal(0, len(rbs))
	})

	t.Run("reverse sort; range start + limit", func() {
		r := &leveldbutil.Range{
			Start: []byte(bs[3]),
			Limit: []byte(bs[9]),
		}

		var rbs []string

		t.NoError(st.Iter(r, func(key []byte, _ []byte) (bool, error) {
			rbs = append(rbs, string(key))

			return true, nil
		}, false))

		sbs := bs[3:9]
		sort.Sort(sort.Reverse(sort.StringSlice(sbs)))

		t.Equal(6, len(rbs))
		t.Equal(sbs, rbs)
	})
}

func (t *testStorage) TestPutBatchFunc() {
	putf := func(key, b []byte) func(batch LeveldbBatch) {
		return func(batch LeveldbBatch) {
			batch.Put(key, b)
		}
	}

	doBatch := func(f func() error) error {
		return f()
	}

	t.Run("ok", func() {
		st := NewMemStorage()
		defer st.Close()

		addf, donef, cancelf := st.BatchFunc(context.Background(), 1, nil)
		defer cancelf()

		bs := make([][]byte, 3333)

		ew, _ := util.NewErrgroupWorker(context.Background(), int64(len(bs)))

		go func() {
			for i := range bs {
				b := util.UUID().Bytes()
				bs[i] = b

				t.NoError(ew.NewJob(func(context.Context, uint64) error {
					return addf(putf(b, b), doBatch)
				}))
			}

			ew.Done()
		}()

		t.NoError(ew.Wait())

		t.NoError(donef(doBatch))

		for i := range bs {
			b, found, err := st.Get(bs[i])
			t.NoError(err)
			t.True(found)
			t.Equal(bs[i], b)
		}
	})

	t.Run("put, delete", func() {
		st := NewMemStorage()
		defer st.Close()

		inserted := util.UUID().Bytes()
		t.NoError(st.Put(inserted, inserted, nil))

		addf, donef, cancelf := st.BatchFunc(context.Background(), 1, nil)
		defer cancelf()

		bs := make([][]byte, 10)

		ew, _ := util.NewErrgroupWorker(context.Background(), 333)

		go func() {
			for i := range bs {
				b := util.UUID().Bytes()
				bs[i] = b

				t.NoError(ew.NewJob(func(context.Context, uint64) error {
					return addf(putf(b, b), doBatch)
				}))
			}

			ew.Done()
		}()

		t.NoError(addf(func(batch LeveldbBatch) {
			batch.Delete(inserted)
		}, doBatch))

		t.NoError(ew.Wait())

		t.NoError(donef(doBatch))

		for i := range bs {
			b, found, err := st.Get(bs[i])
			t.NoError(err)
			t.True(found)
			t.Equal(bs[i], b)
		}

		found, err := st.Exists(inserted)
		t.NoError(err)
		t.False(found)
	})

	t.Run("put after done", func() {
		st := NewMemStorage()
		defer st.Close()

		addf, donef, cancelf := st.BatchFunc(context.Background(), 1, nil)
		defer cancelf()

		t.NoError(addf(putf(util.UUID().Bytes(), util.UUID().Bytes()), doBatch))

		t.NoError(donef(doBatch))

		err := addf(putf(util.UUID().Bytes(), util.UUID().Bytes()), doBatch)
		t.Error(err)
		t.True(errors.Is(err, context.Canceled))
	})

	t.Run("done after done", func() {
		st := NewMemStorage()
		defer st.Close()

		addf, donef, cancelf := st.BatchFunc(context.Background(), 1, nil)
		defer cancelf()

		t.NoError(addf(putf(util.UUID().Bytes(), util.UUID().Bytes()), doBatch))

		t.NoError(donef(doBatch))

		err := donef(doBatch)
		t.Error(err)
		t.True(errors.Is(err, context.Canceled))
	})

	t.Run("cancel", func() {
		st := NewMemStorage()
		defer st.Close()

		addf, _, cancelf := st.BatchFunc(context.Background(), 1, nil)

		put := func() error {
			b := util.UUID().Bytes()

			return addf(putf(b, b), doBatch)
		}

		t.NoError(put())
		t.NoError(put())

		cancelf()
		err := put()
		t.Error(err)
		t.True(errors.Is(err, context.Canceled))
	})

	t.Run("cancel under async", func() {
		st := NewMemStorage()
		defer st.Close()

		addf, donef, cancelf := st.BatchFunc(context.Background(), 1, nil)
		defer cancelf()

		bs := make([][]byte, 3333)

		ew, _ := util.NewErrgroupWorker(context.Background(), int64(len(bs)))

		go func() {
			for i := range bs {
				b := util.UUID().Bytes()
				bs[i] = b

				_ = ew.NewJob(func(_ context.Context, jobid uint64) error {
					if jobid == uint64(len(bs)/2) {
						go cancelf()
					}

					return addf(putf(b, b), doBatch)
				})
			}

			ew.Done()
		}()

		err := ew.Wait()
		t.Error(err)
		if !errors.Is(err, context.Canceled) {
			t.ErrorContains(err, "empty batch", "%+v", err)
		}

		err = donef(doBatch)
		t.Error(err)
		if !errors.Is(err, context.Canceled) {
			t.ErrorContains(err, "empty batch", "%+v", err)
		}
	})

	t.Run("context cancel", func() {
		st := NewMemStorage()
		defer st.Close()

		ctx, cancel := context.WithCancel(context.Background())
		addf, _, cancelf := st.BatchFunc(ctx, 1, nil)
		defer cancelf()

		put := func() error {
			b := util.UUID().Bytes()

			return addf(putf(b, b), doBatch)
		}

		t.NoError(put())
		t.NoError(put())

		cancel()
		err := put()
		t.Error(err)
		t.True(errors.Is(err, context.Canceled))
	})
}

func TestStorage(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).compactionError"),
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mCompaction"),
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).tCompaction"),
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*session).refLoop"),
	)

	suite.Run(t, new(testStorage))
}

type benchmarkPut struct {
	st     *Storage
	closef func()
}

func newBenchmarkPut() *benchmarkPut {
	b := &benchmarkPut{}

	root, err := os.MkdirTemp("", "leveldb")
	if err != nil {
		panic(err)
	}

	st := NewFSStorage(root)
	b.st = st
	b.closef = func() {
		st.Close()
		os.RemoveAll(root)
	}

	return b
}

func (b *benchmarkPut) close() {
	b.closef()
}

func (b *benchmarkPut) key() []byte {
	return util.UUID().Bytes()
}

func (b *benchmarkPut) body() []byte {
	body := make([]byte, 16*9)
	for i := 0; i < 9; i++ {
		copy(body[i*9:], util.UUID().Bytes())
	}

	return body
}

func (b *benchmarkPut) benchmarkPut(size int) {
	for i := 0; i < size; i++ {
		if err := b.st.Put(b.key(), b.body(), nil); err != nil {
			panic(err)
		}
	}
}

func (b *benchmarkPut) benchmarkBatch(size int) {
	var batch leveldb.Batch

	for i := 0; i < size; i++ {
		batch.Put(b.key(), b.body())
	}

	if err := b.st.Batch(&batch, nil); err != nil {
		panic(err)
	}
}

func (b *benchmarkPut) batchsize(size int) (batchsize uint64) {
	batchsize = 333

	if s := uint64(size); s < batchsize {
		batchsize = s / 2
	}

	return
}

func (b *benchmarkPut) benchmarkBatchFunc(size int) {
	add, done, cancel := b.st.BatchFunc(context.Background(), b.batchsize(size), nil)
	defer cancel()

	for i := 0; i < size; i++ {
		if err := add(
			func(batch LeveldbBatch) {
				batch.Put(b.key(), b.body())
			},
			func(f func() error) error {
				return f()
			},
		); err != nil {
			panic(err)
		}
	}

	if err := done(
		func(f func() error) error {
			return f()
		},
	); err != nil {
		panic(err)
	}
}

func (b *benchmarkPut) benchmarkBatchFuncAsync(size int) {
	ew, _ := util.NewErrgroupWorker(context.Background(), int64(b.batchsize(size)))

	add, done, cancel := b.st.BatchFunc(context.Background(), b.batchsize(size), nil)
	defer cancel()

	for i := 0; i < size; i++ {
		if err := add(
			func(batch LeveldbBatch) {
				batch.Put(b.key(), b.body())
			},
			func(f func() error) error {
				return ew.NewJob(func(context.Context, uint64) error {
					return f()
				})
			},
		); err != nil {
			panic(err)
		}
	}

	if err := done(
		func(f func() error) error {
			return ew.NewJob(func(context.Context, uint64) error {
				return f()
			})
		},
	); err != nil {
		panic(err)
	}

	ew.Done()
	if err := ew.Wait(); err != nil {
		panic(err)
	}
}

func benchmarkPutFunc(b *testing.B, f func(*benchmarkPut, int)) {
	put := newBenchmarkPut()
	defer put.close()

	n := 30000

	for size := 1000; size < 100000; size = size + n - (size % 10000) {
		b.Run(fmt.Sprintf("size=%d", size), func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				f(put, size)
			}
		})
	}
}

func BenchmarkPut(b *testing.B) {
	benchmarkPutFunc(b, func(put *benchmarkPut, size int) {
		put.benchmarkPut(size)
	})
}

func BenchmarkPutBatch(b *testing.B) {
	benchmarkPutFunc(b, func(put *benchmarkPut, size int) {
		put.benchmarkBatch(size)
	})
}

func BenchmarkPutBatchFunc(b *testing.B) {
	benchmarkPutFunc(b, func(put *benchmarkPut, size int) {
		put.benchmarkBatchFunc(size)
	})
}

func BenchmarkPutBatchFuncAsync(b *testing.B) {
	benchmarkPutFunc(b, func(put *benchmarkPut, size int) {
		put.benchmarkBatchFuncAsync(size)
	})
}
