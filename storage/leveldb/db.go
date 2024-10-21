package leveldbstorage

import (
	"bytes"
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbOpt "github.com/syndtr/goleveldb/leveldb/opt"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type Storage struct {
	str leveldbStorage.Storage
	ldb *leveldb.DB
	sync.RWMutex
}

func NewStorage(str leveldbStorage.Storage, opt *leveldbOpt.Options) (*Storage, error) {
	ldb, err := leveldb.Open(str, opt)
	if err != nil {
		return nil, storage.ErrConnection.WithMessage(err, "open leveldb")
	}

	return &Storage{ldb: ldb, str: str}, nil
}

func (st *Storage) DB() *leveldb.DB {
	st.RLock()
	defer st.RUnlock()

	return st.ldb
}

func (st *Storage) Close() error {
	st.Lock()
	defer st.Unlock()

	e := util.StringError("close leveldb")

	if st.str == nil {
		return nil
	}

	switch err := st.ldb.Close(); {
	case err == nil, errors.Is(err, leveldbStorage.ErrClosed):
	default:
		return e.WithMessage(storage.ErrInternal.Wrap(err), "close storage")
	}

	switch err := st.str.Close(); {
	case err == nil, errors.Is(err, leveldb.ErrClosed):
		st.str = nil
		st.ldb = nil

		return nil
	default:
		return e.Wrap(storage.ErrInternal.Wrap(err))
	}
}

func (st *Storage) db() (*leveldb.DB, error) {
	st.RLock()
	defer st.RUnlock()

	if st.ldb == nil {
		return nil, storage.ErrClosed.WithStack()
	}

	return st.ldb, nil
}

func (st *Storage) Get(key []byte) ([]byte, bool, error) {
	db, err := st.db()
	if err != nil {
		return nil, false, err
	}

	switch b, err := db.Get(key, nil); {
	case err == nil:
		return b, true, nil
	case errors.Is(err, leveldb.ErrNotFound):
		return nil, false, nil
	default:
		return b, false, storage.ErrExec.WithMessage(err, "get")
	}
}

func (st *Storage) Exists(key []byte) (bool, error) {
	db, err := st.db()
	if err != nil {
		return false, err
	}

	switch b, err := db.Has(key, nil); {
	case err == nil:
		return b, nil
	default:
		return false, storage.ErrExec.WithMessage(err, "check exists")
	}
}

func (st *Storage) Iter(
	r *leveldbutil.Range,
	callback func(key []byte, raw []byte) (bool, error),
	sort bool, // NOTE if true, ascend order
) error {
	db, err := st.db()
	if err != nil {
		return err
	}

	iter := db.NewIterator(r, nil)
	defer iter.Release()

	var seek func() bool
	var next func() bool
	if sort {
		seek = iter.First
		next = iter.Next
	} else {
		seek = iter.Last
		next = iter.Prev
	}

	if !seek() {
		return nil
	}

end:
	for {
		switch keep, err := callback(bytes.Clone(iter.Key()), bytes.Clone(iter.Value())); {
		case err != nil:
			return err
		case !keep:
			break end
		case !next():
			break end
		}
	}

	if err := iter.Error(); err != nil {
		return storage.ErrExec.Errorf("iter")
	}

	return nil
}

func (st *Storage) Put(k, b []byte, opt *leveldbOpt.WriteOptions) error {
	db, err := st.db()
	if err != nil {
		return err
	}

	if err := db.Put(k, b, opt); err != nil {
		return storage.ErrExec.WithMessage(err, "put")
	}

	return nil
}

func (st *Storage) Delete(k []byte, opt *leveldbOpt.WriteOptions) error {
	db, err := st.db()
	if err != nil {
		return err
	}

	if err := db.Delete(k, opt); err != nil {
		return storage.ErrExec.WithMessage(err, "delete")
	}

	return nil
}

func (st *Storage) Batch(batch *leveldb.Batch, wo *leveldbOpt.WriteOptions) error {
	db, err := st.db()
	if err != nil {
		return err
	}

	return errors.WithStack(db.Write(batch, wo))
}

func (st *Storage) Clean() error {
	batch := &leveldb.Batch{}
	defer batch.Reset()

	if _, err := BatchRemove(st, nil, 333); err != nil { //nolint:mnd //...
		return err
	}

	return nil
}

func (st *Storage) BatchFunc(
	ctx context.Context,
	batchsize uint64,
	wo *leveldbOpt.WriteOptions,
) (
	func(func(LeveldbBatch), func(func() error) error) error,
	func(func(func() error) error) error,
	func(),
) {
	return st.BatchFuncWithNewBatch(ctx, batchsize, wo, func() LeveldbBatch {
		return NewDefaultLeveldbBatch(&leveldb.Batch{})
	})
}

func (st *Storage) BatchFuncWithNewBatch(
	ctx context.Context,
	batchsize uint64,
	wo *leveldbOpt.WriteOptions,
	newBatch func() LeveldbBatch,
) (
	add func(func(LeveldbBatch), func(func() error) error) error,
	done func(func(func() error) error) error,
	cancel func(),
) {
	wctx, wctxcancel := context.WithCancel(ctx)

	batchlocked := util.NewLocked(newBatch())
	cancel = func() {
		wctxcancel()

		_ = batchlocked.Empty(func(batch LeveldbBatch, isempty bool) error {
			if !isempty {
				batch.Reset()
			}

			return nil
		})
	}
	savef := func(batch LeveldbBatch) func() error {
		return func() error {
			return st.Batch(batch.LBatch(), wo)
		}
	}
	putf := st.batchAddFunc(batchsize, batchlocked, savef, newBatch)
	donef := st.batchDoneFunc(batchlocked, savef, newBatch)

	return func(put func(LeveldbBatch), doBatch func(func() error) error) error {
			if err := wctx.Err(); err != nil {
				return err
			}

			if err := putf(put, doBatch); err != nil {
				cancel()

				return err
			}

			return nil
		},
		func(doBatch func(func() error) error) error {
			if err := wctx.Err(); err != nil {
				return err
			}

			defer cancel()

			return donef(doBatch)
		}, cancel
}

func (st *Storage) batchAddFunc(
	batchsize uint64,
	batchlocked *util.Locked[LeveldbBatch],
	savef func(LeveldbBatch) func() error,
	newBatch func() LeveldbBatch,
) func(func(LeveldbBatch), func(func() error) error) error {
	return func(put func(LeveldbBatch), doBatch func(func() error) error) error {
		_, err := batchlocked.Set(func(batch LeveldbBatch, isempty bool) (newbatch LeveldbBatch, _ error) {
			if isempty {
				return nil, errors.Errorf("empty batch")
			}

			put(batch)

			if uint64(batch.Len()) < batchsize {
				return nil, util.ErrLockedSetIgnore
			}

			return newBatch(), doBatch(savef(batch))
		})

		return err
	}
}

func (st *Storage) batchDoneFunc(
	batchlocked *util.Locked[LeveldbBatch],
	savef func(LeveldbBatch) func() error,
	newBatch func() LeveldbBatch,
) func(func(func() error) error) error {
	return func(doBatch func(func() error) error) error {
		_, err := batchlocked.Set(func(batch LeveldbBatch, isempty bool) (LeveldbBatch, error) {
			switch {
			case isempty, batch.Len() < 1:
				return nil, util.ErrLockedSetIgnore
			default:
				return newBatch(), doBatch(savef(batch))
			}
		})

		return err
	}
}

func BatchRemove(st *Storage, r *leveldbutil.Range, limit int) (int, error) {
	if _, err := st.db(); err != nil {
		return 0, err
	}

	var removed int

	var batch leveldb.Batch
	defer batch.Reset()

	if r == nil {
		r = &leveldbutil.Range{}
	}

	start := r.Start

	for {
		r.Start = start

		if err := st.Iter(
			r,
			func(key, _ []byte) (bool, error) {
				if batch.Len() == limit {
					start = key

					return false, nil
				}

				batch.Delete(key)

				return true, nil
			},
			true,
		); err != nil {
			return removed, err
		}

		if batch.Len() < 1 {
			break
		}

		if err := st.Batch(&batch, nil); err != nil {
			return removed, err
		}

		removed += batch.Len()

		batch.Reset()
	}

	return removed, nil
}

type LeveldbBatch interface {
	Put([]byte, []byte)
	Delete([]byte)
	Len() int
	Reset()
	LBatch() *leveldb.Batch
}

type DefaultLeveldbBatch struct {
	*leveldb.Batch
}

func NewDefaultLeveldbBatch(b *leveldb.Batch) *DefaultLeveldbBatch {
	return &DefaultLeveldbBatch{
		Batch: b,
	}
}

func (b *DefaultLeveldbBatch) LBatch() *leveldb.Batch {
	return b.Batch
}
