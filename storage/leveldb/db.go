package leveldbstorage

import (
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
	db  *leveldb.DB
	sync.Mutex
}

func NewStorage(str leveldbStorage.Storage, opt *leveldbOpt.Options) (*Storage, error) {
	db, err := leveldb.Open(str, opt)
	if err != nil {
		return nil, storage.ConnectionError.Wrapf(err, "failed to open leveldb")
	}

	return &Storage{db: db, str: str}, nil
}

func (st *Storage) DB() *leveldb.DB {
	return st.db
}

func (st *Storage) Close() error {
	st.Lock()
	defer st.Unlock()

	e := util.StringErrorFunc("failed to close leveldb")

	if st.str == nil {
		return nil
	}

	switch err := st.db.Close(); {
	case err == nil:
	case errors.Is(err, leveldbStorage.ErrClosed):
		return nil
	default:
		return e(storage.InternalError.Wrapf(err, ""), "failed to close storage")
	}

	switch err := st.str.Close(); {
	case err == nil:
		st.str = nil
		st.db = nil

		return nil
	case errors.Is(err, leveldb.ErrClosed):
		return nil
	default:
		return e(storage.InternalError.Wrap(errors.WithStack(err)), "")
	}
}

func (st *Storage) Get(key []byte) ([]byte, bool, error) {
	switch b, err := st.db.Get(key, nil); {
	case err == nil:
		return b, true, nil
	case errors.Is(err, leveldb.ErrNotFound):
		return nil, false, nil
	default:
		return b, false, storage.ExecError.Wrap(errors.Wrap(err, "failed to get"))
	}
}

func (st *Storage) Exists(key []byte) (bool, error) {
	switch b, err := st.db.Has(key, nil); {
	case err == nil:
		return b, nil
	default:
		return false, storage.ExecError.Wrap(errors.Wrap(err, "failed to check exists"))
	}
}

func (st *Storage) Iter(
	r *leveldbutil.Range,
	callback func(key []byte, raw []byte) (bool, error),
	sort bool,
) error {
	iter := st.db.NewIterator(r, nil)
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
		switch keep, err := callback(copyBytes(iter.Key()), copyBytes(iter.Value())); {
		case err != nil:
			return err
		case !keep:
			break end
		case !next():
			break end
		}
	}

	if err := iter.Error(); err != nil {
		return storage.ExecError.Errorf("failed to iter")
	}

	return nil
}

func (st *Storage) Put(k, b []byte, opt *leveldbOpt.WriteOptions) error {
	if err := st.db.Put(k, b, opt); err != nil {
		return storage.ExecError.Wrap(errors.Wrap(err, "failed to put"))
	}

	return nil
}

func (st *Storage) Delete(k []byte, opt *leveldbOpt.WriteOptions) error {
	if err := st.db.Delete(k, opt); err != nil {
		return storage.ExecError.Wrap(errors.Wrap(err, "failed to delete"))
	}

	return nil
}

func (st *Storage) Batch(batch *leveldb.Batch, wo *leveldbOpt.WriteOptions) error {
	return errors.WithStack(st.db.Write(batch, wo))
}

func (st *Storage) Clean() error {
	batch := &leveldb.Batch{}
	defer batch.Reset()

	if _, err := BatchRemove(st, nil, 333); err != nil { //nolint:gomnd //...
		return err
	}

	return nil
}

func copyBytes(b []byte) []byte {
	n := make([]byte, len(b))
	copy(n, b)

	return n
}

func BatchRemove(st *Storage, r *leveldbutil.Range, limit int) (int, error) {
	var removed int

	batch := &leveldb.Batch{}
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

		if err := st.Batch(batch, nil); err != nil {
			return removed, err
		}

		removed += batch.Len()

		batch.Reset()
	}

	return removed, nil
}
