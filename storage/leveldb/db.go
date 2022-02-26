package leveldbstorage

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbOpt "github.com/syndtr/goleveldb/leveldb/opt"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type BaseStorage struct {
	sync.Mutex
	f  string
	db *leveldb.DB
}

func newBaseStorage(f string, st leveldbStorage.Storage, opt *leveldbOpt.Options) (*BaseStorage, error) {
	db, err := leveldb.Open(st, wrtieDBOOptions)
	if err != nil {
		return nil, storage.ConnectionError.Wrapf(err, "failed to open leveldb")
	}

	return &BaseStorage{f: f, db: db}, nil
}

func (st *BaseStorage) Close() error {
	st.Lock()
	defer st.Unlock()

	return st.close()
}

func (st *BaseStorage) Remove() error {
	st.Lock()
	defer st.Unlock()

	e := util.StringErrorFunc("failed to remove leveldb")

	if err := st.close(); err != nil {
		return e(storage.ConnectionError.Wrap(err), "")
	}

	if len(st.f) > 0 {
		switch fi, err := os.Stat(st.f); {
		case err == nil && !fi.IsDir():
			return e(storage.ConnectionError.Errorf("not directory"), "")
		case os.IsNotExist(err):
		case err != nil:
			return e(storage.ConnectionError.Wrap(err), "")
		default:
			if err := os.RemoveAll(filepath.Clean(st.f)); err != nil {
				return e(err, "failed to remove files of leveldb")
			}
		}
	}

	return nil
}

func (st *BaseStorage) close() error {
	switch err := st.db.Close(); {
	case err == nil:
		return nil
	case errors.Is(err, leveldb.ErrClosed):
		return nil
	default:
		return storage.ConnectionError.Wrapf(err, "failed to close leveldb")
	}
}

func (st *BaseStorage) Get(key []byte) ([]byte, bool, error) {
	switch b, err := st.db.Get(key, nil); {
	case err == nil:
		return b, true, nil
	case errors.Is(err, leveldb.ErrNotFound):
		return nil, false, nil
	default:
		return b, false, storage.ExecError.Errorf("failed to get")
	}
}

func (st *BaseStorage) Iter(
	prefix []byte,
	callback func(key []byte, raw []byte) (bool, error),
	sort bool,
) error {
	iter := st.db.NewIterator(leveldbutil.BytesPrefix(prefix), nil)
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
			return errors.Wrap(err, "")
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
