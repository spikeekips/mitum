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

type ReadStorage interface {
	Close() error
	Remove() error
	Get(key []byte) ([]byte, bool, error)
	Exists(key []byte) (bool, error)
	Iter(
		r *leveldbutil.Range,
		callback func(key []byte, raw []byte) (bool, error),
		sort bool,
	) error
}

type BaseStorage struct {
	sync.Mutex
	f   string
	db  *leveldb.DB
	str leveldbStorage.Storage
}

func newBaseStorage(f string, str leveldbStorage.Storage, opt *leveldbOpt.Options) (*BaseStorage, error) {
	db, err := leveldb.Open(str, opt)
	if err != nil {
		return nil, storage.ConnectionError.Wrapf(err, "failed to open leveldb")
	}

	return &BaseStorage{f: f, db: db, str: str}, nil
}

func (st *BaseStorage) Root() string {
	return st.f
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
		return e(storage.InternalError.Wrap(err), "")
	}

	if len(st.f) > 0 {
		switch fi, err := os.Stat(st.f); {
		case err == nil && !fi.IsDir():
			return e(storage.InternalError.Errorf("not directory"), "")
		case os.IsNotExist(err):
			return e(storage.InternalError.Wrap(err), "")
		case err != nil:
			return e(storage.InternalError.Wrap(err), "")
		default:
			if err := os.RemoveAll(filepath.Clean(st.f)); err != nil {
				return e(err, "failed to remove files of leveldb")
			}
		}
	}

	return nil
}

func (st *BaseStorage) close() error {
	e := util.StringErrorFunc("failed to close leveldb")
	switch err := st.str.Close(); {
	case err == nil:
	case errors.Is(err, leveldbStorage.ErrClosed):
		return nil
	default:
		return e(storage.InternalError.Wrapf(err, ""), "failed to close storage")
	}

	switch err := st.db.Close(); {
	case err == nil:
		return nil
	case errors.Is(err, leveldb.ErrClosed):
		return nil
	default:
		return e(storage.InternalError.Wrapf(err, ""), "")
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

func (st *BaseStorage) Exists(key []byte) (bool, error) {
	switch b, err := st.db.Has(key, nil); {
	case err == nil:
		return b, nil
	default:
		return false, storage.ExecError.Errorf("failed to check exists")
	}
}

func (st *BaseStorage) Iter(
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
