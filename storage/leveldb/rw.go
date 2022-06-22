package leveldbstorage

import (
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbOpt "github.com/syndtr/goleveldb/leveldb/opt"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
)

var rwDBOOptions = &leveldbOpt.Options{
	ErrorIfExist:   false,
	ErrorIfMissing: false,
	// WriteBuffer:    math.MaxInt32,
}

type RWStorage struct {
	*BaseStorage
}

// NewRWStorage opens read-write leveldb storage.
func NewRWStorage(f string) (*RWStorage, error) {
	e := util.StringErrorFunc("failed rw leveldb storage")

	lst, err := leveldbStorage.OpenFile(filepath.Clean(f), false)
	if err != nil {
		return nil, e(storage.ConnectionError.Wrapf(err, "failed to open leveldb"), "")
	}

	st, err := newRWStorage(lst, f)
	if err != nil {
		return nil, e(err, "")
	}

	return st, nil
}

func newRWStorage(st leveldbStorage.Storage, f string) (*RWStorage, error) {
	bst, err := newBaseStorage(f, st, rwDBOOptions)
	if err != nil {
		return nil, storage.ConnectionError.Wrapf(err, "failed to open leveldb")
	}

	return &RWStorage{
		BaseStorage: bst,
	}, nil
}

func (st *RWStorage) Put(k, b []byte, opt *leveldbOpt.WriteOptions) error {
	return errors.Wrap(st.db.Put(k, b, opt), "")
}

func (st *RWStorage) Delete(k []byte, opt *leveldbOpt.WriteOptions) error {
	return errors.Wrap(st.db.Delete(k, opt), "")
}

func (st *RWStorage) Write(batch *leveldb.Batch, opt *leveldbOpt.WriteOptions) error {
	return errors.Wrap(st.db.Write(batch, opt), "")
}
