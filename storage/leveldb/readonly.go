package leveldbstorage

import (
	"math"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbOpt "github.com/syndtr/goleveldb/leveldb/opt"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
)

var readonlyDBOOptions = &leveldbOpt.Options{
	Compression:            leveldbOpt.NoCompression,
	CompactionL0Trigger:    math.MaxInt,
	WriteL0PauseTrigger:    math.MaxInt,
	WriteL0SlowdownTrigger: math.MaxInt,
	ErrorIfExist:           false,
	ErrorIfMissing:         true,
	NoWriteMerge:           true,
	ReadOnly:               true,
}

type ReadonlyStorage struct {
	*BaseStorage
}

// ReadonlyStorage load leveldb storage as readonly.
func NewReadonlyStorage(f string) (*ReadonlyStorage, error) {
	e := util.StringErrorFunc("failed readonly leveldb storage")

	lst, err := leveldbStorage.OpenFile(filepath.Clean(f), true)
	if err != nil {
		return nil, e(storage.ConnectionError.Wrapf(err, "failed to open leveldb"), "")
	}

	st, err := newReadonlyStorage(lst, f)
	if err != nil {
		return nil, e(err, "")
	}

	return st, nil
}

func newReadonlyStorage(st leveldbStorage.Storage, f string) (*ReadonlyStorage, error) {
	bst, err := newBaseStorage(f, st, readonlyDBOOptions)
	if err != nil {
		return nil, storage.ConnectionError.Wrapf(err, "failed to open leveldb")
	}

	return &ReadonlyStorage{
		BaseStorage: bst,
	}, nil
}

func NewReadonlyStorageFromWrite(wst *WriteStorage) (*ReadonlyStorage, error) {
	if err := wst.db.SetReadOnly(); err != nil {
		if !errors.Is(err, leveldb.ErrReadOnly) {
			return nil, errors.Wrap(err, "failed to set readonly to BatchStorage")
		}
	}

	return &ReadonlyStorage{
		BaseStorage: wst.BaseStorage,
	}, nil
}
