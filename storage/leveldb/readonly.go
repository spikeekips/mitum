package leveldbstorage

import (
	"math"
	"path/filepath"

	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
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

	bst, err := newBaseStorage(f, lst, readonlyDBOOptions)
	if err != nil {
		return nil, e(err, "failed to open")
	}

	return &ReadonlyStorage{
		BaseStorage: bst,
	}, nil
}

func NewReadonlyStorageFromWrite(wst *WriteStorage) *ReadonlyStorage {
	return &ReadonlyStorage{
		BaseStorage: wst.BaseStorage,
	}
}
