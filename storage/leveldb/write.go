package leveldbstorage

import (
	"math"
	"path/filepath"

	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbOpt "github.com/syndtr/goleveldb/leveldb/opt"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
)

var wrtieDBOOptions = &leveldbOpt.Options{
	Compression:            leveldbOpt.NoCompression,
	CompactionL0Trigger:    math.MaxInt32, // NOTE virtually disable compaction
	WriteL0PauseTrigger:    math.MaxInt32,
	WriteL0SlowdownTrigger: math.MaxInt32,
	ErrorIfExist:           true,
	ErrorIfMissing:         false,
	NoWriteMerge:           true,
	ReadOnly:               false,
	WriteBuffer:            math.MaxInt32,
}

type WriteStorage struct {
	*BaseStorage
	batch *leveldb.Batch
}

// NewWriteStorage creates new leveldb storage.
func NewWriteStorage(f string) (*WriteStorage, error) {
	e := util.StringErrorFunc("failed write leveldb storage")

	st, err := leveldbStorage.OpenFile(filepath.Clean(f), false)
	if err != nil {
		return nil, e(storage.ConnectionError.Wrapf(err, "failed to open leveldb"), "")
	}

	return newWriteStorage(st, f)
}

func newWriteStorage(st leveldbStorage.Storage, f string) (*WriteStorage, error) {
	bst, err := newBaseStorage(f, st, wrtieDBOOptions)
	if err != nil {
		return nil, err
	}

	return &WriteStorage{
		BaseStorage: bst,
		batch:       &leveldb.Batch{},
	}, nil
}

func (st *WriteStorage) Reset() {
	st.batch.Reset()
}

func (st *WriteStorage) Put(k, b []byte) {
	st.batch.Put(k, b)
}

func (st *WriteStorage) Delete(k []byte) {
	st.batch.Delete(k)
}

func (st *WriteStorage) Write() error {
	if st.batch.Len() < 1 {
		return nil
	}

	if err := st.db.Write(st.batch, &leveldbOpt.WriteOptions{Sync: true}); err != nil {
		return storage.ExecError.Errorf("failed to write in write stroage")
	}

	return nil
}
