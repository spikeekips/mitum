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

var batchOOptions = &leveldbOpt.Options{
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

type BatchStorage struct {
	*BaseStorage
	batch *leveldb.Batch
}

// NewBatchStorage creates new leveldb storage.
func NewBatchStorage(f string) (*BatchStorage, error) {
	e := util.StringErrorFunc("failed batch leveldb storage")

	lst, err := leveldbStorage.OpenFile(filepath.Clean(f), false)
	if err != nil {
		return nil, e(storage.ConnectionError.Wrapf(err, "failed to open leveldb"), "")
	}

	st, err := newBatchStorage(lst, f)
	if err != nil {
		return nil, e(err, "")
	}

	return st, nil
}

func newBatchStorage(st leveldbStorage.Storage, f string) (*BatchStorage, error) {
	bst, err := newBaseStorage(f, st, batchOOptions)
	if err != nil {
		return nil, storage.ConnectionError.Wrapf(err, "failed to open leveldb")
	}

	return &BatchStorage{
		BaseStorage: bst,
		batch:       &leveldb.Batch{},
	}, nil
}

func (st *BatchStorage) Close() error {
	st.Lock()
	defer st.Unlock()

	st.batch.Reset()

	return st.BaseStorage.close()
}

func (st *BatchStorage) Reset() {
	st.Lock()
	defer st.Unlock()

	st.batch.Reset()
}

func (st *BatchStorage) Put(k, b []byte) {
	st.Lock()
	defer st.Unlock()

	st.batch.Put(k, b)
}

func (st *BatchStorage) Delete(k []byte) {
	st.Lock()
	defer st.Unlock()

	st.batch.Delete(k)
}

func (st *BatchStorage) Write() error {
	st.Lock()
	defer st.Unlock()

	if st.batch.Len() < 1 {
		return nil
	}

	if err := st.db.Write(st.batch, &leveldbOpt.WriteOptions{Sync: true}); err != nil {
		return storage.ExecError.Errorf("failed to write in batch stroage")
	}

	st.batch.Reset()

	return nil
}
