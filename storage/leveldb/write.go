package leveldbstorage

import (
	"math"
	"path/filepath"
	"sync"

	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbOpt "github.com/syndtr/goleveldb/leveldb/opt"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
)

var writeOptions = &leveldbOpt.Options{
	Compression:            leveldbOpt.NoCompression,
	CompactionL0Trigger:    math.MaxInt32, // NOTE virtually disable compaction
	WriteL0PauseTrigger:    math.MaxInt32,
	WriteL0SlowdownTrigger: math.MaxInt32,
	ErrorIfExist:           false,
	ErrorIfMissing:         false,
	NoWriteMerge:           true,
	ReadOnly:               false,
	WriteBuffer:            math.MaxInt32,
}

type WriteStorage struct {
	sync.Mutex
	*BaseStorage
	batch *leveldb.Batch
}

// NewWriteStorage creates new leveldb storage.
func NewWriteStorage(f string) (*WriteStorage, error) {
	e := util.StringErrorFunc("failed batch leveldb storage")

	lst, err := leveldbStorage.OpenFile(filepath.Clean(f), false)
	if err != nil {
		return nil, e(storage.ConnectionError.Wrapf(err, "failed to open leveldb"), "")
	}

	st, err := newWriteStorage(lst, f)
	if err != nil {
		return nil, e(err, "")
	}

	return st, nil
}

func newWriteStorage(st leveldbStorage.Storage, f string) (*WriteStorage, error) {
	bst, err := newBaseStorage(f, st, writeOptions)
	if err != nil {
		return nil, storage.ConnectionError.Wrapf(err, "failed to open leveldb")
	}

	return &WriteStorage{
		BaseStorage: bst,
		batch:       &leveldb.Batch{},
	}, nil
}

func (st *WriteStorage) Close() error {
	st.Lock()
	defer st.Unlock()

	st.batch.Reset()

	return st.BaseStorage.Close()
}

func (st *WriteStorage) Remove() error {
	st.Lock()
	defer st.Unlock()

	st.batch.Reset()

	return st.BaseStorage.Remove()
}

func (st *WriteStorage) Put(k, b []byte, opt *leveldbOpt.WriteOptions) error {
	if err := st.db.Put(k, b, opt); err != nil {
		return storage.ExecError.Wrapf(err, "failed to put")
	}

	return nil
}

func (st *WriteStorage) Delete(k []byte, opt *leveldbOpt.WriteOptions) error {
	if err := st.db.Delete(k, opt); err != nil {
		return storage.ExecError.Wrapf(err, "failed to delete")
	}

	return nil
}

func (st *WriteStorage) ResetBatch() {
	st.Lock()
	defer st.Unlock()

	st.batch.Reset()
}

func (st *WriteStorage) PutBatch(k, b []byte) {
	st.Lock()
	defer st.Unlock()

	st.batch.Put(k, b)
}

func (st *WriteStorage) DeleteBatch(k []byte) {
	st.Lock()
	defer st.Unlock()

	st.batch.Delete(k)
}

func (st *WriteStorage) Write() error {
	st.Lock()
	defer st.Unlock()

	if st.batch.Len() > 0 {
		if err := st.db.Write(st.batch, &leveldbOpt.WriteOptions{Sync: true}); err != nil {
			return storage.ExecError.Wrapf(err, "failed to write in batch stroage")
		}
	}

	st.batch.Reset()

	return nil
}
