package leveldbstorage

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbOpt "github.com/syndtr/goleveldb/leveldb/opt"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type KeyPrefix [2]byte

func NewPrefixKey(prefix KeyPrefix, a ...[]byte) []byte {
	var sl [][]byte

	switch {
	case len(a) < 1:
		sl = [][]byte{prefix[:]}
	default:
		sl = make([][]byte, len(a)+1)
		sl[0] = prefix[:]

		copy(sl[1:], a)
	}

	return util.ConcatBytesSlice(sl...)
}

type PrefixStorage struct {
	sync.RWMutex
	*Storage
	prefix    []byte
	prefixlen int
}

func NewPrefixStorage(st *Storage, prefix []byte) *PrefixStorage {
	return &PrefixStorage{
		Storage:   st,
		prefix:    prefix,
		prefixlen: len(prefix),
	}
}

func (st *PrefixStorage) RawStorage() *Storage {
	return st.Storage
}

func (st *PrefixStorage) Prefix() []byte {
	return st.prefix
}

func (st *PrefixStorage) Close() error {
	st.Lock()
	defer st.Unlock()

	st.prefix = nil
	st.prefixlen = 0

	return nil
}

func (st *PrefixStorage) Remove() error {
	st.Lock()
	defer st.Unlock()

	return RemoveByPrefix(st.Storage, st.prefix)
}

func (st *PrefixStorage) Get(key []byte) ([]byte, bool, error) {
	k := st.key(key)
	if k == nil {
		return nil, false, storage.ErrClosed.WithStack()
	}

	return st.Storage.Get(k)
}

func (st *PrefixStorage) Exists(key []byte) (bool, error) {
	k := st.key(key)
	if k == nil {
		return false, storage.ErrClosed.WithStack()
	}

	return st.Storage.Exists(k)
}

func (st *PrefixStorage) Iter(
	r *leveldbutil.Range,
	callback func([]byte, []byte) (bool, error),
	sort bool,
) error {
	nr := leveldbutil.BytesPrefix(st.prefix)

	if r != nil {
		if r.Start != nil {
			start := st.key(r.Start)
			if start == nil {
				return storage.ErrClosed.WithStack()
			}

			nr.Start = start
		}

		if r.Limit != nil {
			limit := st.key(r.Limit)
			if limit == nil {
				return storage.ErrClosed.WithStack()
			}

			nr.Limit = limit
		}
	}

	return st.Storage.Iter(
		nr,
		func(key, b []byte) (bool, error) {
			ok, err := st.origkey(key)
			if err != nil {
				return false, err
			}

			return callback(ok, b)
		},
		sort,
	)
}

func (st *PrefixStorage) Put(key, b []byte, opt *leveldbOpt.WriteOptions) error {
	k := st.key(key)
	if k == nil {
		return storage.ErrClosed.WithStack()
	}

	return st.Storage.Put(st.key(key), b, opt)
}

func (st *PrefixStorage) Delete(key []byte, opt *leveldbOpt.WriteOptions) error {
	k := st.key(key)
	if k == nil {
		return storage.ErrClosed.WithStack()
	}

	return st.Storage.Delete(st.key(key), opt)
}

func (st *PrefixStorage) NewBatch() *PrefixStorageBatch {
	return newPrefixStorageBatch(st.prefix)
}

func (st *PrefixStorage) Batch(batch *PrefixStorageBatch, opt *leveldbOpt.WriteOptions) error {
	if k := st.key(util.UUID().Bytes()); k == nil {
		return storage.ErrClosed.WithStack()
	}

	return st.Storage.Batch(batch.Batch, opt)
}

func (st *PrefixStorage) key(b []byte) []byte {
	st.RLock()
	defer st.RUnlock()

	switch {
	case st.prefix == nil:
		return nil
	case len(b) < 1:
		return nil
	}

	return util.ConcatBytesSlice(st.prefix, b)
}

func (st *PrefixStorage) origkey(b []byte) ([]byte, error) {
	st.RLock()
	defer st.RUnlock()

	switch {
	case len(b) < 1:
		return nil, nil
	case len(b) < st.prefixlen:
		return nil, errors.Errorf("get original key; wrong size key")
	}

	return b[st.prefixlen:], nil
}

type PrefixStorageBatch struct {
	*leveldb.Batch
	prefix []byte
}

func newPrefixStorageBatch(prefix []byte) *PrefixStorageBatch {
	return &PrefixStorageBatch{
		Batch:  &leveldb.Batch{},
		prefix: prefix,
	}
}

func (b *PrefixStorageBatch) Put(key, i []byte) {
	b.Batch.Put(util.ConcatBytesSlice(b.prefix, key), i)
}

func (b *PrefixStorageBatch) Delete(key []byte) {
	b.Batch.Delete(util.ConcatBytesSlice(b.prefix, key))
}

func RemoveByPrefix(st *Storage, prefix []byte) error {
	batch := &leveldb.Batch{}

	if err := st.Iter(
		leveldbutil.BytesPrefix(prefix),
		func(key, _ []byte) (bool, error) {
			batch.Delete(key)

			return true, nil
		},
		true,
	); err != nil {
		return errors.Errorf("remove prefix storage")
	}

	return st.Batch(batch, nil)
}
