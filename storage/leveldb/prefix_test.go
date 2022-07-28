package leveldbstorage

import (
	"bytes"
	"sort"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type testPrefixStorage struct {
	suite.Suite
}

func (t *testPrefixStorage) TestNew() {
	st := NewMemStorage()
	defer st.Close()

	pst := NewPrefixStorage(st, util.UUID().Bytes())

	key := util.UUID().Bytes()
	value := util.UUID().Bytes()

	t.NoError(pst.Put(key, value, nil))

	t.Run("in prefix storage", func() {
		b, found, err := pst.Get(key)
		t.True(found)
		t.NoError(err)
		t.Equal(value, b)

		found, err = pst.Exists(key)
		t.True(found)
		t.NoError(err)
	})

	t.Run("in original storage", func() {
		ok := pst.key(key)
		t.NotNil(ok)

		b, found, err := st.Get(ok)
		t.True(found)
		t.NoError(err)
		t.Equal(value, b)

		found, err = st.Exists(ok)
		t.True(found)
		t.NoError(err)
	})

	t.Run("delete in prefix storage", func() {
		t.NoError(pst.Delete(key, nil))

		found, err := pst.Exists(key)
		t.False(found)
		t.NoError(err)
	})

	t.Run("delete in original storage", func() {
		ok := pst.key(key)
		t.NotNil(ok)

		found, err := st.Exists(ok)
		t.False(found)
		t.NoError(err)
	})
}

func (t *testPrefixStorage) TestClose() {
	st := NewMemStorage()
	defer st.Close()

	pst := NewPrefixStorage(st, util.UUID().Bytes())

	t.NoError(pst.Close())
	t.NoError(pst.Close())

	t.Run("prefix storage after closed", func() {
		found, err := pst.Exists(util.UUID().Bytes())
		t.False(found)
		t.Error(err)
		t.True(errors.Is(err, storage.InternalError))
		t.ErrorContains(err, "already closed")
	})

	t.Run("original storage after closed", func() {
		found, err := st.Exists(util.UUID().Bytes())
		t.False(found)
		t.NoError(err)
	})
}

func (t *testPrefixStorage) TestGet() {
	st := NewMemStorage()
	defer st.Close()

	pst := NewPrefixStorage(st, util.UUID().Bytes())

	key := util.UUID().Bytes()
	value := util.UUID().Bytes()

	t.NoError(pst.Put(key, value, nil))

	t.Run("prefix storage: ok", func() {
		b, found, err := pst.Get(key)
		t.True(found)
		t.NoError(err)
		t.Equal(value, b)

		found, err = pst.Exists(key)
		t.True(found)
		t.NoError(err)
	})

	t.Run("prefix storage: not found", func() {
		b, found, err := pst.Get(util.UUID().Bytes())
		t.False(found)
		t.NoError(err)
		t.Nil(b)

		found, err = pst.Exists(util.UUID().Bytes())
		t.False(found)
		t.NoError(err)
	})
}

func (t *testPrefixStorage) TestIter() {
	st := NewMemStorage()
	defer st.Close()

	prefix := []byte(util.ULID().String())
	pst := NewPrefixStorage(st, prefix)
	apst := NewPrefixStorage(st, []byte(util.ULID().String()))

	keys := make([][]byte, 33)
	for i := range keys {
		keys[i] = util.UUID().Bytes()

		t.NoError(pst.Put(keys[i], util.UUID().Bytes(), nil))
	}

	sort.Slice(keys, func(i, j int) bool { // NOTE put another prefix
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	for range keys {
		t.NoError(apst.Put(util.UUID().Bytes(), util.UUID().Bytes(), nil))
	}

	t.Run("prefix storage: empty start and limit", func() {
		ukeys := make([][]byte, len(keys))

		var i int
		t.NoError(pst.Iter(nil, func(key, _ []byte) (bool, error) {
			ukeys[i] = key
			i++

			return true, nil
		}, true))

		t.Equal(keys, ukeys)
	})

	t.Run("prefix storage: with start", func() {
		expected := keys[10:]
		ukeys := make([][]byte, len(expected))

		r := &leveldbutil.Range{Start: keys[10]}

		var i int
		t.NoError(pst.Iter(r, func(key, _ []byte) (bool, error) {
			ukeys[i] = key
			i++

			return true, nil
		}, true))

		t.Equal(expected, ukeys)
	})

	t.Run("prefix storage: with start and limit", func() {
		expected := keys[10:13]
		ukeys := make([][]byte, len(expected))

		r := &leveldbutil.Range{Start: keys[10], Limit: keys[13]}

		var i int
		t.NoError(pst.Iter(r, func(key, _ []byte) (bool, error) {
			ukeys[i] = key
			i++

			return true, nil
		}, true))

		t.Equal(expected, ukeys)
	})

	t.Run("prefix storage: reverse", func() {
		sort.Slice(keys, func(i, j int) bool { // NOTE put another prefix
			return bytes.Compare(keys[i], keys[j]) > 0
		})

		ukeys := make([][]byte, len(keys))

		var i int
		t.NoError(pst.Iter(nil, func(key, _ []byte) (bool, error) {
			ukeys[i] = key
			i++

			return true, nil
		}, false))

		t.Equal(keys, ukeys)
	})
}

func (t *testPrefixStorage) TestPut() {
	st := NewMemStorage()
	defer st.Close()

	pst := NewPrefixStorage(st, util.UUID().Bytes())

	t.Run("ok", func() {
		key := util.UUID().Bytes()
		value := util.UUID().Bytes()

		t.NoError(pst.Put(key, value, nil))

		b, found, err := pst.Get(key)
		t.True(found)
		t.NoError(err)
		t.Equal(value, b)

		found, err = pst.Exists(key)
		t.True(found)
		t.NoError(err)
	})

	t.Run("nil key", func() {
		err := pst.Put(nil, nil, nil)
		t.Error(err)
		t.True(errors.Is(err, storage.InternalError))
	})
}

func (t *testPrefixStorage) TestDelete() {
	st := NewMemStorage()
	defer st.Close()

	pst := NewPrefixStorage(st, util.UUID().Bytes())

	t.Run("ok", func() {
		key := util.UUID().Bytes()
		value := util.UUID().Bytes()

		t.NoError(pst.Put(key, value, nil))

		found, err := pst.Exists(key)
		t.True(found)
		t.NoError(err)

		t.NoError(pst.Delete(key, nil))

		found, err = pst.Exists(key)
		t.False(found)
		t.NoError(err)
	})

	t.Run("not found", func() {
		key := util.UUID().Bytes()

		found, err := pst.Exists(key)
		t.False(found)
		t.NoError(err)

		t.NoError(pst.Delete(key, nil))
	})
}

func (t *testPrefixStorage) TestBatch() {
	st := NewMemStorage()
	defer st.Close()

	pst := NewPrefixStorage(st, util.UUID().Bytes())

	key := util.UUID().Bytes()
	value := util.UUID().Bytes()

	t.NoError(pst.Put(key, value, nil))

	t.Run("put and delete", func() {
		found, err := pst.Exists(key)
		t.NoError(err)
		t.True(found)

		batch := pst.NewBatch()

		newkey := util.UUID().Bytes()
		batch.Put(newkey, util.UUID().Bytes())
		batch.Delete(key)

		t.NoError(pst.Batch(batch, nil))

		found, err = pst.Exists(newkey)
		t.NoError(err)
		t.True(found)

		found, err = pst.Exists(key)
		t.NoError(err)
		t.False(found)
	})
}

func TestPrefixStorage(t *testing.T) {
	suite.Run(t, new(testPrefixStorage))
}
