package leveldbstorage

import (
	"os"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"github.com/syndtr/goleveldb/leveldb"
)

type testStorage struct {
	suite.Suite
	root string
}

func (t *testStorage) SetupTest() {
	d, err := os.MkdirTemp("", "leveldb")
	t.NoError(err)

	t.root = d
}

func (t *testStorage) TearDownTest() {
	defer os.RemoveAll(t.root)
}

func (t *testStorage) TestNew() {
	st := NewFSStorage(t.root)
	defer st.Close()

	b, found, err := st.Get(util.UUID().Bytes())
	t.Nil(b)
	t.False(found)
	t.NoError(err)
}

func (t *testStorage) TestCloseAgain() {
	st := NewFSStorage(t.root)

	t.NoError(st.Close())
	t.NoError(st.Close())
}

func (t *testStorage) TestPut() {
	st := NewMemStorage()
	defer st.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		t.NoError(st.Put([]byte(b.String()), b.Bytes(), nil))
	}

	for k := range bs {
		v, found, err := st.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}
}

func (t *testStorage) TestDelete() {
	st := NewMemStorage()
	defer st.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		t.NoError(st.Put([]byte(b.String()), b.Bytes(), nil))
	}

	deleted := map[string]struct{}{}

	var i int
	for k := range bs {
		if i == 5 {
			break
		}

		t.NoError(st.Delete([]byte(k), nil))
		deleted[k] = struct{}{}

		i++
	}

	for k := range bs {
		v, found, err := st.Get([]byte(k))

		if _, dfound := deleted[k]; dfound {
			t.False(found)
		} else {
			t.NoError(err)
			t.True(found)

			t.Equal(bs[k], v)
		}
	}
}

func (t *testStorage) TestPutBatch() {
	st := NewMemStorage()
	defer st.Close()

	var batch leveldb.Batch

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		batch.Put([]byte(b.String()), b.Bytes())
	}

	t.NoError(st.Batch(&batch, nil))

	for k := range bs {
		v, found, err := st.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}
}

func (t *testStorage) TestDeleteBatch() {
	st := NewMemStorage()
	defer st.Close()

	var batch leveldb.Batch

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		batch.Put([]byte(b.String()), b.Bytes())
	}

	t.NoError(st.Batch(&batch, nil))

	deleted := map[string]struct{}{}

	batch.Reset()

	var i int
	for k := range bs {
		if i == 5 {
			break
		}

		batch.Delete([]byte(k))
		deleted[k] = struct{}{}

		i++
	}

	t.NoError(st.Batch(&batch, nil))

	for k := range bs {
		v, found, err := st.Get([]byte(k))

		if _, dfound := deleted[k]; dfound {
			t.False(found)
		} else {
			t.NoError(err)
			t.True(found)

			t.Equal(bs[k], v)
		}
	}
}

func (t *testStorage) TestCompaction() {
	st := NewMemStorage()
	defer st.Close()

	var batch leveldb.Batch

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		batch.Put([]byte(b.String()), b.Bytes())
	}

	t.NoError(st.Batch(&batch, nil))

	batch.Reset()

	for k := range bs {
		batch.Put([]byte(k), bs[k])
	}
	t.NoError(st.Batch(&batch, nil))

	for k := range bs {
		v, found, err := st.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}

	var count int
	t.NoError(st.Iter(nil, func([]byte, []byte) (bool, error) {
		count++

		return true, nil
	}, true))

	t.Equal(33, count)
}

func TestStorage(t *testing.T) {
	suite.Run(t, new(testStorage))
}
