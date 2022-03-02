package leveldbstorage

import (
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testBatchStorage struct {
	suite.Suite
}

func (t *testBatchStorage) TestNew() {
	d, err := os.MkdirTemp("", "leveldb")
	t.NoError(err)
	defer os.RemoveAll(d)

	wst, err := NewBatchStorage(d)
	t.NoError(err)
	defer wst.Close()

	b, found, err := wst.Get(util.UUID().Bytes())
	t.Nil(b)
	t.False(found)
	t.NoError(err)
}

func (t *testBatchStorage) TestCloseAgain() {
	d, err := os.MkdirTemp("", "leveldb")
	t.NoError(err)
	defer os.RemoveAll(d)

	wst, err := NewBatchStorage(d)
	t.NoError(err)

	t.NoError(wst.Close())
	t.NoError(wst.Close())
}

func (t *testBatchStorage) TestRemove() {
	d, err := os.MkdirTemp("", "leveldb")
	t.NoError(err)

	wst, err := NewBatchStorage(d)
	t.NoError(err)

	t.NoError(wst.Remove())

	_, err = os.Stat(d)
	t.True(os.IsNotExist(err))

	err = wst.Remove()
	t.True(errors.Is(err, storage.ConnectionError))
}

func (t *testBatchStorage) TestPut() {
	wst := NewMemBatchStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.Put([]byte(b.String()), b.Bytes())
	}

	t.NoError(wst.Write())

	for k := range bs {
		v, found, err := wst.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}
}

func (t *testBatchStorage) TestDelete() {
	wst := NewMemBatchStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.Put([]byte(b.String()), b.Bytes())
	}

	t.NoError(wst.Write())

	deleted := map[string]struct{}{}

	var i int
	for k := range bs {
		if i == 5 {
			break
		}

		wst.Delete([]byte(k))
		deleted[k] = struct{}{}

		i++
	}

	t.NoError(wst.Write())

	for k := range bs {
		v, found, err := wst.Get([]byte(k))

		if _, dfound := deleted[k]; dfound {
			t.False(found)
		} else {
			t.NoError(err)
			t.True(found)

			t.Equal(bs[k], v)
		}
	}
}

func (t *testBatchStorage) TestReset() {
	wst := NewMemBatchStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.Put([]byte(b.String()), b.Bytes())
	}

	t.True(wst.batch.Len() > 0)

	wst.Reset()

	t.True(wst.batch.Len() < 1)
}

func (t *testBatchStorage) TestCompaction() {
	wst := NewMemBatchStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.Put([]byte(b.String()), b.Bytes())
	}

	t.NoError(wst.Write())

	for k := range bs {
		wst.Put([]byte(k), bs[k])
	}
	t.NoError(wst.Write())

	for k := range bs {
		v, found, err := wst.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}

	var count int
	t.NoError(wst.Iter(nil, func([]byte, []byte) (bool, error) {
		count++

		return true, nil
	}, true))

	t.Equal(33, count)
}

func TestBatchStorage(t *testing.T) {
	suite.Run(t, new(testBatchStorage))
}
