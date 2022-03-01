package leveldbstorage

import (
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testWriteStorage struct {
	suite.Suite
}

func (t *testWriteStorage) TestNew() {
	d, err := os.MkdirTemp("", "leveldb")
	t.NoError(err)
	defer os.RemoveAll(d)

	wst, err := NewWriteStorage(d)
	t.NoError(err)
	defer wst.Close()

	b, found, err := wst.Get(util.UUID().Bytes())
	t.Nil(b)
	t.False(found)
	t.NoError(err)
}

func (t *testWriteStorage) TestCloseAgain() {
	d, err := os.MkdirTemp("", "leveldb")
	t.NoError(err)
	defer os.RemoveAll(d)

	wst, err := NewWriteStorage(d)
	t.NoError(err)

	t.NoError(wst.Close())
	t.NoError(wst.Close())
}

func (t *testWriteStorage) TestRemove() {
	d, err := os.MkdirTemp("", "leveldb")
	t.NoError(err)

	wst, err := NewWriteStorage(d)
	t.NoError(err)

	t.NoError(wst.Remove())

	_, err = os.Stat(d)
	t.True(os.IsNotExist(err))

	err = wst.Remove()
	t.True(errors.Is(err, storage.ConnectionError))
}

func (t *testWriteStorage) TestPut() {
	wst := NewMemWriteStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.BatchPut([]byte(b.String()), b.Bytes())
	}

	t.NoError(wst.BatchWrite())

	for k := range bs {
		v, found, err := wst.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}
}

func (t *testWriteStorage) TestDelete() {
	wst := NewMemWriteStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.BatchPut([]byte(b.String()), b.Bytes())
	}

	t.NoError(wst.BatchWrite())

	deleted := map[string]struct{}{}

	var i int
	for k := range bs {
		if i == 5 {
			break
		}

		wst.BatchDelete([]byte(k))
		deleted[k] = struct{}{}

		i++
	}

	t.NoError(wst.BatchWrite())

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

func (t *testWriteStorage) TestReset() {
	wst := NewMemWriteStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.BatchPut([]byte(b.String()), b.Bytes())
	}

	t.True(wst.batch.Len() > 0)

	wst.BatchReset()

	t.True(wst.batch.Len() < 1)
}

func (t *testWriteStorage) TestCompaction() {
	wst := NewMemWriteStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.BatchPut([]byte(b.String()), b.Bytes())
	}

	t.NoError(wst.BatchWrite())

	for k := range bs {
		wst.BatchPut([]byte(k), bs[k])
	}
	t.NoError(wst.BatchWrite())

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

func TestWriteStorage(t *testing.T) {
	suite.Run(t, new(testWriteStorage))
}
