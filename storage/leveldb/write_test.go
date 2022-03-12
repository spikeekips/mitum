package leveldbstorage

import (
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testLeveldbWriteStorage struct {
	suite.Suite
}

func (t *testLeveldbWriteStorage) TestNew() {
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

func (t *testLeveldbWriteStorage) TestCloseAgain() {
	d, err := os.MkdirTemp("", "leveldb")
	t.NoError(err)
	defer os.RemoveAll(d)

	wst, err := NewWriteStorage(d)
	t.NoError(err)

	t.NoError(wst.Close())
	t.NoError(wst.Close())
}

func (t *testLeveldbWriteStorage) TestRemove() {
	d, err := os.MkdirTemp("", "leveldb")
	t.NoError(err)

	wst, err := NewWriteStorage(d)
	t.NoError(err)

	t.NoError(wst.Remove())

	_, err = os.Stat(d)
	t.True(os.IsNotExist(err))

	err = wst.Remove()
	t.True(errors.Is(err, storage.InternalError))
}

func (t *testLeveldbWriteStorage) TestPut() {
	wst := NewMemWriteStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		t.NoError(wst.Put([]byte(b.String()), b.Bytes(), nil))
	}

	for k := range bs {
		v, found, err := wst.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}
}

func (t *testLeveldbWriteStorage) TestDelete() {
	wst := NewMemWriteStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		t.NoError(wst.Put([]byte(b.String()), b.Bytes(), nil))
	}

	t.NoError(wst.Write())

	deleted := map[string]struct{}{}

	var i int
	for k := range bs {
		if i == 5 {
			break
		}

		t.NoError(wst.Delete([]byte(k), nil))
		deleted[k] = struct{}{}

		i++
	}

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

func (t *testLeveldbWriteStorage) TestPutBatch() {
	wst := NewMemWriteStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.PutBatch([]byte(b.String()), b.Bytes())
	}

	t.NoError(wst.Write())

	for k := range bs {
		v, found, err := wst.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}
}

func (t *testLeveldbWriteStorage) TestDeleteBatch() {
	wst := NewMemWriteStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.PutBatch([]byte(b.String()), b.Bytes())
	}

	t.NoError(wst.Write())

	deleted := map[string]struct{}{}

	var i int
	for k := range bs {
		if i == 5 {
			break
		}

		wst.DeleteBatch([]byte(k))
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

func (t *testLeveldbWriteStorage) TestResetBatch() {
	wst := NewMemWriteStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.PutBatch([]byte(b.String()), b.Bytes())
	}

	t.True(wst.batch.Len() > 0)

	wst.ResetBatch()

	t.True(wst.batch.Len() < 1)
}

func (t *testLeveldbWriteStorage) TestCompaction() {
	wst := NewMemWriteStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		wst.PutBatch([]byte(b.String()), b.Bytes())
	}

	t.NoError(wst.Write())

	for k := range bs {
		wst.PutBatch([]byte(k), bs[k])
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

func TestLeveldbWriteStorage(t *testing.T) {
	suite.Run(t, new(testLeveldbWriteStorage))
}
