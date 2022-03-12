package leveldbstorage

import (
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testLeveldbReadonlyStorage struct {
	suite.Suite
}

func (t *testLeveldbReadonlyStorage) TestNew() {
	wst := NewMemWriteStorage()
	defer wst.Close()

	bs := map[string][]byte{}
	for range make([]int, 33) {
		b := util.UUID()
		bs[b.String()] = b.Bytes()

		t.NoError(wst.Put([]byte(b.String()), b.Bytes(), nil))
	}

	t.NoError(wst.Write())

	rst, err := NewReadonlyStorageFromWrite(wst)
	t.NoError(err)

	defer rst.Close()

	for k := range bs {
		v, found, err := rst.Get([]byte(k))
		t.NoError(err)
		t.True(found)

		t.Equal(bs[k], v)
	}
}

func TestLeveldbReadonlyStorage(t *testing.T) {
	suite.Run(t, new(testLeveldbReadonlyStorage))
}
