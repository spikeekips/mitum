package util

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type testGCacheObjectPool struct {
	suite.Suite
}

func (t *testGCacheObjectPool) TestNew() {
	p := NewGCacheObjectPool(10)

	_ = (interface{})(p).(ObjectPool)
}

func (t *testGCacheObjectPool) TestGetNotFound() {
	p := NewGCacheObjectPool(10)
	i, found := p.Get("findme")
	t.False(found)
	t.Nil(i)
}

func (t *testGCacheObjectPool) TestGet() {
	p := NewGCacheObjectPool(10)
	p.Set("findme", "showme", nil)

	i, found := p.Get("findme")
	t.True(found)
	t.NotNil(i)
	t.Equal("showme", i)
}

func TestGCacheObjectPool(t *testing.T) {
	suite.Run(t, new(testGCacheObjectPool))
}
