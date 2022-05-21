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
	p.Set("findme", "showme")

	i, found := p.Get("findme")
	t.True(found)
	t.NotNil(i)
	t.Equal("showme", i)
}

func TestGCacheObjectPool(t *testing.T) {
	suite.Run(t, new(testGCacheObjectPool))
}

type testLockedObjectPool struct {
	suite.Suite
}

func (t *testLockedObjectPool) TestNew() {
	p := NewLockedObjectPool()

	_ = (interface{})(p).(ObjectPool)
}

func (t *testLockedObjectPool) TestGetNotFound() {
	p := NewLockedObjectPool()
	i, found := p.Get("findme")
	t.False(found)
	t.Nil(i)
}

func (t *testLockedObjectPool) TestGet() {
	p := NewLockedObjectPool()
	p.Set("findme", "showme")

	i, found := p.Get("findme")
	t.True(found)
	t.NotNil(i)
	t.Equal("showme", i)
}

func TestLockedObjectPool(t *testing.T) {
	suite.Run(t, new(testLockedObjectPool))
}
