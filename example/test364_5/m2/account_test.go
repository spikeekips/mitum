package m2

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testAccountSVM struct {
	suite.Suite
}

func (t *testAccountSVM) TestNew() {
	t.Run("nil prev", func() {
		key := util.UUID().String()
		m := newAccountSVM(key, nil)

		t.Equal(key, m.Key())
		t.EqualValues(0, m.sum)
		t.EqualValues(0, m.prev)
	})

	t.Run("non-nil prev", func() {
		key := util.UUID().String()
		prev := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 33)

		m := newAccountSVM(key, prev)

		t.Equal(key, m.Key())
		t.True(prev.Address.Equal(m.address))
		t.EqualValues(prev.Balance, m.sum)
		t.EqualValues(prev.Balance, m.prev)
	})
}

func (t *testAccountSVM) TestClose() {
	t.Run("sum not changed", func() {
		prev := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 33)
		m := newAccountSVM(util.UUID().String(), prev)
		v, err := m.Close()
		t.NoError(err)
		t.Nil(v)
	})

	t.Run("non-zero sum not changed", func() {
		prev := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 33)
		m := newAccountSVM(util.UUID().String(), prev)
		v, err := m.Close()
		t.NoError(err)
		t.Nil(v)
	})

	t.Run("sum under zero", func() {
		prev := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 0)
		m := newAccountSVM(util.UUID().String(), prev)
		m.sum -= 1
		_, err := m.Close()
		t.ErrorContains(err, "under zero balance")
	})

	t.Run("no address", func() {
		m := newAccountSVM(util.UUID().String(), nil)
		m.sum += 1

		_, err := m.Close()
		t.ErrorContains(err, "no address")
	})

	t.Run("nil prev", func() {
		m := newAccountSVM(util.UUID().String(), nil)
		m.sum += 1
		m.address = base.NewDummyPublickey(util.UUID().String())

		v, err := m.Close()
		t.NoError(err)

		ac := v.(*AccountSV)

		t.True(m.address.Equal(ac.Address))
		t.EqualValues(m.sum, ac.Balance)
	})

	t.Run("non-nil prev", func() {
		prev := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 33)

		m := newAccountSVM(util.UUID().String(), prev)
		m.sum += 1

		v, err := m.Close()
		t.NoError(err)

		ac := v.(*AccountSV)

		t.True(m.address.Equal(ac.Address))
		t.EqualValues(prev.Balance+1, ac.Balance)
		t.EqualValues(m.sum, ac.Balance)
	})
}

func (t *testAccountSVM) TestSubMerge() {
	t.Run("zero balance", func() {
		m := newAccountSVM(util.UUID().String(), nil)

		v := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 0)

		var opindex uint64 = 0
		next, reason, err := m.SubMerge(opindex, v)
		t.ErrorContains(err, "zero balance")
		t.Equal(opindex, next)
		t.Empty(reason)
	})

	t.Run("not sub value balance", func() {
		m := newAccountSVM(util.UUID().String(), nil)

		v := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 1)

		var opindex uint64 = 0
		next, reason, err := m.SubMerge(opindex, v)
		t.ErrorContains(err, "not sub value")
		t.Equal(opindex, next)
		t.Empty(reason)
	})

	t.Run("nil prev", func() {
		m := newAccountSVM(util.UUID().String(), nil)

		v := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), -33)

		var opindex uint64 = 0
		next, reason, err := m.SubMerge(opindex, v)
		t.NoError(err)
		t.Equal(opindex, next)
		t.Equal("account not exists", reason)
	})

	t.Run("insufficient", func() {
		address := base.NewDummyPublickey(util.UUID().String())
		prev := NewAccountSV(address, 33)
		m := newAccountSVM(util.UUID().String(), prev)

		v := NewAccountSV(address, -34)

		var opindex uint64 = 0
		next, reason, err := m.SubMerge(opindex, v)
		t.NoError(err)
		t.Equal(opindex, next)
		t.Equal("insufficient balance", reason)
	})

	t.Run("merged", func() {
		address := base.NewDummyPublickey(util.UUID().String())
		prev := NewAccountSV(address, 33)
		m := newAccountSVM(util.UUID().String(), prev)

		v := NewAccountSV(address, -1)

		var opindex uint64 = 0
		next, reason, err := m.SubMerge(opindex, v)
		t.NoError(err)
		t.Equal(opindex, next)
		t.Empty(reason)

		t.Equal(prev.Balance+v.Balance, m.sum)
		t.Equal(&opindex, m.mergedSub)
		t.Equal(v.Balance, m.mergedSubValue)
	})

	t.Run("higher opindex", func() {
		address := base.NewDummyPublickey(util.UUID().String())
		prev := NewAccountSV(address, 33)
		m := newAccountSVM(util.UUID().String(), prev)

		v := NewAccountSV(address, -1)

		var opindex uint64 = 0
		next, reason, err := m.SubMerge(opindex, v)
		t.NoError(err)
		t.Equal(opindex, next)
		t.Empty(reason)

		// higher opindex
		next, reason, err = m.SubMerge(opindex+1, v)
		t.NoError(err)
		t.Equal(opindex+1, next)
		t.Equal("previous issuer found", reason)
		t.Equal([]uint64{opindex + 1}, m.nextSubs)
		t.Equal(prev.Balance+v.Balance, m.sum)
	})

	t.Run("opindex in next subs", func() {
		address := base.NewDummyPublickey(util.UUID().String())
		prev := NewAccountSV(address, 33)
		m := newAccountSVM(util.UUID().String(), prev)

		v := NewAccountSV(address, -1)

		var opindex uint64 = 0
		next, reason, err := m.SubMerge(opindex, v)
		t.NoError(err)
		t.Equal(opindex, next)
		t.Empty(reason)

		// higher opindex
		next, reason, err = m.SubMerge(opindex+1, v)
		t.NoError(err)
		t.Equal(opindex+1, next)
		t.Equal("previous issuer found", reason)
		t.Equal([]uint64{opindex + 1}, m.nextSubs)
		t.Equal(prev.Balance+v.Balance, m.sum)

		// higher opindex sub merge
		_, _, err = m.SubMerge(opindex+1, v)
		t.ErrorContains(err, "already sub merged")
		t.ErrorContains(err, "in nexts")
		t.Equal(prev.Balance+v.Balance, m.sum)
	})

	t.Run("opindex is merged", func() {
		address := base.NewDummyPublickey(util.UUID().String())
		prev := NewAccountSV(address, 33)
		m := newAccountSVM(util.UUID().String(), prev)

		v := NewAccountSV(address, -1)

		var opindex uint64 = 0
		next, reason, err := m.SubMerge(opindex, v)
		t.NoError(err)
		t.Equal(opindex, next)
		t.Empty(reason)
		t.Equal(prev.Balance+v.Balance, m.sum)

		// same opindex
		next, reason, err = m.SubMerge(opindex, v)
		t.ErrorContains(err, "already sub merged")

		t.Equal(prev.Balance+v.Balance, m.sum)
	})

	t.Run("lower opindex", func() {
		address := base.NewDummyPublickey(util.UUID().String())
		prev := NewAccountSV(address, 33)
		m := newAccountSVM(util.UUID().String(), prev)

		v := NewAccountSV(address, -1)

		var opindex uint64 = 1
		next, reason, err := m.SubMerge(opindex, v)
		t.NoError(err)
		t.Equal(opindex, next)
		t.Empty(reason)
		t.Equal(prev.Balance+v.Balance, m.sum)
		t.Equal(&opindex, m.mergedSub)
		t.Equal(v.Balance, m.mergedSubValue)

		// lower opindex
		nextopindex := opindex - 1
		next, reason, err = m.SubMerge(nextopindex, v)
		t.NoError(err)
		t.Equal(opindex, next)
		t.Equal("previous issuer found", reason)
		t.Equal(prev.Balance+v.Balance, m.sum)
		t.Equal([]uint64{opindex}, m.nextSubs)
		t.Equal(&nextopindex, m.mergedSub)
		t.Equal(v.Balance, m.mergedSubValue)
	})
}

func (t *testAccountSVM) TestAddMerge() {
	t.Run("zero balance", func() {
		m := newAccountSVM(util.UUID().String(), nil)

		v := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 0)

		err := m.AddMerge(0, v)
		t.ErrorContains(err, "zero balance")
	})

	t.Run("sub value", func() {
		m := newAccountSVM(util.UUID().String(), nil)

		v := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), -1)

		err := m.AddMerge(0, v)
		t.ErrorContains(err, "not add value")
	})

	t.Run("nil prev", func() {
		m := newAccountSVM(util.UUID().String(), nil)

		v := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 1)

		t.NoError(m.AddMerge(0, v))
		t.Equal(v.Balance, m.sum)
		t.True(v.Address.Equal(m.address))
	})

	t.Run("merge + merge", func() {
		m := newAccountSVM(util.UUID().String(), nil)

		v := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 1)

		t.NoError(m.AddMerge(1, v))
		t.NoError(m.AddMerge(0, v))

		t.Equal(v.Balance+v.Balance, m.sum)
		t.True(v.Address.Equal(m.address))
	})
}

func (t *testAccountSVM) TestUnmerge() {
	t.Run("zero balance", func() {
		m := newAccountSVM(util.UUID().String(), nil)

		v := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 0)

		err := m.Unmerge(0, v)
		t.ErrorContains(err, "zero balance")
	})

	t.Run("sub value", func() {
		m := newAccountSVM(util.UUID().String(), nil)

		v := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), -1)

		err := m.Unmerge(0, v)
		t.ErrorContains(err, "not add value")
	})

	t.Run("unmerge", func() {
		m := newAccountSVM(util.UUID().String(), nil)

		v := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 1)

		t.NoError(m.Unmerge(0, v))
		t.Equal(v.Balance*-1, m.sum)
		t.True(v.Address.Equal(m.address))
	})

	t.Run("unmerge + unmerge", func() {
		m := newAccountSVM(util.UUID().String(), nil)

		v := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 1)

		t.NoError(m.Unmerge(1, v))
		t.NoError(m.Unmerge(0, v))

		t.Equal((v.Balance+v.Balance)*-1, m.sum)
		t.True(v.Address.Equal(m.address))
	})
}

func TestAccountSVM(t *testing.T) {
	suite.Run(t, new(testAccountSVM))
}
