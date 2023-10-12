package isaac

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testLastVoteproofsHandler struct {
	BaseTestBallots
}

func (t *testLastVoteproofsHandler) newINITVoteproof(point base.Point, isMajority bool) base.INITVoteproof {
	ifact := t.NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
	ivp, err := t.NewINITVoteproof(ifact, t.Local, nil)
	t.NoError(err)

	if !isMajority {
		ivp.SetResult(base.VoteResultDraw).Finish()
	}

	return ivp
}

func (t *testLastVoteproofsHandler) newACCEPTVoteproof(point base.Point, isMajority bool) base.ACCEPTVoteproof {
	afact := t.NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
	avp, err := t.NewACCEPTVoteproof(afact, t.Local, nil)
	t.NoError(err)

	if !isMajority {
		avp.SetResult(base.VoteResultDraw).Finish()
	}

	return avp
}

func (t *testLastVoteproofsHandler) TestSet() {
	t.Run("set in empty", func() {
		point := base.RawPoint(33, 0)

		h := NewLastVoteproofsHandler()
		ivp := t.newINITVoteproof(point, true)
		t.True(h.Set(ivp))

		lvps := h.Last()
		EqualVoteproof(t.Assert(), ivp, lvps.Cap())
		EqualVoteproof(t.Assert(), ivp, lvps.INIT())

		_, found := h.Voteproofs(ivp.Point())
		t.False(found)
	})

	t.Run("set old", func() {
		point := base.RawPoint(33, 0)

		h := NewLastVoteproofsHandler()
		ivp := t.newINITVoteproof(point, true)
		t.True(h.Set(ivp))

		t.False(h.Set(t.newINITVoteproof(point.PrevHeight(), true)))

		lvps := h.Last()
		EqualVoteproof(t.Assert(), ivp, lvps.Cap())
	})

	t.Run("set new, but not majority", func() {
		point := base.RawPoint(33, 0)

		h := NewLastVoteproofsHandler()
		ivp := t.newINITVoteproof(point, true)
		t.True(h.Set(ivp))

		avp := t.newACCEPTVoteproof(point, false)
		t.True(h.Set(avp))

		lvps := h.Last()
		EqualVoteproof(t.Assert(), avp, lvps.Cap())
		EqualVoteproof(t.Assert(), ivp, lvps.Majority())
	})

	t.Run("set init, accpet", func() {
		point := base.RawPoint(33, 0)

		h := NewLastVoteproofsHandler()

		ivp := t.newINITVoteproof(point, true)
		t.True(h.Set(ivp))

		avp := t.newACCEPTVoteproof(point, true)
		t.True(h.Set(avp))

		lvps := h.Last()
		EqualVoteproof(t.Assert(), avp, lvps.Cap())
		EqualVoteproof(t.Assert(), ivp, lvps.INIT())
		EqualVoteproof(t.Assert(), avp, lvps.ACCEPT())

		lvps, found := h.Voteproofs(avp.Point())
		t.True(found)
		EqualVoteproof(t.Assert(), ivp, lvps.Cap())
		t.Nil(lvps.ACCEPT())
		EqualVoteproof(t.Assert(), ivp, lvps.INIT())
	})

	t.Run("set accpet, init", func() {
		point := base.RawPoint(33, 0)

		h := NewLastVoteproofsHandler()

		avp := t.newACCEPTVoteproof(point, true)
		t.True(h.Set(avp))

		ivp := t.newINITVoteproof(point, true)
		t.True(h.Set(ivp))

		lvps, found := h.Voteproofs(avp.Point())
		t.True(found)
		EqualVoteproof(t.Assert(), ivp, lvps.INIT())
		EqualVoteproof(t.Assert(), ivp, lvps.Cap())

		lvps = h.Last()
		EqualVoteproof(t.Assert(), avp, lvps.Cap())
		EqualVoteproof(t.Assert(), ivp, lvps.INIT())
		EqualVoteproof(t.Assert(), avp, lvps.ACCEPT())
	})

	t.Run("set init, previous accpet", func() {
		point := base.RawPoint(33, 0)

		h := NewLastVoteproofsHandler()

		ivp := t.newINITVoteproof(point, true)
		t.True(h.Set(ivp))

		avp := t.newACCEPTVoteproof(point.PrevHeight(), true)
		t.True(h.Set(avp))

		lvps := h.Last()
		EqualVoteproof(t.Assert(), ivp, lvps.Cap())
		EqualVoteproof(t.Assert(), ivp, lvps.INIT())
		EqualVoteproof(t.Assert(), avp, lvps.ACCEPT())

		lvps, found := h.Voteproofs(ivp.Point())
		t.True(found)
		EqualVoteproof(t.Assert(), avp, lvps.Cap())
		t.Nil(lvps.INIT())
		EqualVoteproof(t.Assert(), avp, lvps.ACCEPT())
	})
}

func (t *testLastVoteproofsHandler) TestForceSet() {
	t.Run("set in empty", func() {
		point := base.RawPoint(33, 0)

		h := NewLastVoteproofsHandler()
		ivp := t.newINITVoteproof(point, true)
		t.True(h.ForceSetLast(ivp))

		lvps := h.Last()
		EqualVoteproof(t.Assert(), ivp, lvps.Cap())
		EqualVoteproof(t.Assert(), ivp, lvps.INIT())

		_, found := h.Voteproofs(ivp.Point())
		t.False(found)
	})

	t.Run("set old", func() {
		point := base.RawPoint(33, 0)

		h := NewLastVoteproofsHandler()
		ivp := t.newINITVoteproof(point, true)
		t.True(h.Set(ivp))

		oldivp := t.newINITVoteproof(point.PrevHeight(), true)
		t.True(h.ForceSetLast(oldivp))

		lvps := h.Last()
		EqualVoteproof(t.Assert(), oldivp, lvps.Cap())
	})
}

func TestLastVoteproofsHandler(t *testing.T) {
	suite.Run(t, new(testLastVoteproofsHandler))
}
