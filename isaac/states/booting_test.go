package isaacstates

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testBootingHandler struct {
	isaac.BaseTestBallots
}

func (t *testBootingHandler) newState() *BootingHandler {
	local := t.Local
	params := t.LocalParams

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	newhandler := NewNewBootingHandlerType(local, params,
		func() (base.Manifest, bool, error) {
			return manifest, true, nil
		},
		func(base.Node, base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	_ = (interface{})(newhandler).(newHandler)

	i, err := newhandler.new()
	t.NoError(err)

	st := i.(*BootingHandler)

	avp, _ := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)
	st.setLastVoteproofFunc(avp)

	return st
}

func (t *testBootingHandler) TestNew() {
	st := t.newState()

	_, ok := (interface{})(st).(handler)
	t.True(ok)

	sctx := newBootingSwitchContext(StateStopped)
	deferred, err := st.enter(StateStopped, sctx)
	t.Error(err)
	t.Nil(deferred)

	var rsctx joiningSwitchContext
	t.True(errors.As(err, &rsctx))

	avp := st.lastVoteproofs().ACCEPT()
	base.EqualVoteproof(t.Assert(), avp, rsctx.vp)
}

func (t *testBootingHandler) TestEmptyManifest() {
	st := t.newState()
	st.lastManifest = func() (base.Manifest, bool, error) { return nil, false, nil }

	sctx := newBootingSwitchContext(StateStopped)
	_, err := st.enter(StateStopped, sctx)
	t.Error(err)

	var rsctx SyncingSwitchContext
	t.True(errors.As(err, &rsctx))
	t.Equal(base.GenesisHeight, rsctx.height)
}

func (t *testBootingHandler) TestWrongLastACCEPTVoteproof() {
	st := t.newState()

	oldavp := st.lastVoteproofs().ACCEPT()
	point := oldavp.Point().Point.NextHeight()

	newavp, _ := t.VoteproofsPair(point, point.NextHeight(), valuehash.RandomSHA256(), nil, nil, []isaac.LocalNode{t.Local})
	st.setLastVoteproofFunc(newavp)

	sctx := newBootingSwitchContext(StateStopped)
	_, err := st.enter(StateStopped, sctx)
	t.Error(err)
	t.ErrorContains(err, "failed to enter booting state")
	t.ErrorContains(err, "failed to compare manifest with accept voteproof")
}

func (t *testBootingHandler) TestEmptySuffrage() {
	st := t.newState()
	st.nodeInConsensusNodes = func(base.Node, base.Height) (base.Suffrage, bool, error) { return nil, false, nil }

	sctx := newBootingSwitchContext(StateStopped)
	_, err := st.enter(StateStopped, sctx)
	t.Error(err)
	t.ErrorContains(err, "failed to enter booting state")
	t.ErrorContains(err, "empty suffrage for last manifest")
}

func (t *testBootingHandler) TestNotInSuffrage() {
	st := t.newState()

	suf, _ := isaac.NewTestSuffrage(2)
	st.nodeInConsensusNodes = func(base.Node, base.Height) (base.Suffrage, bool, error) { return suf, false, nil }

	sctx := newBootingSwitchContext(StateStopped)
	_, err := st.enter(StateStopped, sctx)
	t.Error(err)

	var rsctx SyncingSwitchContext
	t.True(errors.As(err, &rsctx))
	manifest, _, _ := st.lastManifest()

	t.Equal(manifest.Height(), rsctx.height)
}

func TestBootingHandler(t *testing.T) {
	suite.Run(t, new(testBootingHandler))
}
