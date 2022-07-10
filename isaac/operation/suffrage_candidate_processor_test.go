package isaacoperation

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testSuffrageCandiateProcessor struct {
	suite.Suite
	networkID base.NetworkID
}

func (t *testSuffrageCandiateProcessor) SetupTest() {
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageCandiateProcessor) TestNewCandidateFromEmpty() {
	getStateFunc := func(string) (base.State, bool, error) {
		return nil, false, nil // NOTE no existing state
	}

	height := base.Height(33)
	pp, err := NewSuffrageCandidateProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	priv := base.NewMPrivatekey()
	candidate := base.RandomAddress("")

	op := NewSuffrageCandidate(NewSuffrageCandidateFact(util.UUID().Bytes(), candidate, priv.Publickey()))
	t.NoError(op.Sign(priv, t.networkID, candidate))

	reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	mergevalues, reason, err := pp.Process(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)
	t.NotEmpty(mergevalues)

	for i := range mergevalues {
		t.NoError(pp.merger.Merge(mergevalues[i], []util.Hash{op.Hash()}))
	}

	t.NoError(pp.merger.Close())

	t.NotNil(pp.merger.Hash())
	t.Equal(height, pp.merger.Height())
	t.Nil(pp.merger.Previous())
	t.Equal(1, len(pp.merger.Operations()))
	t.True(op.Hash().Equal(pp.merger.Operations()[0]))

	v := pp.merger.Value()
	t.NotNil(v)

	cv := v.(base.SuffrageCandidateStateValue)
	t.NotNil(cv)

	t.Equal(1, len(cv.Nodes()))

	cvn := cv.Nodes()[0]
	t.True(candidate.Equal(cvn.Address()))
	t.True(priv.Publickey().Equal(cvn.Publickey()))

	t.Run("marshal", func() {
		b, err := util.MarshalJSON(pp.merger)
		t.NoError(err)

		t.T().Log("marshaled", string(b))
	})
}

func (t *testSuffrageCandiateProcessor) TestNewCandidate() {
	c := isaac.NewSuffrageCandidate(
		base.RandomNode(),
		base.Height(33),
		base.Height(55),
	)

	cv := isaac.NewSuffrageCandidateStateValue([]base.SuffrageCandidate{c})
	st := base.NewBaseState(
		c.Start()-1,
		isaac.SuffrageCandidateStateKey,
		cv,
		valuehash.RandomSHA256(),
		[]util.Hash{valuehash.RandomSHA256()},
	)

	getStateFunc := func(key string) (base.State, bool, error) {
		if key == isaac.SuffrageCandidateStateKey {
			return st, true, nil
		}

		return nil, false, nil
	}

	height := base.Height(33)
	pp, err := NewSuffrageCandidateProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	priv := base.NewMPrivatekey()
	candidate := base.RandomAddress("")

	op := NewSuffrageCandidate(NewSuffrageCandidateFact(util.UUID().Bytes(), candidate, priv.Publickey()))
	t.NoError(op.Sign(priv, t.networkID, candidate))

	reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	mergevalues, reason, err := pp.Process(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)
	t.NotEmpty(mergevalues)

	for i := range mergevalues {
		t.NoError(pp.merger.Merge(mergevalues[i], []util.Hash{op.Hash()}))
	}

	t.NoError(pp.merger.Close())

	t.NoError(pp.merger.Close())

	t.NotNil(pp.merger.Hash())
	t.Equal(height, pp.merger.Height())
	t.True(st.Hash().Equal(pp.merger.Previous()))
	t.Equal(1, len(pp.merger.Operations()))
	t.True(op.Hash().Equal(pp.merger.Operations()[0]))

	v := pp.merger.Value()
	t.NotNil(v)

	ucv := v.(base.SuffrageCandidateStateValue)
	t.NotNil(ucv)

	t.Equal(2, len(ucv.Nodes()))

	cvn := ucv.Nodes()[1] // NOTE newly added
	t.True(candidate.Equal(cvn.Address()))
	t.True(priv.Publickey().Equal(cvn.Publickey()))

	t.Run("marshal", func() {
		b, err := util.MarshalJSON(pp.merger)
		t.NoError(err)

		t.T().Log("marshaled", string(b))
	})
}

func TestSuffrageCandiateProcessor(t *testing.T) {
	suite.Run(t, new(testSuffrageCandiateProcessor))
}
