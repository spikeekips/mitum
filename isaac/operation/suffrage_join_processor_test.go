package isaacoperation

import (
	"context"
	"errors"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testSuffrageJoinProcessor struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testSuffrageJoinProcessor) SetupTest() {
	t.priv = base.NewMPrivatekey()
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageJoinProcessor) prepare(height base.Height) (
	suffragest base.BaseState,
	candidatest base.BaseState,
	existingnodepriv base.Privatekey,
	existingnode base.Node,
	candidatenode base.SuffrageCandidate,
	getStateFunc base.GetStateFunc,
) {
	suffrageheight := base.Height(22)

	existingnodepriv = base.NewMPrivatekey()
	existingnode = isaac.NewNode(existingnodepriv.Publickey(), base.RandomAddress(""))

	suffragest = base.NewBaseState(
		height-1,
		isaac.SuffrageStateKey,
		isaac.NewSuffrageStateValue(suffrageheight, []base.Node{existingnode}),
		valuehash.RandomSHA256(),
		[]util.Hash{valuehash.RandomSHA256()},
	)

	candidate := base.RandomAddress("")

	candidatenode = isaac.NewSuffrageCandidate(
		isaac.NewNode(t.priv.Publickey(), candidate),
		height,
		height+1,
	)

	cv := isaac.NewSuffrageCandidateStateValue([]base.SuffrageCandidate{candidatenode})

	candidatest = base.NewBaseState(
		candidatenode.Start()-1,
		isaac.SuffrageCandidateStateKey,
		cv,
		valuehash.RandomSHA256(),
		[]util.Hash{valuehash.RandomSHA256()},
	)

	getStateFunc = func(key string) (base.State, bool, error) {
		switch key {
		case isaac.SuffrageCandidateStateKey:
			return candidatest, true, nil
		case isaac.SuffrageStateKey:
			return suffragest, true, nil
		default:
			return nil, false, nil
		}
	}

	return suffragest, candidatest, existingnodepriv, existingnode, candidatenode, getStateFunc
}

func (t *testSuffrageJoinProcessor) TestNew() {
	height := base.Height(33)

	suffragest, candidatest, existingnodepriv, existingnode, candidatenode, getStateFunc := t.prepare(height)

	pp, err := NewSuffrageJoinProcessor(
		height,
		67,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	op := NewSuffrageJoin(NewSuffrageJoinFact(candidatenode.Address(), candidatest.Hash()))
	t.NoError(op.Sign(t.priv, t.networkID, candidatenode.Address()))
	t.NoError(op.Sign(existingnodepriv, t.networkID, existingnode.Address()))

	reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	mergevalues, reason, err := pp.Process(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)
	t.NotEmpty(mergevalues)

	t.Equal(2, len(mergevalues))

	mergers := map[string]base.StateValueMerger{}

	for i := range mergevalues {
		v := mergevalues[i]

		merger, found := mergers[v.Key()]
		if !found {
			var st base.State

			switch v.Key() {
			case isaac.SuffrageCandidateStateKey:
				st = candidatest
			case isaac.SuffrageStateKey:
				st = suffragest
			}

			merger = v.Merger(height, st)

			mergers[v.Key()] = merger
		}

		merger.Merge(v, []util.Hash{op.Hash()})
	}

	for _, merger := range mergers {

		var st base.State

		switch merger.Key() {
		case isaac.SuffrageCandidateStateKey:
			st = candidatest
		case isaac.SuffrageStateKey:
			st = suffragest
		}

		t.NoError(merger.Close())

		t.NotNil(merger.Hash())
		t.Equal(height, merger.Height())
		t.True(st.Hash().Equal(merger.Previous()))
		t.Equal(1, len(merger.Operations()))
		t.True(op.Hash().Equal(merger.Operations()[0]))
	}

	t.Run("empty candidate", func() {
		merger := mergers[isaac.SuffrageCandidateStateKey]
		ucv := merger.Value().(base.SuffrageCandidateStateValue)
		t.NotNil(ucv)

		t.Equal(0, len(ucv.Nodes()))
	})

	t.Run("new joined suffrage node", func() {
		merger := mergers[isaac.SuffrageStateKey]
		uv := merger.Value().(base.SuffrageStateValue)
		t.NotNil(uv)

		t.Equal(2, len(uv.Nodes()))

		nodes := uv.Nodes()
		t.True(base.IsEqualNode(nodes[0], existingnode))
		t.True(base.IsEqualNode(nodes[1], candidatenode))
	})
}

func (t *testSuffrageJoinProcessor) TestFromEmptyState() {
	height := base.Height(33)

	_, candidatest, _, _, _, _ := t.prepare(height)

	getStateFunc := func(key string) (base.State, bool, error) {
		switch key {
		case isaac.SuffrageCandidateStateKey:
			return candidatest, true, nil
		default:
			return nil, false, nil
		}
	}

	_, err := NewSuffrageJoinProcessor(
		height,
		67,
		getStateFunc,
		nil,
		nil,
	)
	t.Error(err)
	t.ErrorContains(err, "empty state returned")
	t.True(errors.Is(err, isaac.StopProcessingRetryError))
}

func (t *testSuffrageJoinProcessor) TestFromEmptyCandidateState() {
	height := base.Height(33)

	suffragest, _, existingnodepriv, existingnode, _, _ := t.prepare(height)

	getStateFunc := func(key string) (base.State, bool, error) {
		switch key {
		case isaac.SuffrageStateKey:
			return suffragest, true, nil
		default:
			return nil, false, nil
		}
	}

	pp, err := NewSuffrageJoinProcessor(
		height,
		67,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	candidate := base.RandomAddress("")

	op := NewSuffrageJoin(NewSuffrageJoinFact(candidate, valuehash.RandomSHA256()))
	t.NoError(op.Sign(t.priv, t.networkID, candidate))
	t.NoError(op.Sign(existingnodepriv, t.networkID, existingnode.Address()))

	reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)

	t.ErrorContains(reason, "not candidate")
}

func (t *testSuffrageJoinProcessor) TestPreProcessed() {
	height := base.Height(33)

	suffragest, candidatest, existingnodepriv, existingnode, candidatenode, _ := t.prepare(height)

	getStateFunc := func(key string) (base.State, bool, error) {
		switch key {
		case isaac.SuffrageCandidateStateKey:
			return candidatest, true, nil
		case isaac.SuffrageStateKey:
			return suffragest, true, nil
		default:
			return nil, false, nil
		}
	}

	pp, err := NewSuffrageJoinProcessor(
		height,
		67,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	op := NewSuffrageJoin(NewSuffrageJoinFact(candidatenode.Address(), valuehash.RandomSHA256()))
	t.NoError(op.Sign(t.priv, t.networkID, candidatenode.Address()))
	t.NoError(op.Sign(existingnodepriv, t.networkID, existingnode.Address()))

	reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	anotherop := NewSuffrageJoin(NewSuffrageJoinFact(candidatenode.Address(), valuehash.RandomSHA256()))
	t.NoError(anotherop.Sign(base.NewMPrivatekey(), t.networkID, candidatenode.Address()))
	t.NoError(anotherop.Sign(existingnodepriv, t.networkID, existingnode.Address()))

	reason, err = pp.PreProcess(context.Background(), anotherop, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "already preprocessed")
}

func (t *testSuffrageJoinProcessor) TestNotSignedByCandidate() {
	height := base.Height(33)

	_, _, existingnodepriv, existingnode, candidatenode, getStateFunc := t.prepare(height)

	pp, err := NewSuffrageJoinProcessor(
		height,
		67,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	op := NewSuffrageJoin(NewSuffrageJoinFact(candidatenode.Address(), valuehash.RandomSHA256()))
	t.NoError(op.Sign(base.NewMPrivatekey(), t.networkID, candidatenode.Address()))
	t.NoError(op.Sign(existingnodepriv, t.networkID, existingnode.Address()))

	reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "not signed by candidate key")
}

func (t *testSuffrageJoinProcessor) TestNotCandidate() {
	height := base.Height(33)

	_, _, existingnodepriv, existingnode, _, getStateFunc := t.prepare(height)

	pp, err := NewSuffrageJoinProcessor(
		height,
		67,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	unknown := base.RandomAddress("")
	op := NewSuffrageJoin(NewSuffrageJoinFact(unknown, valuehash.RandomSHA256()))
	t.NoError(op.Sign(existingnodepriv, t.networkID, existingnode.Address()))
	t.NoError(op.Sign(base.NewMPrivatekey(), t.networkID, unknown))

	reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "candidate not in candidates")
}

func (t *testSuffrageJoinProcessor) TestAlreadyInSuffrage() {
	height := base.Height(33)

	suffragest, candidatest, existingnodepriv, existingnode, _, _ := t.prepare(height)

	getStateFunc := func(key string) (base.State, bool, error) {
		switch key {
		case isaac.SuffrageCandidateStateKey:
			return candidatest, true, nil
		case isaac.SuffrageStateKey:
			return suffragest, true, nil
		default:
			return nil, false, nil
		}
	}

	pp, err := NewSuffrageJoinProcessor(
		height,
		67,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	op := NewSuffrageJoin(NewSuffrageJoinFact(existingnode.Address(), candidatest.Hash()))
	t.NoError(op.Sign(existingnodepriv, t.networkID, existingnode.Address()))

	reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "candidate already in suffrage")
}

func (t *testSuffrageJoinProcessor) TestNotEnoughSign() {
	height := base.Height(33)

	_, candidatest, _, _, candidatenode, getStateFunc := t.prepare(height)

	pp, err := NewSuffrageJoinProcessor(
		height,
		67,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	op := NewSuffrageJoin(NewSuffrageJoinFact(candidatenode.Address(), candidatest.Hash()))
	t.NoError(op.Sign(t.priv, t.networkID, candidatenode.Address()))

	reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "not enough signs")
}

func TestSuffrageJoinProcessor(t *testing.T) {
	suite.Run(t, new(testSuffrageJoinProcessor))
}
