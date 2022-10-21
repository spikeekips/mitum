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
	existingnode base.SuffrageNodeStateValue,
	candidatenode base.SuffrageCandidateStateValue,
	getStateFunc base.GetStateFunc,
) {
	suffrageheight := base.Height(22)

	existingnodepriv = base.NewMPrivatekey()
	existingnode = isaac.NewSuffrageNodeStateValue(isaac.NewNode(existingnodepriv.Publickey(), base.RandomAddress("")), base.GenesisHeight+1)

	suffragest = base.NewBaseState(
		height-1,
		isaac.SuffrageStateKey,
		isaac.NewSuffrageNodesStateValue(suffrageheight, []base.SuffrageNodeStateValue{existingnode}),
		valuehash.RandomSHA256(),
		[]util.Hash{valuehash.RandomSHA256()},
	)

	candidate := base.RandomAddress("")

	candidatenode = isaac.NewSuffrageCandidateStateValue(
		isaac.NewNode(t.priv.Publickey(), candidate),
		height+1,
		height+2,
	)

	cv := isaac.NewSuffrageCandidatesStateValue([]base.SuffrageCandidateStateValue{candidatenode})

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

	op := NewSuffrageJoin(NewSuffrageJoinFact(util.UUID().Bytes(), candidatenode.Address(), height+1))
	t.NoError(op.NodeSign(t.priv, t.networkID, candidatenode.Address()))
	t.NoError(op.NodeSign(existingnodepriv, t.networkID, existingnode.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
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

		merger.Merge(v.Value(), []util.Hash{op.Hash()})
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
		ucv := merger.Value().(base.SuffrageCandidatesStateValue)
		t.NotNil(ucv)

		t.Equal(0, len(ucv.Nodes()))
	})

	t.Run("new joined suffrage node", func() {
		merger := mergers[isaac.SuffrageStateKey]
		uv := merger.Value().(base.SuffrageNodesStateValue)
		t.NotNil(uv)

		t.Equal(2, len(uv.Nodes()))

		nodes := uv.Nodes()
		t.True(base.IsEqualNode(nodes[0], existingnode))
		t.True(base.IsEqualNode(nodes[1], candidatenode))

		t.Equal(suffragest.Value().(base.SuffrageNodesStateValue).Height()+1, uv.Height())
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
	t.ErrorContains(err, "empty state")
	t.True(errors.Is(err, isaac.ErrStopProcessingRetry))
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

	op := NewSuffrageJoin(NewSuffrageJoinFact(util.UUID().Bytes(), candidate, height+1))
	t.NoError(op.NodeSign(t.priv, t.networkID, candidate))
	t.NoError(op.NodeSign(existingnodepriv, t.networkID, existingnode.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
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

	op := NewSuffrageJoin(NewSuffrageJoinFact(util.UUID().Bytes(), candidatenode.Address(), height+1))
	t.NoError(op.NodeSign(t.priv, t.networkID, candidatenode.Address()))
	t.NoError(op.NodeSign(existingnodepriv, t.networkID, existingnode.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	anotherop := NewSuffrageJoin(NewSuffrageJoinFact(util.UUID().Bytes(), candidatenode.Address(), height+1))
	t.NoError(anotherop.NodeSign(base.NewMPrivatekey(), t.networkID, candidatenode.Address()))
	t.NoError(anotherop.NodeSign(existingnodepriv, t.networkID, existingnode.Address()))

	_, reason, err = pp.PreProcess(context.Background(), anotherop, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "already preprocessed")
}

func (t *testSuffrageJoinProcessor) TestStartHeightMismatch() {
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

	op := NewSuffrageJoin(NewSuffrageJoinFact(util.UUID().Bytes(), candidatenode.Address(), height))
	t.NoError(op.NodeSign(t.priv, t.networkID, candidatenode.Address()))
	t.NoError(op.NodeSign(existingnodepriv, t.networkID, existingnode.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "start does not match")
}

func (t *testSuffrageJoinProcessor) TestPreProcessConstaint() {
	height := base.Height(33)

	_, _, existingnodepriv, existingnode, candidatenode, getStateFunc := t.prepare(height)

	pp, err := NewSuffrageJoinProcessor(
		height,
		67,
		getStateFunc,
		func(base.Height, base.GetStateFunc) (base.OperationProcessorProcessFunc, error) {
			return func(context.Context, base.Operation, base.GetStateFunc) (base.OperationProcessReasonError, error) {
				return base.NewBaseOperationProcessReasonError("hehehe"), nil
			}, nil
		},
		nil,
	)
	t.NoError(err)

	op := NewSuffrageJoin(NewSuffrageJoinFact(util.UUID().Bytes(), candidatenode.Address(), height+1))
	t.NoError(op.NodeSign(t.priv, t.networkID, candidatenode.Address()))
	t.NoError(op.NodeSign(existingnodepriv, t.networkID, existingnode.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "hehehe")
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

	op := NewSuffrageJoin(NewSuffrageJoinFact(util.UUID().Bytes(), candidatenode.Address(), height+1))
	t.NoError(op.NodeSign(base.NewMPrivatekey(), t.networkID, candidatenode.Address()))
	t.NoError(op.NodeSign(existingnodepriv, t.networkID, existingnode.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
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
	op := NewSuffrageJoin(NewSuffrageJoinFact(util.UUID().Bytes(), unknown, height+1))
	t.NoError(op.NodeSign(existingnodepriv, t.networkID, existingnode.Address()))
	t.NoError(op.NodeSign(base.NewMPrivatekey(), t.networkID, unknown))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
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

	op := NewSuffrageJoin(NewSuffrageJoinFact(util.UUID().Bytes(), existingnode.Address(), height+1))
	t.NoError(op.NodeSign(existingnodepriv, t.networkID, existingnode.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "candidate already in suffrage")
}

func (t *testSuffrageJoinProcessor) TestNotEnoughSign() {
	height := base.Height(33)

	_, _, _, _, candidatenode, getStateFunc := t.prepare(height)

	pp, err := NewSuffrageJoinProcessor(
		height,
		67,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	op := NewSuffrageJoin(NewSuffrageJoinFact(util.UUID().Bytes(), candidatenode.Address(), height+1))
	t.NoError(op.NodeSign(t.priv, t.networkID, candidatenode.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "not enough signs")
}

func TestSuffrageJoinProcessor(t *testing.T) {
	suite.Run(t, new(testSuffrageJoinProcessor))
}
