package isaacoperation

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testSuffrageDisjoinProcessor struct {
	suite.Suite
	networkID base.NetworkID
}

func (t *testSuffrageDisjoinProcessor) SetupTest() {
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageDisjoinProcessor) prepare(height base.Height, n int) (
	suffragest base.BaseState,
	nodes []isaac.LocalNode,
	getStateFunc base.GetStateFunc,
) {
	suffrageheight := base.Height(22)

	nodes = make([]isaac.LocalNode, n)
	nodesstv := make([]base.SuffrageNodeStateValue, n)

	for i := range nodes {
		node := isaac.NewLocalNode(base.NewMPrivatekey(), base.RandomAddress(""))

		nodes[i] = node
		nodesstv[i] = isaac.NewSuffrageNodeStateValue(node, height)
	}

	suffragest = base.NewBaseState(
		height-1,
		isaac.SuffrageStateKey,
		isaac.NewSuffrageNodesStateValue(suffrageheight, nodesstv),
		valuehash.RandomSHA256(),
		[]util.Hash{valuehash.RandomSHA256()},
	)

	getStateFunc = func(key string) (base.State, bool, error) {
		switch key {
		case isaac.SuffrageStateKey:
			return suffragest, true, nil
		default:
			return nil, false, nil
		}
	}

	return suffragest, nodes, getStateFunc
}

func (t *testSuffrageDisjoinProcessor) TestNew() {
	height := base.Height(33)

	suffragest, nodes, getStateFunc := t.prepare(height, 3)

	local := nodes[1]

	pp, err := NewSuffrageDisjoinProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	op := NewSuffrageDisjoin(NewSuffrageDisjoinFact(util.UUID().Bytes(), local.Address(), height))
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	mergevalues, reason, err := pp.Process(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)
	t.NotEmpty(mergevalues)

	t.Equal(1, len(mergevalues))

	mergers := map[string]base.StateValueMerger{}

	for i := range mergevalues {
		v := mergevalues[i]

		merger, found := mergers[v.Key()]
		if !found {
			var st base.State

			switch v.Key() {
			case isaac.SuffrageStateKey:
				st = suffragest
			default:
				t.NoError(errors.Errorf("unknown state found, %q", v.Key()))
			}

			merger = v.Merger(height, st)

			mergers[v.Key()] = merger
		}

		merger.Merge(v.Value(), []util.Hash{op.Hash()})
	}

	for _, merger := range mergers {
		var st base.State

		switch merger.Key() {
		case isaac.SuffrageStateKey:
			st = suffragest
		default:
			t.NoError(errors.Errorf("unknown state found, %q", merger.Key()))
		}

		t.NoError(merger.Close())

		t.NotNil(merger.Hash())
		t.Equal(height, merger.Height())
		t.True(st.Hash().Equal(merger.Previous()))
		t.Equal(1, len(merger.Operations()))
		t.True(op.Hash().Equal(merger.Operations()[0]))
	}

	t.Run("removed from suffrage nodes", func() {
		merger := mergers[isaac.SuffrageStateKey]
		uv := merger.Value().(base.SuffrageNodesStateValue)
		t.NotNil(uv)

		t.Equal(2, len(uv.Nodes()))

		unodes := uv.Nodes()
		t.True(base.IsEqualNode(nodes[0], unodes[0]))
		t.True(base.IsEqualNode(nodes[2], unodes[1]))

		t.Equal(suffragest.Value().(base.SuffrageNodesStateValue).Height()+1, uv.Height())
	})
}

func (t *testSuffrageDisjoinProcessor) TestFromEmptyState() {
	height := base.Height(33)

	getStateFunc := func(key string) (base.State, bool, error) {
		return nil, false, nil
	}

	_, err := NewSuffrageDisjoinProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.Error(err)
	t.ErrorContains(err, "empty state")
	t.True(errors.Is(err, isaac.ErrStopProcessingRetry))
}

func (t *testSuffrageDisjoinProcessor) TestPreProcessed() {
	height := base.Height(33)

	_, nodes, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageDisjoinProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	local := nodes[1]

	op := NewSuffrageDisjoin(NewSuffrageDisjoinFact(util.UUID().Bytes(), local.Address(), height))
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	anotherop := NewSuffrageDisjoin(NewSuffrageDisjoinFact(util.UUID().Bytes(), local.Address(), height))
	t.NoError(anotherop.NodeSign(base.NewMPrivatekey(), t.networkID, local.Address()))

	_, reason, err = pp.PreProcess(context.Background(), anotherop, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "already preprocessed")
}

func (t *testSuffrageDisjoinProcessor) TestStartHeightMismatch() {
	height := base.Height(33)

	_, nodes, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageDisjoinProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	local := nodes[1]

	op := NewSuffrageDisjoin(NewSuffrageDisjoinFact(util.UUID().Bytes(), local.Address(), height+1))
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "start does not match")
}

func (t *testSuffrageDisjoinProcessor) TestPreProcessConstaint() {
	height := base.Height(33)

	_, nodes, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageDisjoinProcessor(
		height,
		getStateFunc,
		func(base.Height, base.GetStateFunc) (base.OperationProcessorProcessFunc, error) {
			return func(context.Context, base.Operation, base.GetStateFunc) (base.OperationProcessReasonError, error) {
				return base.NewBaseOperationProcessReasonError("hehehe"), nil
			}, nil
		},
		nil,
	)
	t.NoError(err)

	local := nodes[1]

	op := NewSuffrageDisjoin(NewSuffrageDisjoinFact(util.UUID().Bytes(), local.Address(), height))
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "hehehe")
}

func (t *testSuffrageDisjoinProcessor) TestPreProcessWithdrew() {
	height := base.Height(33)

	_, nodes, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageDisjoinProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	local := nodes[1]

	op := NewSuffrageDisjoin(NewSuffrageDisjoinFact(util.UUID().Bytes(), local.Address(), height))
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	ctx := context.WithValue(context.Background(), WithdrawPreProcessedContextKey, []base.Address{local.Address()})
	_, reason, err := pp.PreProcess(ctx, op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "already withdrew")
}

func (t *testSuffrageDisjoinProcessor) TestNotSignedByCandidate() {
	height := base.Height(33)

	_, nodes, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageDisjoinProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	local := nodes[1]

	op := NewSuffrageDisjoin(NewSuffrageDisjoinFact(util.UUID().Bytes(), local.Address(), height))
	t.NoError(op.NodeSign(base.NewMPrivatekey(), t.networkID, local.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "not signed by node key")
}

func (t *testSuffrageDisjoinProcessor) TestNotSuffrage() {
	height := base.Height(33)

	_, _, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageDisjoinProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	unknown := base.RandomAddress("")
	op := NewSuffrageDisjoin(NewSuffrageDisjoinFact(util.UUID().Bytes(), unknown, height))
	t.NoError(op.NodeSign(base.NewMPrivatekey(), t.networkID, unknown))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "not in suffrage")
}

func TestSuffrageDisjoinProcessor(t *testing.T) {
	suite.Run(t, new(testSuffrageDisjoinProcessor))
}
