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

type testSuffrageWithdrawProcessor struct {
	suite.Suite
	networkID base.NetworkID
}

func (t *testSuffrageWithdrawProcessor) SetupSuite() {
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageWithdrawProcessor) prepare(height base.Height, n int) (
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

func (t *testSuffrageWithdrawProcessor) TestNew() {
	height := base.Height(33)

	suffragest, nodes, getStateFunc := t.prepare(height, 3)

	local := nodes[0]
	withdrawnode := nodes[1]

	pp, err := NewSuffrageWithdrawProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	op := isaac.NewSuffrageWithdrawOperation(isaac.NewSuffrageWithdrawFact(withdrawnode.Address(), height, height+1, util.UUID().String()))
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	ctx, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	var preprocessed []base.Address
	t.NoError(util.LoadFromContextOK(ctx, WithdrawPreProcessedContextKey, &preprocessed))
	t.Equal(1, len(preprocessed))
	t.True(withdrawnode.Address().Equal(preprocessed[0]))

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

func (t *testSuffrageWithdrawProcessor) TestFromEmptyState() {
	height := base.Height(33)

	getStateFunc := func(key string) (base.State, bool, error) {
		return nil, false, nil
	}

	_, err := NewSuffrageWithdrawProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.Error(err)
	t.ErrorContains(err, "empty state")
	t.True(errors.Is(err, isaac.ErrStopProcessingRetry))
}

func (t *testSuffrageWithdrawProcessor) TestPreProcessed() {
	height := base.Height(33)

	_, nodes, getStateFunc := t.prepare(height, 3)

	withdrawnode := nodes[0]

	pp, err := NewSuffrageWithdrawProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	local := nodes[1]

	op := isaac.NewSuffrageWithdrawOperation(isaac.NewSuffrageWithdrawFact(withdrawnode.Address(), height, height+1, util.UUID().String()))
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	anotherop := isaac.NewSuffrageWithdrawOperation(isaac.NewSuffrageWithdrawFact(withdrawnode.Address(), height, height+1, util.UUID().String()))
	t.NoError(anotherop.NodeSign(base.NewMPrivatekey(), t.networkID, local.Address()))

	_, reason, err = pp.PreProcess(context.Background(), anotherop, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "already preprocessed")
}

func (t *testSuffrageWithdrawProcessor) TestWrongHeight() {
	height := base.Height(33)

	_, nodes, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageWithdrawProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	t.Run("wrong start", func() {
		node := nodes[0]

		op := isaac.NewSuffrageWithdrawOperation(isaac.NewSuffrageWithdrawFact(node.Address(), height+1, height+2, util.UUID().String()))
		t.NoError(op.NodeSign(node.Privatekey(), t.networkID, node.Address()))

		_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
		t.NoError(err)
		t.NotNil(reason)
		t.ErrorContains(reason, "wrong start height")
	})

	t.Run("expired start", func() {
		node := nodes[1]

		op := isaac.NewSuffrageWithdrawOperation(isaac.NewSuffrageWithdrawFact(node.Address(), height-2, height-1, util.UUID().String()))
		t.NoError(op.NodeSign(node.Privatekey(), t.networkID, node.Address()))

		_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
		t.NoError(err)
		t.NotNil(reason)
		t.ErrorContains(reason, "expired")
	})
}

func (t *testSuffrageWithdrawProcessor) TestPreProcessConstaint() {
	height := base.Height(33)

	_, nodes, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageWithdrawProcessor(
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

	op := isaac.NewSuffrageWithdrawOperation(isaac.NewSuffrageWithdrawFact(local.Address(), height, height+1, util.UUID().String()))
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "hehehe")
}

func (t *testSuffrageWithdrawProcessor) TestNotSuffrage() {
	height := base.Height(33)

	_, _, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageWithdrawProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	unknown := base.RandomAddress("")
	op := isaac.NewSuffrageWithdrawOperation(isaac.NewSuffrageWithdrawFact(unknown, height, height+1, util.UUID().String()))
	t.NoError(op.NodeSign(base.NewMPrivatekey(), t.networkID, unknown))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "not in suffrage")
}

func TestSuffrageWithdrawProcessor(t *testing.T) {
	suite.Run(t, new(testSuffrageWithdrawProcessor))
}
