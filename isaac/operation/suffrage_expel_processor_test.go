package isaacoperation

import (
	"context"
	"fmt"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testSuffrageExpelProcessor struct {
	suite.Suite
	networkID base.NetworkID
}

func (t *testSuffrageExpelProcessor) SetupSuite() {
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageExpelProcessor) prepare(height base.Height, n int) (
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

func (t *testSuffrageExpelProcessor) TestNew() {
	height := base.Height(33)

	suffragest, nodes, getStateFunc := t.prepare(height, 3)

	local := nodes[0]
	expelnode := nodes[1]

	pp, err := NewSuffrageExpelProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	op := isaac.NewSuffrageExpelOperation(isaac.NewSuffrageExpelFact(expelnode.Address(), height, height+1, util.UUID().String()))
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	ctx, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	var preprocessed []base.Address
	t.NoError(util.LoadFromContextOK(ctx, ExpelPreProcessedContextKey, &preprocessed))
	t.Equal(1, len(preprocessed))
	t.True(expelnode.Address().Equal(preprocessed[0]))

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
				t.Fail(fmt.Sprintf("unknown state found, %q", v.Key()))
			}

			merger = v.Merger(height, st)

			mergers[v.Key()] = merger
		}

		merger.Merge(v.Value(), op.Hash())
	}

	var nsuffragest base.State

	for _, merger := range mergers {
		var st base.State

		switch merger.Key() {
		case isaac.SuffrageStateKey:
			st = suffragest
		default:
			t.Fail(fmt.Sprintf("unknown state found, %q", merger.Key()))
		}

		nst, err := merger.CloseValue()
		t.NoError(err)

		if merger.Key() == isaac.SuffrageStateKey {
			nsuffragest = nst
		}

		t.NotNil(nst.Hash())
		t.Equal(height, nst.Height())
		t.True(st.Hash().Equal(nst.Previous()))
		t.Equal(1, len(nst.Operations()))
		t.True(op.Hash().Equal(nst.Operations()[0]))
	}

	t.Run("removed from suffrage nodes", func() {
		t.NotNil(nsuffragest)

		uv := nsuffragest.Value().(base.SuffrageNodesStateValue)
		t.NotNil(uv)

		t.Equal(2, len(uv.Nodes()))

		unodes := uv.Nodes()
		t.True(base.IsEqualNode(nodes[0], unodes[0]))
		t.True(base.IsEqualNode(nodes[2], unodes[1]))

		t.Equal(suffragest.Value().(base.SuffrageNodesStateValue).Height()+1, uv.Height())
	})
}

func (t *testSuffrageExpelProcessor) TestFromEmptyState() {
	height := base.Height(33)

	getStateFunc := func(key string) (base.State, bool, error) {
		return nil, false, nil
	}

	_, err := NewSuffrageExpelProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.Error(err)
	t.ErrorContains(err, "empty state")
}

func (t *testSuffrageExpelProcessor) TestPreProcessed() {
	height := base.Height(33)

	_, nodes, getStateFunc := t.prepare(height, 3)

	expelnode := nodes[0]

	pp, err := NewSuffrageExpelProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	local := nodes[1]

	op := isaac.NewSuffrageExpelOperation(isaac.NewSuffrageExpelFact(expelnode.Address(), height, height+1, util.UUID().String()))
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	anotherop := isaac.NewSuffrageExpelOperation(isaac.NewSuffrageExpelFact(expelnode.Address(), height, height+1, util.UUID().String()))
	t.NoError(anotherop.NodeSign(base.NewMPrivatekey(), t.networkID, local.Address()))

	_, reason, err = pp.PreProcess(context.Background(), anotherop, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "already preprocessed")
}

func (t *testSuffrageExpelProcessor) TestWrongHeight() {
	height := base.Height(33)

	_, nodes, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageExpelProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	t.Run("wrong start", func() {
		node := nodes[0]

		op := isaac.NewSuffrageExpelOperation(isaac.NewSuffrageExpelFact(node.Address(), height+1, height+2, util.UUID().String()))
		t.NoError(op.NodeSign(node.Privatekey(), t.networkID, node.Address()))

		_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
		t.NoError(err)
		t.NotNil(reason)
		t.ErrorContains(reason, "wrong start height")
	})

	t.Run("expired start", func() {
		node := nodes[1]

		op := isaac.NewSuffrageExpelOperation(isaac.NewSuffrageExpelFact(node.Address(), height-2, height-1, util.UUID().String()))
		t.NoError(op.NodeSign(node.Privatekey(), t.networkID, node.Address()))

		_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
		t.NoError(err)
		t.NotNil(reason)
		t.ErrorContains(reason, "expired")
	})
}

func (t *testSuffrageExpelProcessor) TestPreProcessConstaint() {
	height := base.Height(33)

	_, nodes, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageExpelProcessor(
		height,
		getStateFunc,
		func(base.Height, base.GetStateFunc) (base.OperationProcessorProcessFunc, error) {
			return func(context.Context, base.Operation, base.GetStateFunc) (base.OperationProcessReasonError, error) {
				return base.NewBaseOperationProcessReason("hehehe"), nil
			}, nil
		},
		nil,
	)
	t.NoError(err)

	local := nodes[1]

	op := isaac.NewSuffrageExpelOperation(isaac.NewSuffrageExpelFact(local.Address(), height, height+1, util.UUID().String()))
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "hehehe")
}

func (t *testSuffrageExpelProcessor) TestNotSuffrage() {
	height := base.Height(33)

	_, _, getStateFunc := t.prepare(height, 3)

	pp, err := NewSuffrageExpelProcessor(
		height,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	unknown := base.RandomAddress("")
	op := isaac.NewSuffrageExpelOperation(isaac.NewSuffrageExpelFact(unknown, height, height+1, util.UUID().String()))
	t.NoError(op.NodeSign(base.NewMPrivatekey(), t.networkID, unknown))

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "not in suffrage")
}

func TestSuffrageExpelProcessor(t *testing.T) {
	suite.Run(t, new(testSuffrageExpelProcessor))
}
