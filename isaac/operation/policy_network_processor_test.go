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

type testNetworkPolicyProcessor struct {
	suite.Suite
	networkID base.NetworkID
}

func (t *testNetworkPolicyProcessor) SetupTest() {
	t.networkID = util.UUID().Bytes()
}

func (t *testNetworkPolicyProcessor) prepare(height base.Height, n int, policy isaac.NetworkPolicy) (
	nodes []base.LocalNode,
	suf base.Suffrage,
	policyst base.State,
	getStateFunc base.GetStateFunc,
) {
	suf, nodes = isaac.NewTestSuffrage(n)

	values := make([]base.SuffrageNodeStateValue, len(nodes))
	for i := range values {
		values[i] = isaac.NewSuffrageNodeStateValue(isaac.NewNode(nodes[i].Publickey(), nodes[i].Address()), base.GenesisHeight+1)
	}

	sufst := base.NewBaseState(
		height-1,
		isaac.SuffrageStateKey,
		isaac.NewSuffrageNodesStateValue(height, values),
		valuehash.RandomSHA256(),
		[]util.Hash{valuehash.RandomSHA256()},
	)

	policyst = base.NewBaseState(
		height-1,
		isaac.NetworkPolicyStateKey,
		isaac.NewNetworkPolicyStateValue(policy),
		valuehash.RandomSHA256(),
		[]util.Hash{valuehash.RandomSHA256()},
	)

	getStateFunc = func(key string) (base.State, bool, error) {
		switch key {
		case isaac.NetworkPolicyStateKey:
			return policyst, true, nil
		case isaac.SuffrageStateKey:
			return sufst, true, nil
		default:
			return nil, false, nil
		}
	}

	return nodes, suf, policyst, getStateFunc
}

func (t *testNetworkPolicyProcessor) TestNew() {
	height := base.Height(33)

	policy := isaac.DefaultNetworkPolicy()
	nodes, _, policyst, getStateFunc := t.prepare(height, 3, policy)

	pp, err := NewNetworkPolicyProcessor(
		height,
		67,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	newpolicy := isaac.DefaultNetworkPolicy()
	newpolicy = newpolicy.SetMaxSuffrageSize(policy.MaxSuffrageSize() + 1)
	newpolicy = newpolicy.SetSuffrageExpelLifespan(policy.SuffrageExpelLifespan() + 1)

	op := NewNetworkPolicy(NewNetworkPolicyFact(util.UUID().Bytes(), newpolicy))
	for i := range nodes {
		t.NoError(op.NodeSign(nodes[i].Privatekey(), t.networkID, nodes[i].Address()))
	}

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)

	mergevalues, reason, err := pp.Process(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.Nil(reason)
	t.NotEmpty(mergevalues)

	t.Equal(1, len(mergevalues))

	mv := mergevalues[0]
	merger := mv.Merger(height, policyst)
	t.NoError(merger.Merge(mv.Value(), op.Hash()))
	t.NoError(merger.Close())

	t.NotNil(merger.Hash())
	t.Equal(isaac.NetworkPolicyStateKey, merger.Key())
	t.Equal(height, merger.Height())
	t.True(policyst.Hash().Equal(merger.Previous()))
	t.True(op.Hash().Equal(merger.Operations()[0]))

	newpolicyvalue, ok := merger.Value().(base.NetworkPolicyStateValue)
	t.True(ok)
	rnewpolicy := newpolicyvalue.Policy()

	base.EqualNetworkPolicy(t.Assert(), newpolicy, rnewpolicy)

	t.T().Log("newpolicy:", util.MustMarshalJSONString(newpolicy))
	t.T().Log("merged newpolicy:", util.MustMarshalJSONString(rnewpolicy))
}

func (t *testNetworkPolicyProcessor) TestNotChanged() {
	height := base.Height(33)

	policy := isaac.DefaultNetworkPolicy()
	nodes, _, _, getStateFunc := t.prepare(height, 3, policy)

	pp, err := NewNetworkPolicyProcessor(
		height,
		67,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	newpolicy := isaac.DefaultNetworkPolicy()

	op := NewNetworkPolicy(NewNetworkPolicyFact(util.UUID().Bytes(), newpolicy))
	for i := range nodes {
		t.NoError(op.NodeSign(nodes[i].Privatekey(), t.networkID, nodes[i].Address()))
	}

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "same with existing network policy")

	mergevalues, reason, err := pp.Process(context.Background(), op, getStateFunc)
	t.Error(err)
	t.Nil(reason)
	t.Empty(mergevalues)
	t.ErrorContains(err, "not pre processed for NetworkPolicy")
}

func (t *testNetworkPolicyProcessor) TestNotEnoughSigns() {
	height := base.Height(33)

	policy := isaac.DefaultNetworkPolicy()
	nodes, _, _, getStateFunc := t.prepare(height, 3, policy)

	pp, err := NewNetworkPolicyProcessor(
		height,
		100,
		getStateFunc,
		nil,
		nil,
	)
	t.NoError(err)

	newpolicy := isaac.DefaultNetworkPolicy()
	newpolicy = newpolicy.SetMaxSuffrageSize(policy.MaxSuffrageSize() + 1)

	op := NewNetworkPolicy(NewNetworkPolicyFact(util.UUID().Bytes(), newpolicy))
	for i := range nodes[:2] {
		t.NoError(op.NodeSign(nodes[i].Privatekey(), t.networkID, nodes[i].Address()))
	}

	_, reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
	t.NoError(err)
	t.NotNil(reason)
	t.ErrorContains(reason, "not enough signs")

	mergevalues, reason, err := pp.Process(context.Background(), op, getStateFunc)
	t.Error(err)
	t.Nil(reason)
	t.Empty(mergevalues)
	t.ErrorContains(err, "not pre processed for NetworkPolicy")
}

func TestNetworkPolicyProcessor(t *testing.T) {
	suite.Run(t, new(testNetworkPolicyProcessor))
}
