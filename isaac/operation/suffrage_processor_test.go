package isaacoperation

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testSuffrageUpdateProcessor struct {
	suite.Suite
	networkID base.NetworkID
}

func (t *testSuffrageUpdateProcessor) SetupSuite() {
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testSuffrageUpdateProcessor) nodes(n int) ([]base.Node, []isaac.LocalNode) {
	ns := make([]base.Node, n)
	ls := make([]isaac.LocalNode, n)
	for i := range ns {
		n := isaac.RandomLocalNode()
		ns[i] = n
		ls[i] = n
	}

	return ns, ls
}

func (t *testSuffrageUpdateProcessor) operation(locals []isaac.LocalNode, news []base.Node, outs []base.Address) base.BaseOperation {
	fact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)
	t.NoError(fact.IsValid(nil))

	op := NewSuffrageUpdate(fact)
	for i := range locals {
		t.NoError(op.Sign(locals[i].Privatekey(), t.networkID))
	}

	t.NoError(op.IsValid(t.networkID))

	return op
}

func (t *testSuffrageUpdateProcessor) TestNew() {
	sufs, _ := t.nodes(2)
	getNodes := func() ([]base.Node, []base.Address, error) {
		return sufs, nil, nil
	}

	p, err := NewSuffrageUpdateProcessor(base.Height(33), base.Threshold(100), getNodes, nil, nil)
	t.NoError(err)
	t.NotNil(p)

	_ = (interface{})(p).(base.OperationProcessor)
}

func (t *testSuffrageUpdateProcessor) TestNewError() {
	t.Run("getNodes error", func() {
		getNodes := func() ([]base.Node, []base.Address, error) {
			return nil, nil, errors.Errorf("hihihi")
		}

		_, err := NewSuffrageUpdateProcessor(base.Height(33), base.Threshold(100), getNodes, nil, nil)
		t.Error(err)
		t.Contains(err.Error(), "hihihi")
	})

	t.Run("empty nodes", func() {
		getNodes := func() ([]base.Node, []base.Address, error) {
			return nil, nil, nil
		}

		_, err := NewSuffrageUpdateProcessor(base.Height(33), base.Threshold(100), getNodes, nil, nil)
		t.Error(err)
		t.True(errors.Is(err, isaac.StopProcessingRetryError))
		t.Contains(err.Error(), "empty nodes")
	})

	t.Run("empty nodes at genesis height", func() {
		getNodes := func() ([]base.Node, []base.Address, error) {
			return nil, nil, nil
		}

		_, err := NewSuffrageUpdateProcessor(base.GenesisHeight, base.Threshold(100), getNodes, nil, nil)
		t.NoError(err)
	})
}

func (t *testSuffrageUpdateProcessor) TestPreProcess() {
	sufs, locals := t.nodes(2)
	getNodes := func() ([]base.Node, []base.Address, error) {
		return sufs, nil, nil
	}

	news, _ := t.nodes(2)
	op := t.operation(locals, news, nil)

	p, err := NewSuffrageUpdateProcessor(base.Height(33), base.Threshold(100), getNodes, nil, nil)
	t.NoError(err)

	reasonerr, err := p.PreProcess(context.Background(), op, func(string) (base.State, bool, error) {
		return nil, false, nil
	})
	t.NoError(err)
	t.Nil(reasonerr)
}

func (t *testSuffrageUpdateProcessor) TestPreProcessNotPassed() {
	sufs, locals := t.nodes(2)
	getNodes := func() ([]base.Node, []base.Address, error) {
		return sufs, nil, nil
	}

	news, _ := t.nodes(2)
	op := t.operation(locals, news, nil)

	p, err := NewSuffrageUpdateProcessor(base.Height(33), base.Threshold(100), getNodes, nil, nil)
	t.NoError(err)

	reasonerr, err := p.PreProcess(context.Background(), op, func(string) (base.State, bool, error) {
		return nil, false, nil
	})
	t.NoError(err)
	t.Nil(reasonerr)
}

func TestSuffrageUpdateProcessor(t *testing.T) {
	suite.Run(t, new(testSuffrageUpdateProcessor))
}
