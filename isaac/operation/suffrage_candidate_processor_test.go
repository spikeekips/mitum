package isaacoperation

import (
	"context"
	"sort"
	"strings"
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
		50,
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
		50,
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

func (t *testSuffrageCandiateProcessor) TestPreProcess() {
	height := base.Height(33)

	getStateFunc := func(key string) (base.State, bool, error) {
		return nil, false, nil
	}

	priv := base.NewMPrivatekey()
	candidate := base.RandomAddress("")

	op := NewSuffrageCandidate(NewSuffrageCandidateFact(util.UUID().Bytes(), candidate, priv.Publickey()))
	t.NoError(op.Sign(priv, t.networkID, candidate))

	t.Run("already processed", func() {
		pp, err := NewSuffrageCandidateProcessor(
			height,
			getStateFunc,
			nil,
			nil,
			50,
		)
		t.NoError(err)

		reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
		t.NoError(err)
		t.Nil(reason)

		// NOTE with same candidate
		opsame := NewSuffrageCandidate(NewSuffrageCandidateFact(util.UUID().Bytes(), candidate, priv.Publickey()))
		t.NoError(opsame.Sign(priv, t.networkID, candidate))

		reason, err = pp.PreProcess(context.Background(), opsame, getStateFunc)
		t.NoError(err)
		t.NotNil(reason)
		t.ErrorContains(reason, "already preprocessed")
	})

	t.Run("already processed with old state", func() {
		c := isaac.NewSuffrageCandidate(
			base.RandomNode(),
			height,
			height+1,
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

		pp, err := NewSuffrageCandidateProcessor(
			height,
			getStateFunc,
			nil,
			nil,
			50,
		)
		t.NoError(err)

		reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
		t.NoError(err)
		t.Nil(reason)

		// NOTE with same candidate
		opsame := NewSuffrageCandidate(NewSuffrageCandidateFact(util.UUID().Bytes(), candidate, priv.Publickey()))
		t.NoError(opsame.Sign(priv, t.networkID, candidate))

		reason, err = pp.PreProcess(context.Background(), opsame, getStateFunc)
		t.NoError(err)
		t.NotNil(reason)
		t.ErrorContains(reason, "already preprocessed")
	})

	t.Run("already candidate", func() {
		c := isaac.NewSuffrageCandidate(
			isaac.NewNode(priv.Publickey(), candidate),
			height,
			height+1,
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

		pp, err := NewSuffrageCandidateProcessor(
			height,
			getStateFunc,
			nil,
			nil,
			50,
		)
		t.NoError(err)

		reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
		t.NoError(err)
		t.NotNil(reason)
		t.ErrorContains(reason, "already candidate")
	})

	t.Run("already candidate, but old", func() {
		c := isaac.NewSuffrageCandidate(
			isaac.NewNode(priv.Publickey(), candidate),
			height-3,
			height-1,
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

		pp, err := NewSuffrageCandidateProcessor(
			height,
			getStateFunc,
			nil,
			nil,
			50,
		)
		t.NoError(err)

		reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
		t.NoError(err)
		t.Nil(reason)
	})

	t.Run("filter by pre constraint func", func() {
		getStateFunc := func(key string) (base.State, bool, error) {
			return nil, false, nil
		}

		pp, err := NewSuffrageCandidateProcessor(
			height,
			getStateFunc,
			func(context.Context, base.Operation, base.GetStateFunc) (base.OperationProcessReasonError, error) {
				return base.NewBaseOperationProcessReasonError("hehehe"), nil
			},
			nil,
			50,
		)
		t.NoError(err)

		reason, err := pp.PreProcess(context.Background(), op, getStateFunc)
		t.NoError(err)
		t.NotNil(reason)
		t.ErrorContains(reason, "hehehe")
	})
}

func (t *testSuffrageCandiateProcessor) TestProcess() {
	height := base.Height(33)

	getStateFunc := func(key string) (base.State, bool, error) {
		return nil, false, nil
	}

	priv := base.NewMPrivatekey()
	candidate := base.RandomAddress("")

	op := NewSuffrageCandidate(NewSuffrageCandidateFact(util.UUID().Bytes(), candidate, priv.Publickey()))
	t.NoError(op.Sign(priv, t.networkID, candidate))

	lifespan := base.Height(50)

	t.Run("ok", func() {
		pp, err := NewSuffrageCandidateProcessor(
			height,
			getStateFunc,
			nil,
			nil,
			lifespan,
		)
		t.NoError(err)

		values, reason, err := pp.Process(context.Background(), op, getStateFunc)
		t.NoError(err)
		t.Nil(reason)
		t.Equal(1, len(values))

		i := values[0]
		t.Equal(isaac.SuffrageCandidateStateKey, i.Key())

		cv := i.Value().(isaac.SuffrageCandidateStateValue)
		t.NoError(cv.IsValid(nil))
		t.Equal(1, len(cv.Nodes()))

		node := cv.Nodes()[0]
		t.True(candidate.Equal(node.Address()))
		t.True(priv.Publickey().Equal(node.Publickey()))
		t.Equal(height+1, node.Start())
		t.Equal(height+1+lifespan, node.Deadline())
	})
}

func (t *testSuffrageCandiateProcessor) TestProcessConcurrent() {
	height := base.Height(33)

	getStateFunc := func(key string) (base.State, bool, error) {
		return nil, false, nil
	}

	lifespan := base.Height(50)

	pp, err := NewSuffrageCandidateProcessor(
		height,
		getStateFunc,
		nil,
		nil,
		lifespan,
	)
	t.NoError(err)

	worker := util.NewErrgroupWorker(context.Background(), 33)
	defer worker.Close()

	privs := make([]base.Privatekey, 3)
	nodes := make([]base.Node, len(privs))
	ops := make([]base.Operation, len(privs))

	for i := range privs {
		i := i

		priv := base.NewMPrivatekey()
		address := base.RandomAddress("")

		privs[i] = priv
		nodes[i] = isaac.NewNode(priv.Publickey(), address)

		t.NoError(worker.NewJob(func(context.Context, uint64) error {
			op := NewSuffrageCandidate(NewSuffrageCandidateFact(util.UUID().Bytes(), address, priv.Publickey()))
			if err := op.Sign(priv, t.networkID, address); err != nil {
				return err
			}

			ops[i] = op

			values, reason, err := pp.Process(context.Background(), op, getStateFunc)
			if err != nil {
				return err
			}

			if reason != nil {
				return reason
			}

			for i := range values {
				t.NoError(pp.merger.Merge(values[i], []util.Hash{op.Hash()}))
			}

			return nil
		}))
	}

	worker.Done()
	t.NoError(worker.Wait())

	t.NoError(pp.merger.Close())

	t.NotNil(pp.merger.Hash())
	t.Equal(height, pp.merger.Height())
	t.Nil(pp.merger.Previous())
	t.Equal(len(ops), len(pp.merger.Operations()))

	sort.Slice(ops, func(i, j int) bool {
		return strings.Compare(ops[i].Hash().String(), ops[j].Hash().String()) < 0
	})

	mops := pp.merger.Operations()

	for i := range ops {
		t.True(ops[i].Hash().Equal(mops[i]))
	}

	v := pp.merger.Value()
	t.NotNil(v)

	cv := v.(base.SuffrageCandidateStateValue)
	t.NotNil(cv)

	t.Equal(len(privs), len(cv.Nodes()))

	sort.Slice(nodes, func(i, j int) bool {
		return strings.Compare(nodes[i].Address().String(), nodes[j].Address().String()) < 0
	})

	mnodes := cv.Nodes()

	for i := range nodes {
		t.True(nodes[i].Address().Equal(mnodes[i].Address()))
		t.True(nodes[i].Publickey().Equal(mnodes[i].Publickey()))
	}

	for i := range mnodes {
		t.NoError(mnodes[i].IsValid(nil))

		t.Equal(height+1, mnodes[i].Start())
		t.Equal(height+1+lifespan, mnodes[i].Deadline())
	}
}

func TestSuffrageCandiateProcessor(t *testing.T) {
	suite.Run(t, new(testSuffrageCandiateProcessor))
}
