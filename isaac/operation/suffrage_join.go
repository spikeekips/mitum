package isaacoperation

import (
	"bytes"
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	SuffrageJoinPermissionFactHint        = hint.MustNewHint("suffrage-join-permission-fact-v0.0.1")
	SuffrageJoinHint                      = hint.MustNewHint("suffrage-join-operation-v0.0.1")
	SuffrageGenesisJoinPermissionFactHint = hint.MustNewHint("suffrage-genesis-join-permission-fact-v0.0.1")
	SuffrageGenesisJoinHint               = hint.MustNewHint("suffrage-genesis-join-operation-v0.0.1")
)

type SuffrageJoinPermissionFact struct {
	candidate base.Address
	state     util.Hash
	base.BaseFact
}

func NewSuffrageJoinPermissionFact(
	candidate base.Address,
	state util.Hash,
) SuffrageJoinPermissionFact {
	var t base.Token
	if state != nil {
		t = base.Token(state.Bytes())
	}

	fact := SuffrageJoinPermissionFact{
		BaseFact:  base.NewBaseFact(SuffrageJoinPermissionFactHint, t),
		candidate: candidate,
		state:     state,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact SuffrageJoinPermissionFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageJoinPermissionFact")

	if err := util.CheckIsValid(nil, false, fact.BaseFact, fact.candidate, fact.state); err != nil {
		return e(err, "")
	}

	if !valuehash.Bytes(fact.BaseFact.Token()).Equal(fact.state) {
		return e(util.ErrInvalid.Errorf("wrong token"), "")
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e(util.ErrInvalid.Errorf("hash does not match"), "")
	}

	return nil
}

func (fact SuffrageJoinPermissionFact) Candidate() base.Address {
	return fact.candidate
}

func (fact SuffrageJoinPermissionFact) State() util.Hash {
	// FIXME check state; it indicates that the candidate registered.
	return fact.state
}

func (fact SuffrageJoinPermissionFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(fact.Token()),
		fact.candidate,
		fact.state,
	))
}

type SuffrageGenesisJoinPermissionFact struct {
	nodes []base.Node
	base.BaseFact
}

func NewSuffrageGenesisJoinPermissionFact(
	nodes []base.Node,
	networkID base.NetworkID,
) SuffrageGenesisJoinPermissionFact {
	fact := SuffrageGenesisJoinPermissionFact{
		BaseFact: base.NewBaseFact(SuffrageGenesisJoinPermissionFactHint, base.Token(networkID)),
		nodes:    nodes,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact SuffrageGenesisJoinPermissionFact) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageGenesisJoinPermissionFact")

	if len(fact.nodes) < 1 {
		return e.Errorf("empty nodes")
	}

	vs := make([]util.IsValider, len(fact.nodes)+1)
	vs[0] = fact.BaseFact

	for i := range fact.nodes {
		vs[i+1] = fact.nodes[i]
	}

	if err := util.CheckIsValid(nil, false, vs...); err != nil {
		return e.Wrap(err)
	}

	if !bytes.Equal(fact.BaseFact.Token(), networkID) {
		return e.Errorf("wrong token")
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e.Errorf("hash does not match")
	}

	return nil
}

func (fact SuffrageGenesisJoinPermissionFact) Nodes() []base.Node {
	return fact.nodes
}

func (fact SuffrageGenesisJoinPermissionFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(fact.Token()),
		util.DummyByter(func() []byte {
			var b bytes.Buffer

			for i := range fact.nodes {
				_, _ = b.Write(fact.nodes[i].HashBytes())
			}

			return b.Bytes()
		}),
	))
}

func NewSuffrageJoin(fact SuffrageJoinPermissionFact) base.BaseOperation {
	return base.NewBaseOperation(SuffrageJoinHint, fact)
}

// SuffrageGenesisJoin is only for used for genesis block
type SuffrageGenesisJoin struct {
	base.BaseOperation
}

func NewSuffrageGenesisJoin(fact SuffrageGenesisJoinPermissionFact) SuffrageGenesisJoin {
	return SuffrageGenesisJoin{
		BaseOperation: base.NewBaseOperation(SuffrageGenesisJoinHint, fact),
	}
}

func (op SuffrageGenesisJoin) IsValid(networkID []byte) error {
	e := util.StringErrorFunc("invalid SuffrageGenesisJoin")
	if err := op.BaseOperation.IsValid(networkID); err != nil {
		return e(err, "")
	}

	if len(op.Signed()) > 1 {
		return e(util.ErrInvalid.Errorf("multiple signed found"), "")
	}

	return nil
}

func (SuffrageGenesisJoin) PreProcess(context.Context, base.GetStateFunc) (base.OperationProcessReasonError, error) {
	return nil, nil
}

func (op SuffrageGenesisJoin) Process(context.Context, base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	fact := op.Fact().(SuffrageGenesisJoinPermissionFact) //nolint:forcetypeassert //...

	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.SuffrageStateKey,
			isaac.NewSuffrageStateValue(base.GenesisHeight, fact.Nodes()),
			nil,
		),
	}, nil, nil
}
