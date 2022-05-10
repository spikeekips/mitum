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
		return e(util.InvalidError.Errorf("wrong token"), "")
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e(util.InvalidError.Errorf("hash does not match"), "")
	}

	return nil
}

func (fact SuffrageJoinPermissionFact) Candidate() base.Address {
	return fact.candidate
}

func (fact SuffrageJoinPermissionFact) State() util.Hash {
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
	node base.Address
	pub  base.Publickey
	base.BaseFact
}

func NewSuffrageGenesisJoinPermissionFact(
	node base.Address,
	pub base.Publickey,
	networkID base.NetworkID,
) SuffrageGenesisJoinPermissionFact {
	fact := SuffrageGenesisJoinPermissionFact{
		BaseFact: base.NewBaseFact(SuffrageGenesisJoinPermissionFactHint, base.Token(networkID)),
		node:     node,
		pub:      pub,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact SuffrageGenesisJoinPermissionFact) IsValid(networkID []byte) error {
	e := util.StringErrorFunc("invalid SuffrageGenesisJoinPermissionFact")

	if err := util.CheckIsValid(nil, false, fact.BaseFact, fact.node, fact.pub); err != nil {
		return e(err, "")
	}

	if !bytes.Equal(fact.BaseFact.Token(), networkID) {
		return e(util.InvalidError.Errorf("wrong token"), "")
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e(util.InvalidError.Errorf("hash does not match"), "")
	}

	return nil
}

func (fact SuffrageGenesisJoinPermissionFact) Node() base.Address {
	return fact.node
}

func (fact SuffrageGenesisJoinPermissionFact) Publickey() base.Publickey {
	return fact.pub
}

func (fact SuffrageGenesisJoinPermissionFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(fact.Token()),
		fact.node,
		fact.pub,
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
		return e(util.InvalidError.Errorf("multiple signed found"), "")
	}

	// BLOCK check signer should be genesis block creator

	fact := op.Fact().(SuffrageGenesisJoinPermissionFact)
	if !fact.Publickey().Equal(op.Signed()[0].Signer()) {
		return e(util.InvalidError.Errorf("signer does not match with publickey"), "")
	}

	return nil
}

func (SuffrageGenesisJoin) PreProcess(context.Context, base.GetStateFunc) (base.OperationProcessReasonError, error) {
	return nil, nil
}

func (op SuffrageGenesisJoin) Process(context.Context, base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	fact := op.Fact().(SuffrageGenesisJoinPermissionFact)

	node := isaac.NewNode(fact.Publickey(), fact.Node())
	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.SuffrageStateKey,
			isaac.NewSuffrageStateValue(base.GenesisHeight, []base.Node{node}),
			nil,
		),
	}, nil, nil
}
