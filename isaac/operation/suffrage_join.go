package isaacoperation

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
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
	base.BaseFact
	candidate base.Address
	// NOTE suffrage candidate state hash, when candidate is added to SuffrageCandidateState
	state util.Hash
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

func (suf SuffrageJoinPermissionFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageJoinPermission")

	if err := util.CheckIsValid(nil, false, suf.BaseFact, suf.candidate, suf.state); err != nil {
		return e(err, "")
	}

	if !valuehash.Bytes(suf.BaseFact.Token()).Equal(suf.state) {
		return e(util.InvalidError.Errorf("wrong token"), "")
	}

	if !suf.Hash().Equal(suf.hash()) {
		return e(util.InvalidError.Errorf("hash does not match"), "")
	}

	return nil
}

func (suf SuffrageJoinPermissionFact) Candidate() base.Address {
	return suf.candidate
}

func (suf SuffrageJoinPermissionFact) State() util.Hash {
	return suf.state
}

func (suf SuffrageJoinPermissionFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(suf.Token()),
		suf.candidate,
		suf.state,
	))
}

type SuffrageGenesisJoinPermissionFact struct {
	base.BaseFact
	node base.Address
	pub  base.Publickey
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

func (suf SuffrageGenesisJoinPermissionFact) IsValid(networkID []byte) error {
	e := util.StringErrorFunc("invalid SuffrageGenesisJoinPermission")

	if err := util.CheckIsValid(nil, false, suf.BaseFact, suf.node, suf.pub); err != nil {
		return e(err, "")
	}

	if !bytes.Equal(suf.BaseFact.Token(), networkID) {
		return e(util.InvalidError.Errorf("wrong token"), "")
	}

	if !suf.Hash().Equal(suf.hash()) {
		return e(util.InvalidError.Errorf("hash does not match"), "")
	}

	return nil
}

func (suf SuffrageGenesisJoinPermissionFact) Node() base.Address {
	return suf.node
}

func (suf SuffrageGenesisJoinPermissionFact) Publickey() base.Publickey {
	return suf.pub
}

func (suf SuffrageGenesisJoinPermissionFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(suf.Token()),
		suf.node,
		suf.pub,
	))
}

func NewSuffrageJoinPermissionNodeSignedFact(
	node base.Address,
	priv base.Privatekey,
	networkID base.NetworkID,
	fact SuffrageJoinPermissionFact,
) (base.NodeSigned, error) {
	signed, err := base.BaseNodeSignedFromFact(node, priv, networkID, fact)
	if err != nil {
		return nil, errors.Wrap(err, "failed to sign SuffrageJoinPermissionNodeSignedFact")
	}

	return signed, nil
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

	fact := op.Fact().(SuffrageGenesisJoinPermissionFact)
	if !fact.Publickey().Equal(op.Signed()[0].Signer()) {
		return e(util.InvalidError.Errorf("signer does not match with publickey"), "")
	}

	return nil
}

func (op SuffrageGenesisJoin) PreProcess(context.Context, base.GetStateFunc) (base.OperationProcessReasonError, error) {
	return nil, nil
}

func (op SuffrageGenesisJoin) Process(context.Context, base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
	fact := op.Fact().(SuffrageGenesisJoinPermissionFact)

	node := isaac.NewNode(fact.Publickey(), fact.Node())
	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.SuffrageStateKey,
			isaac.NewSuffrageStateValue(base.GenesisHeight, nil, []base.Node{node}),
			nil,
		),
	}, nil, nil
}
