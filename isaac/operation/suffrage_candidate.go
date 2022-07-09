package isaacoperation

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	SuffrageCandidateFactHint = hint.MustNewHint("suffrage-candidate-fact-v0.0.1")
	SuffrageCandidateHint     = hint.MustNewHint("suffrage-candidate-operation-v0.0.1")
)

type SuffrageCandidateFact struct {
	address   base.Address
	publickey base.Publickey
	base.BaseFact
}

func NewSuffrageCandidateFact(
	token base.Token,
	address base.Address,
	publickey base.Publickey,
) SuffrageCandidateFact {
	fact := SuffrageCandidateFact{
		BaseFact:  base.NewBaseFact(SuffrageCandidateFactHint, token),
		address:   address,
		publickey: publickey,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact SuffrageCandidateFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageCandidateFact")

	if err := util.CheckIsValid(nil, false, fact.BaseFact, fact.address, fact.publickey); err != nil {
		return e(err, "")
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e(util.ErrInvalid.Errorf("hash does not match"), "")
	}

	return nil
}

func (fact SuffrageCandidateFact) Address() base.Address {
	return fact.address
}

func (fact SuffrageCandidateFact) Publickey() base.Publickey {
	return fact.publickey
}

func (fact SuffrageCandidateFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(fact.Token()),
		fact.address,
		fact.publickey,
	))
}

type SuffrageCandidate struct {
	base.BaseNodeOperation
}

func NewSuffrageCandidate(fact SuffrageCandidateFact) SuffrageCandidate {
	return SuffrageCandidate{
		BaseNodeOperation: base.NewBaseNodeOperation(SuffrageCandidateHint, fact),
	}
}

func (op SuffrageCandidate) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageCandidate")

	if err := op.BaseNodeOperation.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	sfs := op.Signed()

	if _, duplicated := util.CheckSliceDuplicated(sfs, func(_ interface{}, i int) string {
		ns := sfs[i].(base.NodeSigned) //nolint:forcetypeassert //...

		return ns.Node().String() + "-" + ns.Signer().String()
	}); duplicated {
		return e.Errorf("duplicated signed found")
	}

	fact, ok := op.Fact().(SuffrageCandidateFact)
	if !ok {
		return e.Errorf("not SuffrageCandidateFact, %T", op.Fact())
	}

	for i := range sfs {
		ns := sfs[i].(base.NodeSigned) //nolint:forcetypeassert //...

		if !ns.Node().Equal(fact.Address()) {
			continue
		}

		if err := sfs[i].Verify(networkID, op.Fact().Hash().Bytes()); err != nil {
			return e.Wrapf(err, "failed to verify signed by Candidate")
		}

		break
	}

	return nil
}
