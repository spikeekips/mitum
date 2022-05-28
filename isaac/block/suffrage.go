package isaacblock

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
)

var SuffrageProofHint = hint.MustNewHint("suffrage-proof-v0.0.1")

type SuffrageProof struct {
	m         base.BlockMap
	st        base.State
	voteproof base.ACCEPTVoteproof
	proof     fixedtree.Proof
	hint.BaseHinter
}

func NewSuffrageProof(
	ht hint.Hint,
	m base.BlockMap,
	st base.State,
	proof fixedtree.Proof,
	voteproof base.ACCEPTVoteproof,
) SuffrageProof {
	return SuffrageProof{
		BaseHinter: hint.NewBaseHinter(ht),
		m:          m,
		st:         st,
		proof:      proof,
		voteproof:  voteproof,
	}
}

func (s SuffrageProof) IsValid(b []byte) error {
	e := util.StringErrorFunc("invalid SuffrageProof")

	if err := s.BaseHinter.IsValid(SuffrageProofHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(b, false, s.m, s.st, s.proof, s.voteproof); err != nil {
		return e(err, "")
	}

	if s.st.Height() != s.m.Manifest().Height() {
		return e(util.ErrInvalid.Errorf("state height does not match with manifest"), "")
	}

	if _, err := s.Suffrage(); err != nil {
		return e(util.ErrInvalid.Wrap(err), "")
	}

	if s.voteproof.Result() != base.VoteResultMajority {
		return e(util.ErrInvalid.Errorf("accept voteproof is not majority"), "")
	}

	return nil
}

func (s SuffrageProof) Map() base.BlockMap {
	return s.m
}

func (s SuffrageProof) State() base.State {
	return s.st
}

func (s SuffrageProof) ACCEPTVoteproof() base.ACCEPTVoteproof {
	return s.voteproof
}

func (s SuffrageProof) Proof() fixedtree.Proof {
	return s.proof
}

func (s SuffrageProof) Suffrage() (base.Suffrage, error) {
	return isaac.NewSuffrageFromState(s.st)
}

func (s SuffrageProof) SuffrageHeight() base.Height {
	i, err := base.LoadSuffrageState(s.st)
	if err != nil {
		return base.NilHeight
	}

	return i.Height()
}

// Prove should be called after IsValid().
func (s SuffrageProof) Prove(previousState base.State) error {
	e := util.StringErrorFunc("failed to prove SuffrageProof")

	if s.m.Manifest().Height() == base.GenesisHeight {
		switch {
		case previousState != nil:
			return e(nil, "previous state should be nil for genesis")
		case s.st.Height() != base.GenesisHeight:
			return e(nil, "invalid state height; not genesis height")
		}
	}

	var previoussuf base.Suffrage

	switch {
	case s.m.Manifest().Height() == base.GenesisHeight:
	case s.st.Height() <= previousState.Height():
		return e(nil, "invalid previous state; higher height")
	case !s.st.Previous().Equal(previousState.Hash()):
		return e(nil, "not previous state; hash does not match")
	default:
		suf, err := isaac.NewSuffrageFromState(previousState)
		if err != nil {
			return e(err, "")
		}

		previous, _ := base.LoadSuffrageState(previousState)
		current, _ := base.LoadSuffrageState(s.st)

		if current.Height() != previous.Height()+1 {
			return e(nil, "invalid previous state value; not +1")
		}

		previoussuf = suf
	}

	switch {
	case !s.m.Manifest().Hash().Equal(s.voteproof.BallotMajority().NewBlock()):
		return e(nil, "manifest doest not match with suffrage voteproof")
	case previoussuf == nil && s.m.Manifest().Suffrage() != nil:
		return e(nil, "suffrage should be nil for genesis")
	case previoussuf != nil && !s.m.Manifest().Suffrage().Equal(previousState.Hash()):
		return e(nil, "suffrage does not match with previous suffrage")
	}

	if err := s.proof.Prove(s.st.Hash().String()); err != nil {
		return e(err, "failed to prove suffrage")
	}

	if previoussuf != nil {
		if err := base.IsValidVoteproofWithSuffrage(s.voteproof, previoussuf); err != nil {
			return e(err, "")
		}
	}

	return nil
}
