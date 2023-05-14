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
	m base.BlockMap,
	st base.State,
	proof fixedtree.Proof,
	voteproof base.ACCEPTVoteproof,
) SuffrageProof {
	return SuffrageProof{
		BaseHinter: hint.NewBaseHinter(SuffrageProofHint),
		m:          m,
		st:         st,
		proof:      proof,
		voteproof:  voteproof,
	}
}

func (s SuffrageProof) IsValid(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageProof")

	if err := s.BaseHinter.IsValid(SuffrageProofHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(b, false, s.m, s.st, s.proof, s.voteproof); err != nil {
		return e.Wrap(err)
	}

	if s.st.Height() != s.m.Manifest().Height() {
		return e.Errorf("state height does not match with manifest")
	}

	if _, err := s.Suffrage(); err != nil {
		return e.Wrap(err)
	}

	if s.voteproof.Result() != base.VoteResultMajority {
		return e.Errorf("accept voteproof is not majority")
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
	i, err := base.LoadSuffrageNodesStateValue(s.st)
	if err != nil {
		return base.NilHeight
	}

	return i.Height()
}

// Prove should be called after IsValid().
func (s SuffrageProof) Prove(previousState base.State) error {
	e := util.StringError("prove SuffrageProof")

	if s.m.Manifest().Height() == base.GenesisHeight {
		switch {
		case previousState != nil:
			return e.Errorf("previous state should be nil for genesis")
		case s.st.Height() != base.GenesisHeight:
			return e.Errorf("invalid state height; not genesis height")
		}
	}

	var previoussuf base.Suffrage

	switch {
	case s.m.Manifest().Height() == base.GenesisHeight:
	case s.st.Height() <= previousState.Height():
		return e.Errorf("invalid previous state; higher height")
	case !s.st.Previous().Equal(previousState.Hash()):
		return e.Errorf("not previous state; hash does not match")
	default:
		suf, err := isaac.NewSuffrageFromState(previousState)
		if err != nil {
			return e.Wrap(err)
		}

		previous, _ := base.LoadSuffrageNodesStateValue(previousState)
		current, _ := base.LoadSuffrageNodesStateValue(s.st)

		if current.Height() != previous.Height()+1 {
			return e.Errorf("invalid previous state value; not +1")
		}

		previoussuf = suf
	}

	if !s.m.Manifest().Hash().Equal(s.voteproof.BallotMajority().NewBlock()) {
		return e.Errorf("manifest doest not match with suffrage voteproof")
	}

	if err := s.proof.Prove(s.st.Hash().String()); err != nil {
		return e.WithMessage(err, "prove suffrage")
	}

	if previoussuf != nil {
		if err := isaac.IsValidVoteproofWithSuffrage(s.voteproof, previoussuf); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}
