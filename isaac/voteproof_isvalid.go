package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

func IsValidVoteproofWithSuffrage(vp base.Voteproof, suf base.Suffrage) error {
	e := util.ErrInvalid.Errorf("invalid voteproof with suffrage")

	if vp == nil {
		return e.Errorf("nil voteproof")
	}

	var expels []base.SuffrageExpelOperation

	if w, ok := vp.(base.HasExpels); ok {
		expels = w.Expels()
	}

	th := vp.Threshold()
	rsuf := suf

	if len(expels) > 0 {
		for i := range expels {
			if err := IsValidExpelWithSuffrage(vp.Point().Height(), expels[i], suf); err != nil {
				return e.Wrap(err)
			}
		}

		switch i, err := NewSuffrageWithExpels(suf, vp.Threshold(), expels); {
		case err != nil:
			return e.Wrap(err)
		default:
			rsuf = i
			th = base.MaxThreshold
		}
	}

	if _, ok := vp.(base.StuckVoteproof); ok {
		if suf.Len() != len(vp.SignFacts())+len(expels) {
			return e.Errorf("not enough sign facts with expels")
		}
	}

	if err := base.IsValidVoteproofWithSuffrage(vp, rsuf, th); err != nil {
		return e.Wrap(err)
	}

	return nil
}
