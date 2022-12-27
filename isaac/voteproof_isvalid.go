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

	var withdraws []base.SuffrageWithdrawOperation

	switch t := vp.(type) {
	case INITVoteproof:
		withdraws = t.Withdraws()
	case ACCEPTVoteproof:
		withdraws = t.Withdraws()
	default:
		return e.Errorf("unknown voteproof, %T", vp)
	}

	rsuf := suf

	if len(withdraws) > 0 {
		for i := range withdraws {
			if err := IsValidWithdrawWithSuffrage(vp.Point().Height(), withdraws[i], suf); err != nil {
				return e.Wrap(err)
			}
		}

		switch i, err := NewSuffrageWithWithdraws(suf, vp.Threshold(), withdraws); {
		case err != nil:
			return e.Wrap(err)
		default:
			rsuf = i
		}
	}

	if err := base.IsValidVoteproofWithSuffrage(vp, rsuf); err != nil {
		return e.Wrap(err)
	}

	return nil
}
